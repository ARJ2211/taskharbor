package redis

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ARJ2211/taskharbor/taskharbor/driver"
)

// skip if no REDIS_ADDR; else return a driver + unique queue name so tests don't step on each other
func testRedis(t *testing.T) (*Driver, string) {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set, skipping Redis integration test")
	}
	ctx := context.Background()
	d, err := New(ctx, addr, DB(14), KeyPrefix("th-test"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	queue := "q-" + strings.ReplaceAll(t.Name(), "/", "_")
	return d, queue
}

func TestNew_InvalidAddr(t *testing.T) {
	ctx := context.Background()
	_, err := New(ctx, "localhost:0")
	if err == nil {
		t.Fatal("expected error when connecting to invalid address")
	}
}

func TestRedis_EnqueueReserveAck(t *testing.T) {
	d, queue := testRedis(t)
	ctx := context.Background()

	rec := driver.JobRecord{
		ID:         "test-job-1",
		Type:       "test",
		Queue:      queue,
		Payload:    []byte(`{"x":1}`),
		RunAt:      time.Time{},
		Timeout:    time.Second,
		CreatedAt:  time.Now().UTC(),
		Attempts:   0,
		MaxAttempts: 3,
	}
	if err := rec.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := d.Enqueue(ctx, rec); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	got, lease, ok, err := d.Reserve(ctx, queue, now, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected one job to be reserved")
	}
	if got.ID != rec.ID || got.Type != rec.Type {
		t.Errorf("got job %q %q, want %q %q", got.ID, got.Type, rec.ID, rec.Type)
	}
	if lease.Token == "" || !lease.ExpiresAt.After(now) {
		t.Errorf("bad lease: token=%q expires=%v", lease.Token, lease.ExpiresAt)
	}
	if err := d.Ack(ctx, got.ID, lease.Token, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}

	_, _, ok2, err := d.Reserve(ctx, queue, time.Now().UTC(), 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if ok2 {
		t.Fatal("expected no job after ack")
	}
}

func TestRedis_SchedulePromotion(t *testing.T) {
	d, queue := testRedis(t)
	ctx := context.Background()

	t0 := time.Date(2026, 1, 20, 10, 0, 0, 0, time.UTC)
	runAt := t0.Add(10 * time.Second)
	rec := driver.JobRecord{
		ID:        "job-sched-1",
		Type:      "report.build",
		Payload:   []byte(`{"id":123}`),
		Queue:     queue,
		RunAt:     runAt,
		CreatedAt: t0,
	}
	if err := d.Enqueue(ctx, rec); err != nil {
		t.Fatal(err)
	}

	_, _, ok, err := d.Reserve(ctx, queue, t0, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected ok=false before runAt, got ok=true")
	}

	got, _, ok, err := d.Reserve(ctx, queue, runAt, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected ok=true at runAt, got ok=false")
	}
	if got.ID != rec.ID {
		t.Errorf("got id %q, want %q", got.ID, rec.ID)
	}
}

func TestRedis_FailMovesToDLQ(t *testing.T) {
	d, queue := testRedis(t)
	ctx := context.Background()

	now := time.Date(2026, 1, 20, 10, 0, 0, 0, time.UTC)
	rec := driver.JobRecord{
		ID:        "job-fail-1",
		Type:      "task.fail",
		Payload:   []byte(`{"x":1}`),
		Queue:     queue,
		RunAt:     time.Time{},
		CreatedAt: now,
	}
	if err := d.Enqueue(ctx, rec); err != nil {
		t.Fatal(err)
	}

	_, lease, ok, err := d.Reserve(ctx, queue, now, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected ok=true, got ok=false")
	}
	if err := d.Fail(ctx, rec.ID, lease.Token, now, "boom"); err != nil {
		t.Fatal(err)
	}

	// After fail, nothing runnable left (job is in DLQ, not in ready)
	_, _, ok, err = d.Reserve(ctx, queue, now, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected no job after fail (job in DLQ)")
	}
}

func TestRedis_ReserveDoesNotDoubleDeliverInflight(t *testing.T) {
	d, queue := testRedis(t)
	ctx := context.Background()

	now := time.Date(2026, 1, 20, 10, 0, 0, 0, time.UTC)
	rec := driver.JobRecord{
		ID:        "job-once-1",
		Type:      "task.once",
		Payload:   []byte(`{}`),
		Queue:     queue,
		RunAt:     time.Time{},
		CreatedAt: now,
	}
	if err := d.Enqueue(ctx, rec); err != nil {
		t.Fatal(err)
	}

	got1, _, ok, err := d.Reserve(ctx, queue, now, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || got1.ID != rec.ID {
		t.Fatalf("expected one job %q, got ok=%v id=%q", rec.ID, ok, got1.ID)
	}

	_, _, ok, err = d.Reserve(ctx, queue, now, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected ok=false when only job is inflight, got ok=true")
	}
}

func TestRedis_RetryMovesInflightBackToScheduled(t *testing.T) {
	d, queue := testRedis(t)
	ctx := context.Background()

	t0 := time.Date(2026, 1, 20, 10, 0, 0, 0, time.UTC)
	retryAt := t0.Add(10 * time.Second)
	rec := driver.JobRecord{
		ID:        "job-retry-1",
		Type:      "task.retry",
		Payload:   []byte(`{"x":1}`),
		Queue:     queue,
		RunAt:     time.Time{},
		CreatedAt: t0,
	}
	if err := d.Enqueue(ctx, rec); err != nil {
		t.Fatal(err)
	}

	_, lease, ok, err := d.Reserve(ctx, queue, t0, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected ok=true, got ok=false")
	}
	if err := d.Retry(ctx, rec.ID, lease.Token, t0, driver.RetryUpdate{
		RunAt:     retryAt,
		Attempts:  1,
		LastError: "boom",
		FailedAt:  t0,
	}); err != nil {
		t.Fatal(err)
	}

	_, _, ok, err = d.Reserve(ctx, queue, t0, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected ok=false before retryAt, got ok=true")
	}

	got, _, ok, err := d.Reserve(ctx, queue, retryAt, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected ok=true at retryAt, got ok=false")
	}
	if got.Attempts != 1 {
		t.Errorf("expected attempts=1, got %d", got.Attempts)
	}
	if got.LastError != "boom" {
		t.Errorf("expected last_error=boom, got %q", got.LastError)
	}
}

func TestRedis_AckRejectsLeaseMismatch(t *testing.T) {
	d, queue := testRedis(t)
	ctx := context.Background()

	now := time.Date(2026, 1, 20, 10, 0, 0, 0, time.UTC)
	rec := driver.JobRecord{
		ID:        "job-lease-mismatch",
		Type:      "task.once",
		Payload:   []byte(`{}`),
		Queue:     queue,
		CreatedAt: now,
	}
	if err := d.Enqueue(ctx, rec); err != nil {
		t.Fatal(err)
	}

	_, lease, ok, err := d.Reserve(ctx, queue, now, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || lease.Token == "" {
		t.Fatal("expected reserved job with token")
	}

	if err := d.Ack(ctx, rec.ID, driver.LeaseToken("wrong"), now); err == nil {
		t.Fatal("expected error for lease mismatch, got nil")
	} else if !errors.Is(err, driver.ErrLeaseMismatch) {
		t.Fatalf("expected ErrLeaseMismatch, got %v", err)
	}

	if err := d.Ack(ctx, rec.ID, lease.Token, now); err != nil {
		t.Fatalf("ack with correct token should succeed: %v", err)
	}
}

func TestRedis_ReclaimExpiredLeaseMakesJobRunnableAgain(t *testing.T) {
	d, queue := testRedis(t)
	ctx := context.Background()

	t0 := time.Date(2026, 1, 20, 10, 0, 0, 0, time.UTC)
	rec := driver.JobRecord{
		ID:        "job-reclaim-1",
		Type:      "task.once",
		Payload:   []byte(`{}`),
		Queue:     queue,
		CreatedAt: t0,
	}
	if err := d.Enqueue(ctx, rec); err != nil {
		t.Fatal(err)
	}

	_, lease1, ok, err := d.Reserve(ctx, queue, t0, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected ok=true, got ok=false")
	}

	_, _, ok, err = d.Reserve(ctx, queue, t0.Add(5*time.Second), 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected ok=false before lease expiry, got ok=true")
	}

	_, lease2, ok, err := d.Reserve(ctx, queue, t0.Add(11*time.Second), 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected ok=true after lease expiry (reclaim), got ok=false")
	}
	if lease2.Token == lease1.Token {
		t.Fatal("expected new lease token after reclaim")
	}
	if err := d.Ack(ctx, rec.ID, lease2.Token, t0.Add(11*time.Second)); err != nil {
		t.Fatal(err)
	}
}

func TestRedis_ExtendLeasePreventsReclaim(t *testing.T) {
	d, queue := testRedis(t)
	ctx := context.Background()

	t0 := time.Date(2026, 1, 20, 10, 0, 0, 0, time.UTC)
	rec := driver.JobRecord{
		ID:        "job-heartbeat-1",
		Type:      "task.once",
		Payload:   []byte(`{}`),
		Queue:     queue,
		CreatedAt: t0,
	}
	if err := d.Enqueue(ctx, rec); err != nil {
		t.Fatal(err)
	}

	_, lease, ok, err := d.Reserve(ctx, queue, t0, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected ok=true, got ok=false")
	}

	// Extend lease at t0+5 so expiry becomes t0+15
	newLease, err := d.ExtendLease(ctx, rec.ID, lease.Token, t0.Add(5*time.Second), 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !newLease.ExpiresAt.After(t0.Add(10 * time.Second)) {
		t.Errorf("extend should push expiry out: got %v", newLease.ExpiresAt)
	}

	// At t0+11 old expiry would have passed; with extend, job still inflight so no reserve
	_, _, ok, err = d.Reserve(ctx, queue, t0.Add(11*time.Second), 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected ok=false while lease extended (job still inflight), got ok=true")
	}

	if err := d.Ack(ctx, rec.ID, newLease.Token, t0.Add(11*time.Second)); err != nil {
		t.Fatal(err)
	}
}

func TestRedis_ReserveInvalidLeaseDuration(t *testing.T) {
	d, queue := testRedis(t)
	ctx := context.Background()

	_, _, _, err := d.Reserve(ctx, queue, time.Now().UTC(), 0)
	if err == nil {
		t.Fatal("expected error for lease duration 0")
	}
	if !errors.Is(err, driver.ErrInvalidLeaseDuration) {
		t.Fatalf("expected ErrInvalidLeaseDuration, got %v", err)
	}
}

func TestRedis_EnqueueValidation(t *testing.T) {
	d, queue := testRedis(t)
	ctx := context.Background()

	bad := driver.JobRecord{
		ID:    "x",
		Type:  "", // invalid
		Queue: queue,
	}
	if err := d.Enqueue(ctx, bad); err == nil {
		t.Fatal("expected validation error for empty type")
	}
}

func TestRedis_ExtendLeaseExpiredReturnsError(t *testing.T) {
	d, queue := testRedis(t)
	ctx := context.Background()

	t0 := time.Date(2026, 1, 20, 10, 0, 0, 0, time.UTC)
	rec := driver.JobRecord{
		ID:        "job-extend-expired",
		Type:      "task.once",
		Payload:   []byte(`{}`),
		Queue:     queue,
		CreatedAt: t0,
	}
	if err := d.Enqueue(ctx, rec); err != nil {
		t.Fatal(err)
	}

	_, lease, ok, err := d.Reserve(ctx, queue, t0, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected ok=true")
	}

	// Try to extend after expiry (now = t0+10, lease expired at t0+5)
	_, err = d.ExtendLease(ctx, rec.ID, lease.Token, t0.Add(10*time.Second), 10*time.Second)
	if err == nil {
		t.Fatal("expected error when extending expired lease")
	}
	if !errors.Is(err, driver.ErrLeaseExpired) {
		t.Fatalf("expected ErrLeaseExpired, got %v", err)
	}

	// Job should have been reclaimed; we can reserve it again
	_, lease2, ok, err := d.Reserve(ctx, queue, t0.Add(10*time.Second), 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected job to be reclaimable after extend failed")
	}
	if err := d.Ack(ctx, rec.ID, lease2.Token, t0.Add(10*time.Second)); err != nil {
		t.Fatal(err)
	}
}

func TestRedis_ClosedDriverReturnsError(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set")
	}
	ctx := context.Background()
	dr, err := New(ctx, addr, DB(14), KeyPrefix("th-test-closed"))
	if err != nil {
		t.Fatal(err)
	}
	dr.Close()

	err = dr.Enqueue(ctx, driver.JobRecord{ID: "x", Type: "t", Queue: "closed-test", CreatedAt: time.Now()})
	if err == nil {
		t.Fatal("expected error when enqueue on closed driver")
	}
	if !errors.Is(err, ErrDriverClosed) {
		t.Fatalf("expected ErrDriverClosed, got %v", err)
	}
}
