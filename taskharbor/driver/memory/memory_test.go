package memory

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ARJ2211/taskharbor/taskharbor/driver"
)

func TestMemory_EnqueueReserveAck(t *testing.T) {
	ctx := context.Background()
	d := New()

	now := time.Date(2026, 1, 20, 10, 0, 0, 0, time.UTC)

	rec := driver.JobRecord{
		ID:        "job-1",
		Type:      "email.send",
		Payload:   []byte(`{"to":"a@b.com"}`),
		Queue:     "default",
		RunAt:     time.Time{},
		CreatedAt: now,
	}

	err := d.Enqueue(ctx, rec)
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	got, lease, ok, err := d.Reserve(ctx, "default", now, 30*time.Second)
	if err != nil {
		t.Fatalf("reserve failed: %v", err)
	}
	if !ok {
		t.Fatalf("expected ok=true, got ok=false")
	}
	if got.ID != rec.ID {
		t.Fatalf("expected id %s, got %s", rec.ID, got.ID)
	}

	if d.InflightSize("default") != 1 {
		t.Fatalf("expected inflight size 1, got %d", d.InflightSize("default"))
	}

	err = d.Ack(ctx, rec.ID, lease.Token, now)
	if err != nil {
		t.Fatalf("ack failed: %v", err)
	}

	if d.InflightSize("default") != 0 {
		t.Fatalf("expected inflight size 0, got %d", d.InflightSize("default"))
	}
}

func TestMemory_SchedulePromotion(t *testing.T) {
	ctx := context.Background()
	d := New()

	t0 := time.Date(2026, 1, 20, 10, 0, 0, 0, time.UTC)
	runAt := t0.Add(10 * time.Second)

	rec := driver.JobRecord{
		ID:        "job-2",
		Type:      "report.build",
		Payload:   []byte(`{"id":123}`),
		Queue:     "default",
		RunAt:     runAt,
		CreatedAt: t0,
	}

	err := d.Enqueue(ctx, rec)
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	_, _, ok, err := d.Reserve(ctx, "default", t0, 30*time.Second)
	if err != nil {
		t.Fatalf("reserve failed: %v", err)
	}
	if ok {
		t.Fatalf("expected ok=false before runAt, got ok=true")
	}

	_, _, ok, err = d.Reserve(ctx, "default", runAt, 30*time.Second)
	if err != nil {
		t.Fatalf("reserve failed: %v", err)
	}
	if !ok {
		t.Fatalf("expected ok=true at runAt, got ok=false")
	}
}

func TestMemory_FailMovesToDLQ(t *testing.T) {
	ctx := context.Background()
	d := New()

	now := time.Date(2026, 1, 20, 10, 0, 0, 0, time.UTC)

	rec := driver.JobRecord{
		ID:        "job-3",
		Type:      "task.fail",
		Payload:   []byte(`{"x":1}`),
		Queue:     "default",
		RunAt:     time.Time{},
		CreatedAt: now,
	}

	err := d.Enqueue(ctx, rec)
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	_, lease, ok, err := d.Reserve(ctx, "default", now, 30*time.Second)
	if err != nil {
		t.Fatalf("reserve failed: %v", err)
	}
	if !ok {
		t.Fatalf("expected ok=true, got ok=false")
	}

	err = d.Fail(ctx, rec.ID, lease.Token, now, "boom")
	if err != nil {
		t.Fatalf("fail failed: %v", err)
	}

	if d.InflightSize("default") != 0 {
		t.Fatalf("expected inflight size 0, got %d", d.InflightSize("default"))
	}

	items := d.DLQItems("default")
	if len(items) != 1 {
		t.Fatalf("expected 1 dlq item, got %d", len(items))
	}
	if items[0].Record.ID != rec.ID {
		t.Fatalf("expected dlq job id %s, got %s", rec.ID, items[0].Record.ID)
	}
	if items[0].Reason != "boom" {
		t.Fatalf("expected dlq reason boom, got %s", items[0].Reason)
	}
}

func TestMemory_ReserveDoesNotDoubleDeliverInflight(t *testing.T) {
	ctx := context.Background()
	d := New()

	now := time.Date(2026, 1, 20, 10, 0, 0, 0, time.UTC)

	rec := driver.JobRecord{
		ID:        "job-4",
		Type:      "task.once",
		Payload:   []byte(`{}`),
		Queue:     "default",
		RunAt:     time.Time{},
		CreatedAt: now,
	}

	err := d.Enqueue(ctx, rec)
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	got1, _, ok, err := d.Reserve(ctx, "default", now, 30*time.Second)
	if err != nil {
		t.Fatalf("reserve failed: %v", err)
	}
	if !ok {
		t.Fatalf("expected ok=true, got ok=false")
	}
	if got1.ID != rec.ID {
		t.Fatalf("expected id %s, got %s", rec.ID, got1.ID)
	}

	_, _, ok, err = d.Reserve(ctx, "default", now, 30*time.Second)
	if err != nil {
		t.Fatalf("reserve failed: %v", err)
	}
	if ok {
		t.Fatalf("expected ok=false when only job is inflight, got ok=true")
	}
}

func TestMemory_RetryMovesInflightBackToScheduled(t *testing.T) {
	ctx := context.Background()
	d := New()

	t0 := time.Date(2026, 1, 20, 10, 0, 0, 0, time.UTC)
	retryAt := t0.Add(10 * time.Second)

	rec := driver.JobRecord{
		ID:        "job-retry-1",
		Type:      "task.retry",
		Payload:   []byte(`{"x":1}`),
		Queue:     "default",
		RunAt:     time.Time{},
		CreatedAt: t0,
	}

	if err := d.Enqueue(ctx, rec); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	_, lease, ok, err := d.Reserve(ctx, "default", t0, 30*time.Second)
	if err != nil {
		t.Fatalf("reserve failed: %v", err)
	}
	if !ok {
		t.Fatalf("expected ok=true, got ok=false")
	}

	if err := d.Retry(ctx, rec.ID, lease.Token, t0, driver.RetryUpdate{
		RunAt:     retryAt,
		Attempts:  1,
		LastError: "boom",
		FailedAt:  t0,
	}); err != nil {
		t.Fatalf("retry failed: %v", err)
	}

	if d.InflightSize("default") != 0 {
		t.Fatalf("expected inflight size 0, got %d", d.InflightSize("default"))
	}

	_, _, ok, err = d.Reserve(ctx, "default", t0, 30*time.Second)
	if err != nil {
		t.Fatalf("reserve failed: %v", err)
	}
	if ok {
		t.Fatalf("expected ok=false before retryAt, got ok=true")
	}

	got, _, ok, err := d.Reserve(ctx, "default", retryAt, 30*time.Second)
	if err != nil {
		t.Fatalf("reserve failed: %v", err)
	}
	if !ok {
		t.Fatalf("expected ok=true at retryAt, got ok=false")
	}
	if got.Attempts != 1 {
		t.Fatalf("expected attempts=1, got %d", got.Attempts)
	}
	if got.LastError != "boom" {
		t.Fatalf("expected last_error=boom, got %s", got.LastError)
	}
}

func TestMemory_AckRejectsLeaseMismatch(t *testing.T) {
	ctx := context.Background()
	d := New()

	now := time.Date(2026, 1, 20, 10, 0, 0, 0, time.UTC)

	rec := driver.JobRecord{
		ID:        "job-lease-mismatch",
		Type:      "task.once",
		Payload:   []byte(`{}`),
		Queue:     "default",
		CreatedAt: now,
	}

	if err := d.Enqueue(ctx, rec); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	_, lease, ok, err := d.Reserve(ctx, "default", now, 30*time.Second)
	if err != nil {
		t.Fatalf("reserve failed: %v", err)
	}
	if !ok {
		t.Fatalf("expected ok=true, got ok=false")
	}
	if lease.Token == "" {
		t.Fatalf("expected non-empty lease token")
	}

	// wrong token should be rejected
	if err := d.Ack(ctx, rec.ID, driver.LeaseToken("wrong"), now); err == nil {
		t.Fatalf("expected error for lease mismatch, got nil")
	} else if !errors.Is(err, driver.ErrLeaseMismatch) {
		t.Fatalf("expected ErrLeaseMismatch, got %v", err)
	}

	if d.InflightSize("default") != 1 {
		t.Fatalf("expected inflight size 1, got %d", d.InflightSize("default"))
	}

	// correct token should succeed
	if err := d.Ack(ctx, rec.ID, lease.Token, now); err != nil {
		t.Fatalf("ack failed: %v", err)
	}
}

func TestMemory_ReclaimExpiredLeaseMakesJobRunnableAgain(t *testing.T) {
	ctx := context.Background()
	d := New()

	t0 := time.Date(2026, 1, 20, 10, 0, 0, 0, time.UTC)

	rec := driver.JobRecord{
		ID:        "job-lease-expire-1",
		Type:      "task.once",
		Payload:   []byte(`{}`),
		Queue:     "default",
		CreatedAt: t0,
	}

	if err := d.Enqueue(ctx, rec); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	_, lease1, ok, err := d.Reserve(ctx, "default", t0, 10*time.Second)
	if err != nil {
		t.Fatalf("reserve failed: %v", err)
	}
	if !ok {
		t.Fatalf("expected ok=true, got ok=false")
	}

	// before expiry: should not deliver
	_, _, ok, err = d.Reserve(ctx, "default", t0.Add(5*time.Second), 10*time.Second)
	if err != nil {
		t.Fatalf("reserve failed: %v", err)
	}
	if ok {
		t.Fatalf("expected ok=false before lease expiry, got ok=true")
	}

	// after expiry: reserve should reclaim + re-lease
	_, lease2, ok, err := d.Reserve(ctx, "default", t0.Add(11*time.Second), 10*time.Second)
	if err != nil {
		t.Fatalf("reserve failed: %v", err)
	}
	if !ok {
		t.Fatalf("expected ok=true after lease expiry, got ok=false")
	}
	if lease2.Token == "" {
		t.Fatalf("expected non-empty lease token")
	}
	if lease2.Token == lease1.Token {
		t.Fatalf("expected new lease token after reclaim")
	}

	if err := d.Ack(ctx, rec.ID, lease2.Token, t0.Add(11*time.Second)); err != nil {
		t.Fatalf("ack failed: %v", err)
	}
}
