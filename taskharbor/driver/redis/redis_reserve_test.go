package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ARJ2211/taskharbor/taskharbor/driver"
)

func TestReserve_NoJobsReturnsOkFalse(t *testing.T) {
	d, queue := testRedis(t)
	ctx := context.Background()

	_, _, ok, err := d.Reserve(ctx, queue, time.Now().UTC(), 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected ok=false when no jobs")
	}
}

func TestReserve_InvalidLeaseDuration(t *testing.T) {
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

func TestReserve_SchedulePromotion(t *testing.T) {
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
	if _, _, err := d.Enqueue(ctx, rec); err != nil {
		t.Fatal(err)
	}

	_, _, ok, err := d.Reserve(ctx, queue, t0, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected ok=false before runAt")
	}

	got, _, ok, err := d.Reserve(ctx, queue, runAt, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected ok=true at runAt")
	}
	if got.ID != rec.ID {
		t.Errorf("got id %q, want %q", got.ID, rec.ID)
	}
}

func TestReserve_NoDoubleDeliverWhileInflight(t *testing.T) {
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
	if _, _, err := d.Enqueue(ctx, rec); err != nil {
		t.Fatal(err)
	}

	got1, _, ok, err := d.Reserve(ctx, queue, now, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || got1.ID != rec.ID {
		t.Fatalf("expected job %q, got ok=%v id=%q", rec.ID, ok, got1.ID)
	}

	_, _, ok, err = d.Reserve(ctx, queue, now, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected ok=false when only job is inflight")
	}
}

func TestReserve_ReclaimExpiredLease(t *testing.T) {
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
	if _, _, err := d.Enqueue(ctx, rec); err != nil {
		t.Fatal(err)
	}

	_, lease1, ok, err := d.Reserve(ctx, queue, t0, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected ok=true")
	}

	_, _, ok, err = d.Reserve(ctx, queue, t0.Add(5*time.Second), 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected ok=false before lease expiry")
	}

	_, lease2, ok, err := d.Reserve(ctx, queue, t0.Add(11*time.Second), 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected ok=true after reclaim")
	}
	if lease2.Token == lease1.Token {
		t.Fatal("expected new lease token after reclaim")
	}
}
