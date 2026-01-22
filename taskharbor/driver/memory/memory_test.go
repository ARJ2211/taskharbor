package memory

import (
	"context"
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

	got, ok, err := d.Reserve(ctx, "default", now)
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

	err = d.Ack(ctx, rec.ID)
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

	_, ok, err := d.Reserve(ctx, "default", t0)
	if err != nil {
		t.Fatalf("reserve failed: %v", err)
	}
	if ok {
		t.Fatalf("expected ok=false before runAt, got ok=true")
	}

	_, ok, err = d.Reserve(ctx, "default", runAt)
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

	_, ok, err := d.Reserve(ctx, "default", now)
	if err != nil {
		t.Fatalf("reserve failed: %v", err)
	}
	if !ok {
		t.Fatalf("expected ok=true, got ok=false")
	}

	err = d.Fail(ctx, rec.ID, "boom")
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

	got1, ok, err := d.Reserve(ctx, "default", now)
	if err != nil {
		t.Fatalf("reserve failed: %v", err)
	}
	if !ok {
		t.Fatalf("expected ok=true, got ok=false")
	}
	if got1.ID != rec.ID {
		t.Fatalf("expected id %s, got %s", rec.ID, got1.ID)
	}

	_, ok, err = d.Reserve(ctx, "default", now)
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

	_, ok, err := d.Reserve(ctx, "default", t0)
	if err != nil {
		t.Fatalf("reserve failed: %v", err)
	}
	if !ok {
		t.Fatalf("expected ok=true, got ok=false")
	}

	if err := d.Retry(ctx, rec.ID, driver.RetryUpdate{
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

	_, ok, err = d.Reserve(ctx, "default", t0)
	if err != nil {
		t.Fatalf("reserve failed: %v", err)
	}
	if ok {
		t.Fatalf("expected ok=false before retryAt, got ok=true")
	}

	got, ok, err := d.Reserve(ctx, "default", retryAt)
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
