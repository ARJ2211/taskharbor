package redis

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ARJ2211/taskharbor/taskharbor/driver"
)

func TestAck_SuccessRemovesJob(t *testing.T) {
	d, queue := testRedis(t)
	ctx := context.Background()

	now := time.Now().UTC()
	rec := driver.JobRecord{
		ID:          "job-ack-1",
		Type:        "test",
		Queue:       queue,
		Payload:     []byte(`{}`),
		RunAt:       time.Time{},
		CreatedAt:   now,
		MaxAttempts: 3,
	}
	if _, _, err := d.Enqueue(ctx, rec); err != nil {
		t.Fatal(err)
	}

	got, lease, ok, err := d.Reserve(ctx, queue, now, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected ok=true")
	}

	if err := d.Ack(ctx, got.ID, lease.Token, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}

	_, _, ok, err = d.Reserve(ctx, queue, time.Now().UTC(), 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected no job after ack")
	}
}

func TestAck_LeaseMismatchRejected(t *testing.T) {
	d, queue := testRedis(t)
	ctx := context.Background()

	now := time.Date(2026, 1, 20, 10, 0, 0, 0, time.UTC)
	jobID := fmt.Sprintf("job-lease-mismatch-%d", now.UnixNano())
	rec := driver.JobRecord{
		ID:        jobID,
		Type:      "task.once",
		Payload:   []byte(`{}`),
		Queue:     queue,
		CreatedAt: now,
	}
	if _, _, err := d.Enqueue(ctx, rec); err != nil {
		t.Fatal(err)
	}

	_, lease, ok, err := d.Reserve(ctx, queue, now, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || lease.Token == "" {
		t.Fatal("expected reserved job with token")
	}

	err = d.Ack(ctx, rec.ID, driver.LeaseToken("wrong"), now)
	if err == nil {
		t.Fatal("expected error for lease mismatch")
	}
	if !errors.Is(err, driver.ErrLeaseMismatch) {
		t.Fatalf("expected ErrLeaseMismatch, got %v", err)
	}

	if err := d.Ack(ctx, rec.ID, lease.Token, now); err != nil {
		t.Fatalf("ack with correct token should succeed: %v", err)
	}
}
