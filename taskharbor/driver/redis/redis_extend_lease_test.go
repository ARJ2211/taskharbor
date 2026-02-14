package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ARJ2211/taskharbor/taskharbor/driver"
)

func TestExtendLease_PreventsReclaim(t *testing.T) {
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
	if _, _, err := d.Enqueue(ctx, rec); err != nil {
		t.Fatal(err)
	}

	_, lease, ok, err := d.Reserve(ctx, queue, t0, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected ok=true")
	}

	newLease, err := d.ExtendLease(ctx, rec.ID, lease.Token, t0.Add(5*time.Second), 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !newLease.ExpiresAt.After(t0.Add(10 * time.Second)) {
		t.Errorf("extend should push expiry out: got %v", newLease.ExpiresAt)
	}

	_, _, ok, err = d.Reserve(ctx, queue, t0.Add(11*time.Second), 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected ok=false while lease extended")
	}

	if err := d.Ack(ctx, rec.ID, newLease.Token, t0.Add(11*time.Second)); err != nil {
		t.Fatal(err)
	}
}

func TestExtendLease_ExpiredReturnsError(t *testing.T) {
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
	if _, _, err := d.Enqueue(ctx, rec); err != nil {
		t.Fatal(err)
	}

	_, lease, ok, err := d.Reserve(ctx, queue, t0, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected ok=true")
	}

	_, err = d.ExtendLease(ctx, rec.ID, lease.Token, t0.Add(10*time.Second), 10*time.Second)
	if err == nil {
		t.Fatal("expected error when extending expired lease")
	}
	if !errors.Is(err, driver.ErrLeaseExpired) {
		t.Fatalf("expected ErrLeaseExpired, got %v", err)
	}

	_, lease2, ok, err := d.Reserve(ctx, queue, t0.Add(10*time.Second), 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected job reclaimable after extend failed")
	}
	if err := d.Ack(ctx, rec.ID, lease2.Token, t0.Add(10*time.Second)); err != nil {
		t.Fatal(err)
	}
}
