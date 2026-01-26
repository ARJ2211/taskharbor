package memory

import (
	"context"
	"testing"
	"time"

	"github.com/ARJ2211/taskharbor/taskharbor/driver"
)

func TestMemory_NoDoubleDeliverDuringValidLease(t *testing.T) {
	ctx := context.Background()
	d := New()

	t0 := time.Date(2026, 1, 20, 10, 0, 0, 0, time.UTC)

	rec := driver.JobRecord{
		ID:        "job-lease-nodouble-1",
		Type:      "task.once",
		Payload:   []byte(`{}`),
		Queue:     "default",
		CreatedAt: t0,
	}

	if err := d.Enqueue(ctx, rec); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	// Reserve job with a 10s lease.
	_, lease1, ok, err := d.Reserve(ctx, "default", t0, 10*time.Second)
	if err != nil {
		t.Fatalf("reserve failed: %v", err)
	}
	if !ok {
		t.Fatalf("expected ok=true, got ok=false")
	}
	if lease1.Token == "" {
		t.Fatalf("expected non-empty lease token")
	}
	if !lease1.ExpiresAt.Equal(t0.Add(10 * time.Second)) {
		t.Fatalf("expected expiresAt=%v, got %v", t0.Add(10*time.Second), lease1.ExpiresAt)
	}

	// Before expiry, it must not be reservable again
	_, _, ok, err = d.Reserve(ctx, "default", t0.Add(5*time.Second), 10*time.Second)
	if err != nil {
		t.Fatalf("reserve before expiry failed: %v", err)
	}
	if ok {
		t.Fatalf("expected ok=false before lease expiry, got ok=true")
	}

	// After expiry, it should be reclaimed and delivered again with a new lease
	_, lease2, ok, err := d.Reserve(ctx, "default", t0.Add(11*time.Second), 10*time.Second)
	if err != nil {
		t.Fatalf("reserve after expiry failed: %v", err)
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

	// Clean up
	if err := d.Ack(ctx, rec.ID, lease2.Token, t0.Add(11*time.Second)); err != nil {
		t.Fatalf("ack failed: %v", err)
	}
}

func TestMemory_HeartbeatPreventsReclaim(t *testing.T) {
	ctx := context.Background()
	d := New()

	t0 := time.Date(2026, 1, 20, 10, 0, 0, 0, time.UTC)

	rec := driver.JobRecord{
		ID:        "job-lease-heartbeat-1",
		Type:      "task.once",
		Payload:   []byte(`{}`),
		Queue:     "default",
		CreatedAt: t0,
	}

	if err := d.Enqueue(ctx, rec); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	// Reserve with 10s lease => expires at t0+10
	_, lease, ok, err := d.Reserve(ctx, "default", t0, 10*time.Second)
	if err != nil {
		t.Fatalf("reserve failed: %v", err)
	}
	if !ok {
		t.Fatalf("expected ok=true, got ok=false")
	}

	// Heartbeat at t0+8 => new expiry t0+18
	lease, err = d.ExtendLease(ctx, rec.ID, lease.Token, t0.Add(8*time.Second), 10*time.Second)
	if err != nil {
		t.Fatalf("extend lease failed: %v", err)
	}
	if !lease.ExpiresAt.Equal(t0.Add(18 * time.Second)) {
		t.Fatalf("expected expiresAt=%v, got %v", t0.Add(18*time.Second), lease.ExpiresAt)
	}

	// After original expiry (t0+10) but before extended expiry (t0+18),
	// reserve must NOT reclaim or deliver the job.
	_, _, ok, err = d.Reserve(ctx, "default", t0.Add(11*time.Second), 10*time.Second)
	if err != nil {
		t.Fatalf("reserve failed: %v", err)
	}
	if ok {
		t.Fatalf("expected ok=false (lease still valid), got ok=true")
	}

	// Another heartbeat at t0+17 => new expiry t0+27
	lease, err = d.ExtendLease(ctx, rec.ID, lease.Token, t0.Add(17*time.Second), 10*time.Second)
	if err != nil {
		t.Fatalf("extend lease failed: %v", err)
	}
	if !lease.ExpiresAt.Equal(t0.Add(27 * time.Second)) {
		t.Fatalf("expected expiresAt=%v, got %v", t0.Add(27*time.Second), lease.ExpiresAt)
	}

	// Still before extended expiry: no reclaim.
	_, _, ok, err = d.Reserve(ctx, "default", t0.Add(19*time.Second), 10*time.Second)
	if err != nil {
		t.Fatalf("reserve failed: %v", err)
	}
	if ok {
		t.Fatalf("expected ok=false (lease still valid), got ok=true")
	}

	// After latest expiry: should reclaim and deliver again with a new token.
	_, lease2, ok, err := d.Reserve(ctx, "default", t0.Add(28*time.Second), 10*time.Second)
	if err != nil {
		t.Fatalf("reserve after expiry failed: %v", err)
	}
	if !ok {
		t.Fatalf("expected ok=true after expiry, got ok=false")
	}
	if lease2.Token == "" {
		t.Fatalf("expected non-empty lease token")
	}
	if lease2.Token == lease.Token {
		t.Fatalf("expected new lease token after reclaim")
	}

	// Cleanup
	if err := d.Ack(ctx, rec.ID, lease2.Token, t0.Add(28*time.Second)); err != nil {
		t.Fatalf("ack failed: %v", err)
	}
}
