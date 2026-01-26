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

	// Reserve job with a 10s lease
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
