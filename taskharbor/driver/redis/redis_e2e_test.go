package redis

import (
	"context"
	"testing"
	"time"

	"github.com/ARJ2211/taskharbor/taskharbor/driver"
)

// Full flow: enqueue -> reserve -> ack. Then reserve again gets nothing.
func TestE2E_EnqueueReserveAck(t *testing.T) {
	d, queue := testRedis(t)
	ctx := context.Background()

	rec := driver.JobRecord{
		ID:          "e2e-job-1",
		Type:        "test",
		Queue:       queue,
		Payload:     []byte(`{"x":1}`),
		RunAt:       time.Time{},
		Timeout:     time.Second,
		CreatedAt:   time.Now().UTC(),
		Attempts:    0,
		MaxAttempts: 3,
	}
	if err := rec.Validate(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := d.Enqueue(ctx, rec); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	got, lease, ok, err := d.Reserve(ctx, queue, now, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected one job")
	}
	if got.ID != rec.ID || got.Type != rec.Type {
		t.Errorf("got %q %q, want %q %q", got.ID, got.Type, rec.ID, rec.Type)
	}
	if lease.Token == "" || !lease.ExpiresAt.After(now) {
		t.Errorf("bad lease")
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
