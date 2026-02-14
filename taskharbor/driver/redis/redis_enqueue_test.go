package redis

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/ARJ2211/taskharbor/taskharbor/driver"
)

func TestNew_InvalidAddr(t *testing.T) {
	ctx := context.Background()
	_, err := New(ctx, "localhost:0")
	if err == nil {
		t.Fatal("expected error when connecting to invalid address")
	}
}

func TestEnqueue_ValidationRejectsEmptyType(t *testing.T) {
	d, queue := testRedis(t)
	ctx := context.Background()

	bad := driver.JobRecord{
		ID:    "x",
		Type:  "",
		Queue: queue,
	}
	_, _, err := d.Enqueue(ctx, bad)
	if err == nil {
		t.Fatal("expected validation error for empty type")
	}
	if !errors.Is(err, driver.ErrJobTypeRequired) {
		t.Fatalf("expected ErrJobTypeRequired, got %v", err)
	}
}

func TestEnqueue_ThenReserveReturnsJob(t *testing.T) {
	d, queue := testRedis(t)
	ctx := context.Background()

	rec := driver.JobRecord{
		ID:          "job-enq-1",
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

	got, lease, ok, err := d.Reserve(ctx, queue, time.Now().UTC(), 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected one job reserved")
	}
	if got.ID != rec.ID || got.Type != rec.Type {
		t.Errorf("got %q %q, want %q %q", got.ID, got.Type, rec.ID, rec.Type)
	}
	if lease.Token == "" {
		t.Error("expected non-empty lease token")
	}
	_ = lease
}

func TestClosedDriver_EnqueueReturnsError(t *testing.T) {
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

	_, _, err = dr.Enqueue(ctx, driver.JobRecord{ID: "x", Type: "t", Queue: "closed-test", CreatedAt: time.Now()})
	if err == nil {
		t.Fatal("expected error when enqueue on closed driver")
	}
	if !errors.Is(err, ErrDriverClosed) {
		t.Fatalf("expected ErrDriverClosed, got %v", err)
	}
}
