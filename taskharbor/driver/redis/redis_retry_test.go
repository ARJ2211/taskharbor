package redis

import (
	"context"
	"testing"
	"time"

	"github.com/ARJ2211/taskharbor/taskharbor/driver"
)

func TestRetry_MovesBackToScheduled(t *testing.T) {
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
	if _, _, err := d.Enqueue(ctx, rec); err != nil {
		t.Fatal(err)
	}

	_, lease, ok, err := d.Reserve(ctx, queue, t0, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected ok=true")
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
		t.Fatal("expected ok=false before retryAt")
	}

	got, _, ok, err := d.Reserve(ctx, queue, retryAt, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected ok=true at retryAt")
	}
	if got.Attempts != 1 {
		t.Errorf("expected attempts=1, got %d", got.Attempts)
	}
	if got.LastError != "boom" {
		t.Errorf("expected last_error=boom, got %q", got.LastError)
	}
}
