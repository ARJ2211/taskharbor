package redis

import (
	"context"
	"testing"
	"time"

	"github.com/ARJ2211/taskharbor/taskharbor/driver"
)

func TestFail_MovesToDLQ(t *testing.T) {
	d, queue := testRedis(t)
	ctx := context.Background()

	now := time.Date(2026, 1, 20, 10, 0, 0, 0, time.UTC)
	rec := driver.JobRecord{
		ID:        "job-fail-1",
		Type:      "task.fail",
		Payload:   []byte(`{"x":1}`),
		Queue:     queue,
		RunAt:     time.Time{},
		CreatedAt: now,
	}
	if _, _, err := d.Enqueue(ctx, rec); err != nil {
		t.Fatal(err)
	}

	_, lease, ok, err := d.Reserve(ctx, queue, now, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected ok=true")
	}
	if err := d.Fail(ctx, rec.ID, lease.Token, now, "boom"); err != nil {
		t.Fatal(err)
	}

	_, _, ok, err = d.Reserve(ctx, queue, now, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected no job after fail (in DLQ)")
	}
}
