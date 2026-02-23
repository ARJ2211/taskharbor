package memory

import (
	"context"
	"testing"
	"time"

	"github.com/ARJ2211/taskharbor/taskharbor/driver"
)

func TestAdmin_Inspect_List_RequeueDLQ(t *testing.T) {
	d := New()
	ctx := context.Background()
	now := time.Now().UTC()
	q := "q0"

	recReady := driver.JobRecord{
		ID:        "job_ready",
		Type:      "echo",
		Queue:     q,
		CreatedAt: now.Add(-3 * time.Second),
	}
	if _, _, err := d.Enqueue(ctx, recReady); err != nil {
		t.Fatal(err)
	}

	recSched := driver.JobRecord{
		ID:        "job_sched",
		Type:      "echo",
		Queue:     q,
		RunAt:     now.Add(10 * time.Second),
		CreatedAt: now.Add(-2 * time.Second),
	}
	if _, _, err := d.Enqueue(ctx, recSched); err != nil {
		t.Fatal(err)
	}

	// Inspect ready
	ji, err := d.Inspect(ctx, "job_ready", now)
	if err != nil {
		t.Fatal(err)
	}
	if ji.State != driver.StateReady {
		t.Fatalf("expected ready, got %s", ji.State)
	}

	// Reserve -> inflight
	r, lease, ok, err := d.Reserve(ctx, q, now, 5*time.Second)
	if err != nil || !ok {
		t.Fatalf("reserve err=%v ok=%v", err, ok)
	}
	if r.ID != "job_ready" {
		t.Fatalf("expected job_ready reserved, got %s", r.ID)
	}

	ji, err = d.Inspect(ctx, "job_ready", now)
	if err != nil {
		t.Fatal(err)
	}
	if ji.State != driver.StateInflight || ji.Lease == nil {
		t.Fatalf("expected inflight w/ lease, got state=%s lease=%v", ji.State, ji.Lease)
	}

	// Fail -> DLQ
	if err := d.Fail(ctx, "job_ready", lease.Token, now, "boom"); err != nil {
		t.Fatal(err)
	}
	ji, err = d.Inspect(ctx, "job_ready", now)
	if err != nil {
		t.Fatal(err)
	}
	if ji.State != driver.StateDLQ || ji.DLQ == nil {
		t.Fatalf("expected dlq w/ info, got state=%s dlq=%v", ji.State, ji.DLQ)
	}

	// Requeue DLQ -> ready
	if err := d.RequeueDLQ(ctx, "job_ready", now, driver.RequeueOptions{Queue: q, ResetAttempts: true}); err != nil {
		t.Fatal(err)
	}
	ji, err = d.Inspect(ctx, "job_ready", now)
	if err != nil {
		t.Fatal(err)
	}
	if ji.State != driver.StateReady {
		t.Fatalf("expected ready after requeue, got %s", ji.State)
	}

	// Reserve + Ack -> done
	r2, lease2, ok, err := d.Reserve(ctx, q, now, 5*time.Second)
	if err != nil || !ok {
		t.Fatalf("reserve2 err=%v ok=%v", err, ok)
	}
	if r2.ID != "job_ready" {
		t.Fatalf("expected job_ready reserved again, got %s", r2.ID)
	}
	if err := d.Ack(ctx, "job_ready", lease2.Token, now); err != nil {
		t.Fatal(err)
	}

	ji, err = d.Inspect(ctx, "job_ready", now)
	if err != nil {
		t.Fatal(err)
	}
	if ji.State != driver.StateDone {
		t.Fatalf("expected done, got %s", ji.State)
	}

	// List all states in queue
	page, err := d.List(ctx, driver.ListRequest{Queue: q, Now: now, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Jobs) == 0 {
		t.Fatalf("expected some jobs in list")
	}
}

func TestAdmin_List_Pagination(t *testing.T) {
	d := New()
	ctx := context.Background()
	now := time.Now().UTC()
	q := "q0"

	for i := 0; i < 3; i++ {
		rec := driver.JobRecord{
			ID:        "job_" + string(rune('a'+i)),
			Type:      "echo",
			Queue:     q,
			CreatedAt: now.Add(time.Duration(i) * time.Second),
		}
		if _, _, err := d.Enqueue(ctx, rec); err != nil {
			t.Fatal(err)
		}
	}

	p1, err := d.List(ctx, driver.ListRequest{Queue: q, Now: now, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(p1.Jobs) != 2 || p1.NextCursor == "" {
		t.Fatalf("expected 2 jobs + cursor, got %d cursor=%q", len(p1.Jobs), p1.NextCursor)
	}

	p2, err := d.List(ctx, driver.ListRequest{Queue: q, Now: now, Limit: 2, Cursor: p1.NextCursor})
	if err != nil {
		t.Fatal(err)
	}
	if len(p2.Jobs) != 1 {
		t.Fatalf("expected 1 job on second page, got %d", len(p2.Jobs))
	}
}
