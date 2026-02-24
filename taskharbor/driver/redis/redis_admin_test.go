package redis

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/ARJ2211/taskharbor/taskharbor/driver"
	"github.com/ARJ2211/taskharbor/taskharbor/internal/envutil"
)

func TestRedisAdmin_Inspect_List_RequeueDLQ(t *testing.T) {
	cwd, _ := os.Getwd()
	_ = envutil.LoadRepoDotenv(cwd)

	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	d, err := New(ctx, addr)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = d.Close() }()

	// isolate keys
	d.opts.prefix = "taskharbor_admin_test:" + time.Now().UTC().Format("20060102150405.000000000")

	now := time.Now().UTC()
	q := "q0"

	recSched := driver.JobRecord{
		ID:          "admin_sched_1",
		Type:        "t",
		Queue:       q,
		Payload:     []byte(`{"x":1}`),
		RunAt:       now.Add(10 * time.Second),
		CreatedAt:   now.Add(-3 * time.Second),
		MaxAttempts: 3,
	}
	if _, _, err := d.Enqueue(ctx, recSched); err != nil {
		t.Fatalf("Enqueue sched: %v", err)
	}

	recDLQ := driver.JobRecord{
		ID:          "admin_dlq_1",
		Type:        "t",
		Queue:       q,
		Payload:     []byte(`{}`),
		CreatedAt:   now.Add(-2 * time.Second),
		MaxAttempts: 3,
	}
	if _, _, err := d.Enqueue(ctx, recDLQ); err != nil {
		t.Fatalf("Enqueue dlq: %v", err)
	}

	// Reserve + Fail -> DLQ
	_, lease, ok, err := d.Reserve(ctx, q, now, 5*time.Second)
	if err != nil || !ok {
		t.Fatalf("Reserve: ok=%v err=%v", ok, err)
	}
	if err := d.Fail(ctx, recDLQ.ID, lease.Token, now.Add(1*time.Second), "boom"); err != nil {
		t.Fatalf("Fail: %v", err)
	}

	jiSched, err := d.Inspect(ctx, recSched.ID, now)
	if err != nil {
		t.Fatalf("Inspect sched: %v", err)
	}
	if jiSched.State != driver.StateScheduled {
		t.Fatalf("expected scheduled, got %s", jiSched.State)
	}

	jiDLQ, err := d.Inspect(ctx, recDLQ.ID, now)
	if err != nil {
		t.Fatalf("Inspect dlq: %v", err)
	}
	if jiDLQ.State != driver.StateDLQ || jiDLQ.DLQ == nil {
		t.Fatalf("expected dlq with info, got state=%s dlq=%v", jiDLQ.State, jiDLQ.DLQ)
	}

	pageDLQ, err := d.List(ctx, driver.ListRequest{Queue: q, State: driver.StateDLQ, Now: now, Limit: 10})
	if err != nil {
		t.Fatalf("List dlq: %v", err)
	}
	if len(pageDLQ.Jobs) != 1 || pageDLQ.Jobs[0].ID != recDLQ.ID {
		t.Fatalf("expected dlq job %s, got %#v", recDLQ.ID, pageDLQ.Jobs)
	}

	// Requeue DLQ -> ready
	if err := d.RequeueDLQ(ctx, recDLQ.ID, now, driver.RequeueOptions{Queue: q, ResetAttempts: true}); err != nil {
		t.Fatalf("RequeueDLQ: %v", err)
	}
	jiReady, err := d.Inspect(ctx, recDLQ.ID, now)
	if err != nil {
		t.Fatalf("Inspect after requeue: %v", err)
	}
	if jiReady.State != driver.StateReady {
		t.Fatalf("expected ready, got %s", jiReady.State)
	}

	// Reserve + Ack -> done
	_, lease2, ok, err := d.Reserve(ctx, q, now.Add(2*time.Second), 5*time.Second)
	if err != nil || !ok {
		t.Fatalf("Reserve2: ok=%v err=%v", ok, err)
	}
	if err := d.Ack(ctx, recDLQ.ID, lease2.Token, now.Add(3*time.Second)); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	jiDone, err := d.Inspect(ctx, recDLQ.ID, now.Add(3*time.Second))
	if err != nil {
		t.Fatalf("Inspect done: %v", err)
	}
	if jiDone.State != driver.StateDone {
		t.Fatalf("expected done, got %s", jiDone.State)
	}

	pageDone, err := d.List(ctx, driver.ListRequest{Queue: q, State: driver.StateDone, Now: now, Limit: 10})
	if err != nil {
		t.Fatalf("List done: %v", err)
	}
	if len(pageDone.Jobs) != 1 || pageDone.Jobs[0].ID != recDLQ.ID {
		t.Fatalf("expected done job %s, got %#v", recDLQ.ID, pageDone.Jobs)
	}
}

func TestRedisAdmin_List_Pagination(t *testing.T) {
	cwd, _ := os.Getwd()
	_ = envutil.LoadRepoDotenv(cwd)

	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	d, err := New(ctx, addr)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = d.Close() }()

	d.opts.prefix = "taskharbor_admin_page_test:" + time.Now().UTC().Format("20060102150405.000000000")

	now := time.Now().UTC()
	q := "q0"

	for i := 0; i < 3; i++ {
		rec := driver.JobRecord{
			ID:        "admin_page_" + string(rune('a'+i)),
			Type:      "t",
			Queue:     q,
			Payload:   []byte(`{}`),
			CreatedAt: now.Add(time.Duration(i) * time.Second),
		}
		if _, _, err := d.Enqueue(ctx, rec); err != nil {
			t.Fatalf("Enqueue %d: %v", i, err)
		}
	}

	p1, err := d.List(ctx, driver.ListRequest{Queue: q, State: driver.StateReady, Now: now, Limit: 2})
	if err != nil {
		t.Fatalf("List p1: %v", err)
	}
	if len(p1.Jobs) != 2 || p1.NextCursor == "" {
		t.Fatalf("expected 2 jobs + cursor, got %d cursor=%q", len(p1.Jobs), p1.NextCursor)
	}

	p2, err := d.List(ctx, driver.ListRequest{Queue: q, State: driver.StateReady, Now: now, Limit: 2, Cursor: p1.NextCursor})
	if err != nil {
		t.Fatalf("List p2: %v", err)
	}
	if len(p2.Jobs) != 1 {
		t.Fatalf("expected 1 job on second page, got %d", len(p2.Jobs))
	}
}
