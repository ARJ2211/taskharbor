package postgres

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ARJ2211/taskharbor/taskharbor/driver"
	"github.com/ARJ2211/taskharbor/taskharbor/internal/envutil"
	"github.com/jackc/pgx/v5/pgxpool"
)

func newAdminPoolAndDriver(t *testing.T) (context.Context, *pgxpool.Pool, *Driver) {
	t.Helper()

	wd, _ := os.Getwd()
	_ = envutil.LoadRepoDotenv(wd)

	dsn := os.Getenv("TASKHARBOR_TEST_DSN")
	if dsn == "" {
		t.Skip("TASKHARBOR_TEST_DSN not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)

	if err := ApplyMigrations(ctx, pool); err != nil {
		t.Fatalf("ApplyMigrations: %v", err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM th_jobs`); err != nil {
		t.Fatalf("cleanup: %v", err)
	}

	d, err := NewWithPool(pool)
	if err != nil {
		t.Fatalf("NewWithPool: %v", err)
	}
	return ctx, pool, d
}

func TestPostgresAdmin_Inspect_List_RequeueDLQ(t *testing.T) {
	ctx, pool, d := newAdminPoolAndDriver(t)
	_ = pool

	now := time.Date(2026, 2, 23, 12, 0, 0, 0, time.UTC)

	// scheduled job
	recSched := driver.JobRecord{
		ID:          "admin_sched_1",
		Type:        "t",
		Queue:       "default",
		Payload:     []byte(`{"x":1}`),
		RunAt:       now.Add(30 * time.Second),
		CreatedAt:   now.Add(-3 * time.Second),
		MaxAttempts: 3,
	}
	if _, _, err := d.Enqueue(ctx, recSched); err != nil {
		t.Fatalf("Enqueue sched: %v", err)
	}

	// job we'll push to DLQ with attempts>0, then requeue with reset
	recDLQ := driver.JobRecord{
		ID:          "admin_dlq_1",
		Type:        "t",
		Queue:       "default",
		Payload:     []byte(`{}`),
		CreatedAt:   now.Add(-2 * time.Second),
		MaxAttempts: 3,
	}
	if _, _, err := d.Enqueue(ctx, recDLQ); err != nil {
		t.Fatalf("Enqueue dlq: %v", err)
	}

	// reserve + retry to bump attempts
	_, lease1, ok, err := d.Reserve(ctx, "default", now, 5*time.Second)
	if err != nil || !ok {
		t.Fatalf("Reserve#1: ok=%v err=%v", ok, err)
	}
	if err := d.Retry(ctx, recDLQ.ID, lease1.Token, now.Add(1*time.Second), driver.RetryUpdate{
		Attempts:  1,
		LastError: "boom",
		FailedAt:  now.Add(1 * time.Second),
		RunAt:     time.Time{},
	}); err != nil {
		t.Fatalf("Retry: %v", err)
	}

	// reserve again + fail to DLQ
	_, lease2, ok, err := d.Reserve(ctx, "default", now.Add(2*time.Second), 5*time.Second)
	if err != nil || !ok {
		t.Fatalf("Reserve#2: ok=%v err=%v", ok, err)
	}
	if err := d.Fail(ctx, recDLQ.ID, lease2.Token, now.Add(3*time.Second), "max attempts reached"); err != nil {
		t.Fatalf("Fail: %v", err)
	}

	// Inspect scheduled
	ji, err := d.Inspect(ctx, recSched.ID, now)
	if err != nil {
		t.Fatalf("Inspect sched: %v", err)
	}
	if ji.State != driver.StateScheduled {
		t.Fatalf("expected scheduled, got %s", ji.State)
	}

	// Inspect dlq
	ji2, err := d.Inspect(ctx, recDLQ.ID, now.Add(4*time.Second))
	if err != nil {
		t.Fatalf("Inspect dlq: %v", err)
	}
	if ji2.State != driver.StateDLQ || ji2.DLQ == nil {
		t.Fatalf("expected dlq with info, got state=%s dlq=%v", ji2.State, ji2.DLQ)
	}

	// List scheduled only
	pageSched, err := d.List(ctx, driver.ListRequest{
		Queue: "default",
		State: driver.StateScheduled,
		Now:   now,
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("List scheduled: %v", err)
	}
	if len(pageSched.Jobs) != 1 || pageSched.Jobs[0].ID != recSched.ID {
		t.Fatalf("expected 1 scheduled job %s, got %#v", recSched.ID, pageSched.Jobs)
	}

	// List dlq only
	pageDLQ, err := d.List(ctx, driver.ListRequest{
		Queue: "default",
		State: driver.StateDLQ,
		Now:   now,
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("List dlq: %v", err)
	}
	if len(pageDLQ.Jobs) != 1 || pageDLQ.Jobs[0].ID != recDLQ.ID {
		t.Fatalf("expected 1 dlq job %s, got %#v", recDLQ.ID, pageDLQ.Jobs)
	}

	// Requeue from DLQ with reset attempts
	if err := d.RequeueDLQ(ctx, recDLQ.ID, now.Add(5*time.Second), driver.RequeueOptions{
		Queue:         "default",
		ResetAttempts: true,
	}); err != nil {
		t.Fatalf("RequeueDLQ: %v", err)
	}

	ji3, err := d.Inspect(ctx, recDLQ.ID, now.Add(5*time.Second))
	if err != nil {
		t.Fatalf("Inspect after requeue: %v", err)
	}
	if ji3.State != driver.StateReady {
		t.Fatalf("expected ready after requeue, got %s", ji3.State)
	}
	if ji3.Record.Attempts != 0 || ji3.Record.LastError != "" {
		t.Fatalf("expected attempts reset, got attempts=%d last_error=%q", ji3.Record.Attempts, ji3.Record.LastError)
	}

	// Reservable again
	_, lease3, ok, err := d.Reserve(ctx, "default", now.Add(6*time.Second), 5*time.Second)
	if err != nil || !ok {
		t.Fatalf("Reserve after requeue: ok=%v err=%v", ok, err)
	}
	if err := d.Ack(ctx, recDLQ.ID, lease3.Token, now.Add(7*time.Second)); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	ji4, err := d.Inspect(ctx, recDLQ.ID, now.Add(8*time.Second))
	if err != nil {
		t.Fatalf("Inspect done: %v", err)
	}
	if ji4.State != driver.StateDone {
		t.Fatalf("expected done, got %s", ji4.State)
	}
}

func TestPostgresAdmin_List_Pagination(t *testing.T) {
	ctx, _, d := newAdminPoolAndDriver(t)
	now := time.Date(2026, 2, 23, 12, 0, 0, 0, time.UTC)

	for i := 0; i < 3; i++ {
		rec := driver.JobRecord{
			ID:        fmt.Sprintf("admin_page_%d", i),
			Type:      "t",
			Queue:     "default",
			Payload:   []byte(`{}`),
			CreatedAt: now.Add(time.Duration(i) * time.Second),
		}
		if _, _, err := d.Enqueue(ctx, rec); err != nil {
			t.Fatalf("Enqueue %d: %v", i, err)
		}
	}

	p1, err := d.List(ctx, driver.ListRequest{Queue: "default", Now: now, Limit: 2})
	if err != nil {
		t.Fatalf("List p1: %v", err)
	}
	if len(p1.Jobs) != 2 || p1.NextCursor == "" {
		t.Fatalf("expected 2 jobs + cursor, got %d cursor=%q", len(p1.Jobs), p1.NextCursor)
	}

	p2, err := d.List(ctx, driver.ListRequest{Queue: "default", Now: now, Limit: 2, Cursor: p1.NextCursor})
	if err != nil {
		t.Fatalf("List p2: %v", err)
	}
	if len(p2.Jobs) != 1 {
		t.Fatalf("expected 1 job on second page, got %d", len(p2.Jobs))
	}
}
