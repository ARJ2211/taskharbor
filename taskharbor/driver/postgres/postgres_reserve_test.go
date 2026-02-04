package postgres

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/ARJ2211/taskharbor/taskharbor/driver"
	"github.com/ARJ2211/taskharbor/taskharbor/internal/envutil"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestReserve_NoJobs(t *testing.T) {
	_ = envutil.LoadRepoDotenv(".")
	dsn := os.Getenv("TASKHARBOR_TEST_DSN")
	if dsn == "" {
		t.Skip("TASKHARBOR_TEST_DSN not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	defer pool.Close()

	if err := ApplyMigrations(ctx, pool); err != nil {
		t.Fatalf("ApplyMigrations: %v", err)
	}
	if _, err := pool.Exec(ctx, `TRUNCATE th_jobs`); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	d, err := NewWithPool(pool)
	if err != nil {
		t.Fatalf("NewWithPool: %v", err)
	}

	_, _, ok, err := d.Reserve(ctx, "default", time.Now().UTC(), 5*time.Second)
	if err != nil {
		t.Fatalf("Reserve err: %v", err)
	}
	if ok {
		t.Fatalf("expected ok=false")
	}
}

func TestReserve_NoDoubleReserve_AndReclaim(t *testing.T) {
	_ = envutil.LoadRepoDotenv(".")
	dsn := os.Getenv("TASKHARBOR_TEST_DSN")
	if dsn == "" {
		t.Skip("TASKHARBOR_TEST_DSN not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	defer pool.Close()

	if err := ApplyMigrations(ctx, pool); err != nil {
		t.Fatalf("ApplyMigrations: %v", err)
	}
	if _, err := pool.Exec(ctx, `TRUNCATE th_jobs`); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	d, err := NewWithPool(pool)
	if err != nil {
		t.Fatalf("NewWithPool: %v", err)
	}

	now1 := time.Now().UTC()

	// Due scheduled job (run_at in the past) so first reserve returns a non-zero RunAt.
	rec := driver.JobRecord{
		ID:          "job_sched_1",
		Type:        "t",
		Queue:       "default",
		Payload:     []byte(`{}`),
		RunAt:       now1.Add(-1 * time.Minute),
		Timeout:     1 * time.Second,
		CreatedAt:   now1,
		Attempts:    0,
		MaxAttempts: 3,
	}

	if err := d.Enqueue(ctx, rec); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	r1, l1, ok, err := d.Reserve(ctx, "default", now1, 1*time.Second)
	if err != nil {
		t.Fatalf("Reserve#1: %v", err)
	}
	if !ok {
		t.Fatalf("expected ok=true")
	}
	if r1.ID != rec.ID {
		t.Fatalf("expected %s got %s", rec.ID, r1.ID)
	}
	if r1.RunAt.IsZero() {
		t.Fatalf("expected first reserve to keep original run_at (scheduled due)")
	}

	// second reserve while lease valid should return ok=false
	_, _, ok2, err := d.Reserve(ctx, "default", now1, 1*time.Second)
	if err != nil {
		t.Fatalf("Reserve#2: %v", err)
	}
	if ok2 {
		t.Fatalf("expected ok=false while lease is valid")
	}

	// reclaim after lease expiry (advance 'now' manually)
	now2 := now1.Add(2 * time.Second)
	r2, l2, ok3, err := d.Reserve(ctx, "default", now2, 1*time.Second)
	if err != nil {
		t.Fatalf("Reserve#3 (reclaim): %v", err)
	}
	if !ok3 {
		t.Fatalf("expected ok=true after expiry")
	}
	if r2.ID != rec.ID {
		t.Fatalf("expected reclaimed job id %s got %s", rec.ID, r2.ID)
	}
	if !r2.RunAt.IsZero() {
		t.Fatalf("expected reclaim to clear run_at to zero (NULL in DB), got %v", r2.RunAt)
	}
	if l1.Token == l2.Token {
		t.Fatalf("expected new lease token after reclaim")
	}
	if !l2.ExpiresAt.After(now2) {
		t.Fatalf("expected lease expiry after now2")
	}
}
