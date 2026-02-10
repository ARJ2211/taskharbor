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

func TestRetry_NotInflight(t *testing.T) {
	wd, _ := os.Getwd()
	_ = envutil.LoadRepoDotenv(wd)
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
	if _, err := pool.Exec(ctx, `DELETE FROM th_jobs`); err != nil {
		t.Fatalf("cleanup: %v", err)
	}

	d, err := NewWithPool(pool)
	if err != nil {
		t.Fatalf("NewWithPool: %v", err)
	}

	now := time.Date(2026, 2, 5, 12, 0, 0, 0, time.UTC)

	rec := driver.JobRecord{
		ID:          "job_retry_ready",
		Type:        "t",
		Queue:       "default",
		Payload:     []byte(`{}`),
		CreatedAt:   now,
		MaxAttempts: 3,
	}

	if _, _, err := d.Enqueue(ctx, rec); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	err = d.Retry(ctx, rec.ID, "tok", now, driver.RetryUpdate{
		Attempts:  1,
		LastError: "x",
		FailedAt:  now,
		RunAt:     time.Time{},
	})
	if err != driver.ErrJobNotInflight {
		t.Fatalf("expected ErrJobNotInflight, got %v", err)
	}
}

func TestRetry_Success_ImmediateAndScheduled(t *testing.T) {
	wd, _ := os.Getwd()
	_ = envutil.LoadRepoDotenv(wd)
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
	if _, err := pool.Exec(ctx, `DELETE FROM th_jobs`); err != nil {
		t.Fatalf("cleanup: %v", err)
	}

	d, err := NewWithPool(pool)
	if err != nil {
		t.Fatalf("NewWithPool: %v", err)
	}

	t0 := time.Date(2026, 2, 5, 12, 0, 0, 0, time.UTC)

	rec := driver.JobRecord{
		ID:          "job_retry_1",
		Type:        "t",
		Queue:       "default",
		Payload:     []byte(`{}`),
		CreatedAt:   t0,
		MaxAttempts: 3,
	}

	if _, _, err := d.Enqueue(ctx, rec); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	_, lease, ok, err := d.Reserve(ctx, "default", t0, 5*time.Second)
	if err != nil || !ok {
		t.Fatalf("Reserve: ok=%v err=%v", ok, err)
	}

	// Retry immediately (RunAt zero -> NULL)
	err = d.Retry(ctx, rec.ID, lease.Token, t0.Add(1*time.Second), driver.RetryUpdate{
		Attempts:  1,
		LastError: "boom",
		FailedAt:  t0.Add(1 * time.Second),
		RunAt:     time.Time{},
	})
	if err != nil {
		t.Fatalf("Retry immediate: %v", err)
	}

	// Should be reservable right away (and gives us the lease we need for the next Retry)
	_, lease2, ok2, err := d.Reserve(ctx, "default", t0.Add(2*time.Second), 5*time.Second)
	if err != nil || !ok2 {
		t.Fatalf("Reserve after retry: ok=%v err=%v", ok2, err)
	}

	// Now schedule via Retry (future RunAt) using the same inflight lease
	future := t0.Add(30 * time.Second)
	err = d.Retry(ctx, rec.ID, lease2.Token, t0.Add(4*time.Second), driver.RetryUpdate{
		Attempts:  2,
		LastError: "again",
		FailedAt:  t0.Add(4 * time.Second),
		RunAt:     future,
	})
	if err != nil {
		t.Fatalf("Retry scheduled: %v", err)
	}

	// Not reservable before due
	_, _, ok3, err := d.Reserve(ctx, "default", t0.Add(10*time.Second), 5*time.Second)
	if err != nil {
		t.Fatalf("Reserve before due: %v", err)
	}
	if ok3 {
		t.Fatalf("expected ok=false before run_at")
	}

	// Reservable when due
	_, _, ok4, err := d.Reserve(ctx, "default", future, 5*time.Second)
	if err != nil || !ok4 {
		t.Fatalf("Reserve at due: ok=%v err=%v", ok4, err)
	}

}
