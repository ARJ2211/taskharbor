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

func TestExtendLease_InvalidDuration(t *testing.T) {
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

	d, err := NewWithPool(pool)
	if err != nil {
		t.Fatalf("NewWithPool: %v", err)
	}

	_, err = d.ExtendLease(ctx, "x", "t", time.Now().UTC(), 0)
	if err != driver.ErrInvalidLeaseDuration {
		t.Fatalf("expected ErrInvalidLeaseDuration, got %v", err)
	}
}

func TestExtendLease_NotInflight(t *testing.T) {
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
		t.Fatalf("truncate: %v", err)
	}

	d, err := NewWithPool(pool)
	if err != nil {
		t.Fatalf("NewWithPool: %v", err)
	}

	now := time.Date(2026, 2, 5, 12, 0, 0, 0, time.UTC)

	rec := driver.JobRecord{
		ID:          "job1",
		Type:        "t",
		Queue:       "default",
		Payload:     []byte(`{}`),
		CreatedAt:   now,
		MaxAttempts: 3,
	}

	if _, _, err := d.Enqueue(ctx, rec); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	_, err = d.ExtendLease(ctx, rec.ID, "tok", now, 5*time.Second)
	if err != driver.ErrJobNotInflight {
		t.Fatalf("expected ErrJobNotInflight, got %v", err)
	}
}

func TestExtendLease_MismatchExpiredAndSuccess(t *testing.T) {
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
		t.Fatalf("truncate: %v", err)
	}

	d, err := NewWithPool(pool)
	if err != nil {
		t.Fatalf("NewWithPool: %v", err)
	}

	t0 := time.Date(2026, 2, 5, 12, 0, 0, 0, time.UTC)

	rec := driver.JobRecord{
		ID:          "job2",
		Type:        "t",
		Queue:       "default",
		Payload:     []byte(`{}`),
		CreatedAt:   t0,
		MaxAttempts: 3,
	}

	if _, _, err := d.Enqueue(ctx, rec); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	_, lease, ok, err := d.Reserve(ctx, "default", t0, 2*time.Second)
	if err != nil || !ok {
		t.Fatalf("Reserve: ok=%v err=%v", ok, err)
	}

	// Token mismatch (lease still valid)
	_, err = d.ExtendLease(ctx, rec.ID, driver.LeaseToken("wrong"), t0.Add(1*time.Second), 5*time.Second)
	if err != driver.ErrLeaseMismatch {
		t.Fatalf("expected ErrLeaseMismatch, got %v", err)
	}

	// Expired takes precedence over mismatch
	_, err = d.ExtendLease(ctx, rec.ID, driver.LeaseToken("wrong"), t0.Add(3*time.Second), 5*time.Second)
	if err != driver.ErrLeaseExpired {
		t.Fatalf("expected ErrLeaseExpired, got %v", err)
	}

	// Re-reserve after expiry to get a fresh lease, then extend successfully
	_, lease2, ok, err := d.Reserve(ctx, "default", t0.Add(3*time.Second), 2*time.Second)
	if err != nil || !ok {
		t.Fatalf("Reserve2: ok=%v err=%v", ok, err)
	}

	newLease, err := d.ExtendLease(ctx, rec.ID, lease2.Token, t0.Add(4*time.Second), 10*time.Second)
	if err != nil {
		t.Fatalf("ExtendLease success: %v", err)
	}
	if newLease.Token != lease2.Token {
		t.Fatalf("expected same token, got %v vs %v", newLease.Token, lease2.Token)
	}
	if !newLease.ExpiresAt.Equal(t0.Add(14 * time.Second)) {
		t.Fatalf("expected expiry %v got %v", t0.Add(14*time.Second), newLease.ExpiresAt)
	}

	_ = lease // keep compiler happy if you remove earlier checks
}
