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

func newTestPoolAndDriver(t *testing.T) (context.Context, *pgxpool.Pool, *Driver) {
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

	// Avoid TRUNCATE (lock-heavy); DELETE is enough for tests.
	if _, err := pool.Exec(ctx, `DELETE FROM th_jobs`); err != nil {
		t.Fatalf("cleanup: %v", err)
	}

	d, err := NewWithPool(pool)
	if err != nil {
		t.Fatalf("NewWithPool: %v", err)
	}

	return ctx, pool, d
}

func TestFail_NotInflight(t *testing.T) {
	ctx, _, d := newTestPoolAndDriver(t)

	now := time.Date(2026, 2, 5, 12, 0, 0, 0, time.UTC)

	rec := driver.JobRecord{
		ID:          "job_fail_ready",
		Type:        "t",
		Queue:       "default",
		Payload:     []byte(`{}`),
		CreatedAt:   now,
		MaxAttempts: 3,
	}

	if _, _, err := d.Enqueue(ctx, rec); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	err := d.Fail(ctx, rec.ID, driver.LeaseToken("tok"), now, "reason")
	if err != driver.ErrJobNotInflight {
		t.Fatalf("expected ErrJobNotInflight, got %v", err)
	}
}

func TestFail_TokenMismatch(t *testing.T) {
	ctx, _, d := newTestPoolAndDriver(t)

	t0 := time.Date(2026, 2, 5, 12, 0, 0, 0, time.UTC)

	rec := driver.JobRecord{
		ID:          "job_fail_mismatch",
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

	err = d.Fail(ctx, rec.ID, driver.LeaseToken("wrong"), t0.Add(1*time.Second), "reason")
	if err != driver.ErrLeaseMismatch {
		t.Fatalf("expected ErrLeaseMismatch, got %v", err)
	}

	_ = lease
}

func TestFail_ExpiredTakesPrecedenceOverMismatch(t *testing.T) {
	ctx, _, d := newTestPoolAndDriver(t)

	t0 := time.Date(2026, 2, 5, 12, 0, 0, 0, time.UTC)

	rec := driver.JobRecord{
		ID:          "job_fail_expired",
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

	// After expiry, expired should be returned even if token is wrong.
	err = d.Fail(ctx, rec.ID, driver.LeaseToken("wrong"), t0.Add(3*time.Second), "reason")
	if err != driver.ErrLeaseExpired {
		t.Fatalf("expected ErrLeaseExpired, got %v", err)
	}

	_ = lease
}

func TestFail_SuccessMovesToDLQAndClearsLease(t *testing.T) {
	ctx, pool, d := newTestPoolAndDriver(t)

	t0 := time.Date(2026, 2, 5, 12, 0, 0, 0, time.UTC)

	rec := driver.JobRecord{
		ID:          "job_fail_success",
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

	failNow := t0.Add(1 * time.Second)
	reason := "boom"

	if err := d.Fail(ctx, rec.ID, lease.Token, failNow, reason); err != nil {
		t.Fatalf("Fail: %v", err)
	}

	// Verify DB state
	var status string
	var dlqReason *string
	var dlqFailedAt *time.Time
	var leaseTok *string
	var leaseExp *time.Time

	row := pool.QueryRow(ctx, `
		SELECT status, dlq_reason, dlq_failed_at, lease_token, lease_expires_at
		FROM th_jobs
		WHERE id = $1
	`, rec.ID)

	if err := row.Scan(&status, &dlqReason, &dlqFailedAt, &leaseTok, &leaseExp); err != nil {
		t.Fatalf("scan: %v", err)
	}

	if status != "dlq" {
		t.Fatalf("expected status dlq, got %s", status)
	}
	if dlqReason == nil || *dlqReason != reason {
		t.Fatalf("expected dlq_reason %q, got %v", reason, dlqReason)
	}
	if dlqFailedAt == nil || !dlqFailedAt.UTC().Equal(failNow.UTC()) {
		t.Fatalf("expected dlq_failed_at %v, got %v", failNow.UTC(), dlqFailedAt)
	}
	if leaseTok != nil || leaseExp != nil {
		t.Fatalf("expected lease cleared, got token=%v exp=%v", leaseTok, leaseExp)
	}

	// Verify not reservable anymore
	_, _, ok2, err := d.Reserve(ctx, "default", t0.Add(2*time.Second), 5*time.Second)
	if err != nil {
		t.Fatalf("Reserve after dlq: %v", err)
	}
	if ok2 {
		t.Fatalf("expected ok=false after dlq")
	}
}
