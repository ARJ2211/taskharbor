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

func newE2EPoolAndDriver(t *testing.T) (context.Context, *pgxpool.Pool, *Driver) {
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

func TestPostgresDriver_E2E_HappyPath(t *testing.T) {
	ctx, pool, d := newE2EPoolAndDriver(t)

	t0 := time.Date(2026, 2, 6, 12, 0, 0, 0, time.UTC)

	rec := driver.JobRecord{
		ID:          "e2e_happy_1",
		Type:        "t",
		Queue:       "default",
		Payload:     []byte(`{"ok":true}`),
		CreatedAt:   t0,
		MaxAttempts: 3,
	}

	if _, _, err := d.Enqueue(ctx, rec); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	r1, lease, ok, err := d.Reserve(ctx, "default", t0, 5*time.Second)
	if err != nil || !ok {
		t.Fatalf("Reserve: ok=%v err=%v", ok, err)
	}
	if r1.ID != rec.ID {
		t.Fatalf("reserved wrong job: got %s want %s", r1.ID, rec.ID)
	}

	if err := d.Ack(ctx, rec.ID, lease.Token, t0.Add(1*time.Second)); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	// Verify terminal state in DB (done + no lease)
	var status string
	var leaseTok *string
	var leaseExp *time.Time
	row := pool.QueryRow(ctx, `SELECT status, lease_token, lease_expires_at FROM th_jobs WHERE id=$1`, rec.ID)
	if err := row.Scan(&status, &leaseTok, &leaseExp); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if status != "done" {
		t.Fatalf("expected status done, got %s", status)
	}
	if leaseTok != nil || leaseExp != nil {
		t.Fatalf("expected lease cleared, got token=%v exp=%v", leaseTok, leaseExp)
	}

	// Not reservable anymore
	_, _, ok2, err := d.Reserve(ctx, "default", t0.Add(2*time.Second), 5*time.Second)
	if err != nil {
		t.Fatalf("Reserve after done: %v", err)
	}
	if ok2 {
		t.Fatalf("expected ok=false after done")
	}
}

func TestPostgresDriver_E2E_RetryScheduleThenAck(t *testing.T) {
	ctx, pool, d := newE2EPoolAndDriver(t)

	t0 := time.Date(2026, 2, 6, 12, 0, 0, 0, time.UTC)

	rec := driver.JobRecord{
		ID:          "e2e_retry_1",
		Type:        "t",
		Queue:       "default",
		Payload:     []byte(`{"x":1}`),
		CreatedAt:   t0,
		MaxAttempts: 3,
	}

	if _, _, err := d.Enqueue(ctx, rec); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	_, lease1, ok, err := d.Reserve(ctx, "default", t0, 5*time.Second)
	if err != nil || !ok {
		t.Fatalf("Reserve#1: ok=%v err=%v", ok, err)
	}

	future := t0.Add(30 * time.Second)

	// Worker fails and schedules retry in the future
	if err := d.Retry(ctx, rec.ID, lease1.Token, t0.Add(1*time.Second), driver.RetryUpdate{
		Attempts:  1,
		LastError: "boom",
		FailedAt:  t0.Add(1 * time.Second),
		RunAt:     future,
	}); err != nil {
		t.Fatalf("Retry: %v", err)
	}

	// Verify state after retry: ready + scheduled + no lease
	var status string
	var runAt *time.Time
	var leaseTok *string
	var leaseExp *time.Time
	var attempts int
	var lastErr string
	var failedAt *time.Time

	row := pool.QueryRow(ctx, `
		SELECT status, run_at, lease_token, lease_expires_at, attempts, last_error, failed_at
		FROM th_jobs
		WHERE id=$1
	`, rec.ID)

	if err := row.Scan(&status, &runAt, &leaseTok, &leaseExp, &attempts, &lastErr, &failedAt); err != nil {
		t.Fatalf("scan: %v", err)
	}

	if status != "ready" {
		t.Fatalf("expected status ready, got %s", status)
	}
	if runAt == nil || !runAt.UTC().Equal(future.UTC()) {
		t.Fatalf("expected run_at %v, got %v", future.UTC(), runAt)
	}
	if leaseTok != nil || leaseExp != nil {
		t.Fatalf("expected lease cleared, got token=%v exp=%v", leaseTok, leaseExp)
	}
	if attempts != 1 {
		t.Fatalf("expected attempts=1, got %d", attempts)
	}
	if lastErr != "boom" {
		t.Fatalf("expected last_error=boom, got %q", lastErr)
	}
	if failedAt == nil || !failedAt.UTC().Equal(t0.Add(1*time.Second).UTC()) {
		t.Fatalf("expected failed_at %v, got %v", t0.Add(1*time.Second).UTC(), failedAt)
	}

	// Not reservable before due
	_, _, ok2, err := d.Reserve(ctx, "default", t0.Add(10*time.Second), 5*time.Second)
	if err != nil {
		t.Fatalf("Reserve before due: %v", err)
	}
	if ok2 {
		t.Fatalf("expected ok=false before run_at")
	}

	// Reservable at/after due
	_, lease2, ok3, err := d.Reserve(ctx, "default", future, 5*time.Second)
	if err != nil || !ok3 {
		t.Fatalf("Reserve at due: ok=%v err=%v", ok3, err)
	}

	if err := d.Ack(ctx, rec.ID, lease2.Token, future.Add(1*time.Second)); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	// Terminal state
	var status2 string
	row2 := pool.QueryRow(ctx, `SELECT status FROM th_jobs WHERE id=$1`, rec.ID)
	if err := row2.Scan(&status2); err != nil {
		t.Fatalf("scan2: %v", err)
	}
	if status2 != "done" {
		t.Fatalf("expected status done, got %s", status2)
	}
}

func TestPostgresDriver_E2E_FailMovesToDLQ(t *testing.T) {
	ctx, pool, d := newE2EPoolAndDriver(t)

	t0 := time.Date(2026, 2, 6, 12, 0, 0, 0, time.UTC)

	rec := driver.JobRecord{
		ID:          "e2e_dlq_1",
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
	if err := d.Fail(ctx, rec.ID, lease.Token, failNow, "boom"); err != nil {
		t.Fatalf("Fail: %v", err)
	}

	var status string
	var dlqReason *string
	var dlqFailedAt *time.Time
	var leaseTok *string
	var leaseExp *time.Time

	row := pool.QueryRow(ctx, `
		SELECT status, dlq_reason, dlq_failed_at, lease_token, lease_expires_at
		FROM th_jobs
		WHERE id=$1
	`, rec.ID)

	if err := row.Scan(&status, &dlqReason, &dlqFailedAt, &leaseTok, &leaseExp); err != nil {
		t.Fatalf("scan: %v", err)
	}

	if status != "dlq" {
		t.Fatalf("expected status dlq, got %s", status)
	}
	if dlqReason == nil || *dlqReason != "boom" {
		t.Fatalf("expected dlq_reason boom, got %v", dlqReason)
	}
	if dlqFailedAt == nil || !dlqFailedAt.UTC().Equal(failNow.UTC()) {
		t.Fatalf("expected dlq_failed_at %v, got %v", failNow.UTC(), dlqFailedAt)
	}
	if leaseTok != nil || leaseExp != nil {
		t.Fatalf("expected lease cleared, got token=%v exp=%v", leaseTok, leaseExp)
	}

	// Not reservable anymore
	_, _, ok2, err := d.Reserve(ctx, "default", t0.Add(2*time.Second), 5*time.Second)
	if err != nil {
		t.Fatalf("Reserve after dlq: %v", err)
	}
	if ok2 {
		t.Fatalf("expected ok=false after dlq")
	}
}

func TestPostgresDriver_E2E_RetryUntilMaxAttemptsThenDLQ(t *testing.T) {
	ctx, pool, d := newE2EPoolAndDriver(t)

	t0 := time.Date(2026, 2, 6, 12, 0, 0, 0, time.UTC)

	rec := driver.JobRecord{
		ID:          "e2e_max_attempts_1",
		Type:        "t",
		Queue:       "default",
		Payload:     []byte(`{}`),
		CreatedAt:   t0,
		MaxAttempts: 3, // after 3 failures we expect DLQ via Fail
	}

	if _, _, err := d.Enqueue(ctx, rec); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	now := t0
	for attempt := 1; attempt <= rec.MaxAttempts; attempt++ {
		// Reserve
		_, lease, ok, err := d.Reserve(ctx, "default", now, 5*time.Second)
		if err != nil || !ok {
			t.Fatalf("Reserve attempt %d: ok=%v err=%v", attempt, ok, err)
		}

		now = now.Add(1 * time.Second)

		if attempt < rec.MaxAttempts {
			// Retry immediately (RunAt zero => NULL)
			if err := d.Retry(ctx, rec.ID, lease.Token, now, driver.RetryUpdate{
				Attempts:  attempt,
				LastError: "boom",
				FailedAt:  now,
				RunAt:     time.Time{},
			}); err != nil {
				t.Fatalf("Retry attempt %d: %v", attempt, err)
			}
		} else {
			// Final attempt: DLQ
			if err := d.Fail(ctx, rec.ID, lease.Token, now, "max attempts reached"); err != nil {
				t.Fatalf("Fail final attempt: %v", err)
			}
		}

		now = now.Add(1 * time.Second)
	}

	// Verify DLQ state
	var status string
	var dlqReason *string
	var dlqFailedAt *time.Time
	var attempts int

	row := pool.QueryRow(ctx, `
		SELECT status, dlq_reason, dlq_failed_at, attempts
		FROM th_jobs
		WHERE id=$1
	`, rec.ID)

	if err := row.Scan(&status, &dlqReason, &dlqFailedAt, &attempts); err != nil {
		t.Fatalf("scan: %v", err)
	}

	if status != "dlq" {
		t.Fatalf("expected status dlq, got %s", status)
	}
	if dlqReason == nil || *dlqReason != "max attempts reached" {
		t.Fatalf("expected dlq_reason %q, got %v", "max attempts reached", dlqReason)
	}
	if dlqFailedAt == nil {
		t.Fatalf("expected dlq_failed_at to be set")
	}

	// attempts should reflect the last Retry update (MaxAttempts-1),
	// because the final transition is Fail (not Retry).
	if attempts != rec.MaxAttempts-1 {
		t.Fatalf("expected attempts=%d, got %d", rec.MaxAttempts-1, attempts)
	}

	// Not reservable anymore
	_, _, ok, err := d.Reserve(ctx, "default", now.Add(10*time.Second), 5*time.Second)
	if err != nil {
		t.Fatalf("Reserve after dlq: %v", err)
	}
	if ok {
		t.Fatalf("expected ok=false after dlq")
	}
}
