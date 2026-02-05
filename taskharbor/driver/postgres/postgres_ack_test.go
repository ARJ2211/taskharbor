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

func newTestPool(t *testing.T) (*pgxpool.Pool, context.Context, context.CancelFunc) {
	t.Helper()

	wd, _ := os.Getwd()
	_ = envutil.LoadRepoDotenv(wd)

	dsn := os.Getenv("TASKHARBOR_TEST_DSN")
	if dsn == "" {
		t.Skip("TASKHARBOR_TEST_DSN not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		cancel()
		t.Fatalf("pgxpool.New: %v", err)
	}

	if err := ApplyMigrations(ctx, pool); err != nil {
		pool.Close()
		cancel()
		t.Fatalf("ApplyMigrations: %v", err)
	}

	if _, err := pool.Exec(ctx, `DELETE FROM th_jobs`); err != nil {
		pool.Close()
		cancel()
		t.Fatalf("cleanup: %v", err)
	}

	return pool, ctx, cancel
}

func newJobRecord(id string, now time.Time) driver.JobRecord {
	return driver.JobRecord{
		ID:          id,
		Type:        "t",
		Queue:       "default",
		Payload:     []byte(`{}`),
		RunAt:       time.Time{},
		Timeout:     2 * time.Second,
		CreatedAt:   now.UTC(),
		Attempts:    0,
		MaxAttempts: 3,
		LastError:   "",
		FailedAt:    time.Time{},
	}
}

func TestAck_NotInflight(t *testing.T) {
	pool, ctx, cancel := newTestPool(t)
	defer cancel()
	defer pool.Close()

	d, err := NewWithPool(pool)
	if err != nil {
		t.Fatalf("NewWithPool: %v", err)
	}

	t0 := time.Date(2026, 2, 5, 12, 0, 0, 0, time.UTC)
	id := fmt.Sprintf("ack_not_inflight_%d", time.Now().UnixNano())

	rec := newJobRecord(id, t0)
	if err := d.Enqueue(ctx, rec); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	err = d.Ack(ctx, id, driver.LeaseToken("tok"), t0)
	if err != driver.ErrJobNotInflight {
		t.Fatalf("expected ErrJobNotInflight, got %v", err)
	}
}

func TestAck_TokenMismatch(t *testing.T) {
	pool, ctx, cancel := newTestPool(t)
	defer cancel()
	defer pool.Close()

	d, err := NewWithPool(pool)
	if err != nil {
		t.Fatalf("NewWithPool: %v", err)
	}

	t0 := time.Date(2026, 2, 5, 12, 0, 0, 0, time.UTC)
	id := fmt.Sprintf("ack_mismatch_%d", time.Now().UnixNano())

	rec := newJobRecord(id, t0)
	if err := d.Enqueue(ctx, rec); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	_, lease, ok, err := d.Reserve(ctx, "default", t0, 5*time.Second)
	if err != nil || !ok {
		t.Fatalf("Reserve: ok=%v err=%v", ok, err)
	}

	err = d.Ack(ctx, id, driver.LeaseToken("wrong"), t0.Add(1*time.Second))
	if err != driver.ErrLeaseMismatch {
		t.Fatalf("expected ErrLeaseMismatch, got %v", err)
	}

	// still inflight
	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM th_jobs WHERE id=$1`, id).Scan(&status); err != nil {
		t.Fatalf("scan status: %v", err)
	}
	if status != "inflight" {
		t.Fatalf("expected status inflight, got %s", status)
	}

	_ = lease
}

func TestAck_Expired(t *testing.T) {
	pool, ctx, cancel := newTestPool(t)
	defer cancel()
	defer pool.Close()

	d, err := NewWithPool(pool)
	if err != nil {
		t.Fatalf("NewWithPool: %v", err)
	}

	t0 := time.Date(2026, 2, 5, 12, 0, 0, 0, time.UTC)
	id := fmt.Sprintf("ack_expired_%d", time.Now().UnixNano())

	rec := newJobRecord(id, t0)
	if err := d.Enqueue(ctx, rec); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	_, lease, ok, err := d.Reserve(ctx, "default", t0, 2*time.Second)
	if err != nil || !ok {
		t.Fatalf("Reserve: ok=%v err=%v", ok, err)
	}

	// Call Ack after lease expiry (use correct token to isolate the expired path)
	err = d.Ack(ctx, id, lease.Token, t0.Add(3*time.Second))
	if err != driver.ErrLeaseExpired {
		t.Fatalf("expected ErrLeaseExpired, got %v", err)
	}
}

func TestAck_Success_MarksDone(t *testing.T) {
	pool, ctx, cancel := newTestPool(t)
	defer cancel()
	defer pool.Close()

	d, err := NewWithPool(pool)
	if err != nil {
		t.Fatalf("NewWithPool: %v", err)
	}

	t0 := time.Date(2026, 2, 5, 12, 0, 0, 0, time.UTC)
	id := fmt.Sprintf("ack_success_%d", time.Now().UnixNano())

	rec := newJobRecord(id, t0)
	if err := d.Enqueue(ctx, rec); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	_, lease, ok, err := d.Reserve(ctx, "default", t0, 10*time.Second)
	if err != nil || !ok {
		t.Fatalf("Reserve: ok=%v err=%v", ok, err)
	}

	if err := d.Ack(ctx, id, lease.Token, t0.Add(1*time.Second)); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	// verify done + lease cleared
	var status string
	var leaseTok *string
	var leaseExp *time.Time
	if err := pool.QueryRow(ctx, `
		SELECT status, lease_token, lease_expires_at
		FROM th_jobs WHERE id=$1
	`, id).Scan(&status, &leaseTok, &leaseExp); err != nil {
		t.Fatalf("scan: %v", err)
	}

	if status != "done" {
		t.Fatalf("expected status done, got %s", status)
	}
	if leaseTok != nil || leaseExp != nil {
		t.Fatalf("expected lease fields NULL, got token=%v exp=%v", leaseTok, leaseExp)
	}

	// should not be reservable anymore
	_, _, ok2, err := d.Reserve(ctx, "default", t0.Add(2*time.Second), 5*time.Second)
	if err != nil {
		t.Fatalf("Reserve after ack: %v", err)
	}
	if ok2 {
		t.Fatalf("expected ok=false after ack (done jobs not reservable)")
	}

	// acking again should behave like not inflight
	err = d.Ack(ctx, id, lease.Token, t0.Add(2*time.Second))
	if err != driver.ErrJobNotInflight {
		t.Fatalf("expected ErrJobNotInflight on second ack, got %v", err)
	}
}
