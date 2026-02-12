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

func TestEnqueue_ValidateFirst(t *testing.T) {
	d := &Driver{pool: nil} // ensureOpen will fail, but Validate should run first
	_, _, err := d.Enqueue(context.Background(), driver.JobRecord{})
	if err == nil {
		t.Fatalf("expected validation error, got nil")
	}
}

func TestEnqueue_PersistsFields(t *testing.T) {
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

	// DELETE is less lock-aggressive than TRUNCATE and avoids CI stalls.
	if _, err := pool.Exec(ctx, `DELETE FROM th_jobs`); err != nil {
		t.Fatalf("cleanup: %v", err)
	}

	d, err := NewWithPool(pool)
	if err != nil {
		t.Fatalf("NewWithPool: %v", err)
	}

	// Use deterministic time (and microsecond precision for Postgres TIMESTAMPTZ).
	now := time.Date(2026, 2, 6, 21, 12, 27, 747854050, time.UTC).Truncate(time.Microsecond)

	rec := driver.JobRecord{
		ID:             "job_1",
		Type:           "email",
		Queue:          "default",
		Payload:        []byte(`{"x":1}`),
		RunAt:          time.Time{}, // should become NULL
		Timeout:        5 * time.Second,
		CreatedAt:      now,
		Attempts:       0,
		MaxAttempts:    3,
		LastError:      "",
		FailedAt:       time.Time{}, // NULL
		IdempotencyKey: "",
	}

	if _, _, err := d.Enqueue(ctx, rec); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	var (
		runAt     *time.Time
		status    string
		timeoutNS int64
		idemKey   *string
	)

	row := pool.QueryRow(ctx, `
		SELECT run_at, status, timeout_nanos, idempotency_key
		FROM th_jobs
		WHERE id = $1
	`, rec.ID)

	if err := row.Scan(&runAt, &status, &timeoutNS, &idemKey); err != nil {
		t.Fatalf("scan: %v", err)
	}

	if runAt != nil {
		t.Fatalf("expected run_at NULL, got %v", *runAt)
	}
	if status != "ready" {
		t.Fatalf("expected status ready, got %s", status)
	}
	if timeoutNS != rec.Timeout.Nanoseconds() {
		t.Fatalf("timeout mismatch: got %d want %d", timeoutNS, rec.Timeout.Nanoseconds())
	}
	if idemKey != nil {
		t.Fatalf("expected idempotency_key NULL, got %q", *idemKey)
	}

	// scheduled job
	rec2 := rec
	rec2.ID = "job_2"
	rec2.RunAt = now.Add(1 * time.Hour).Truncate(time.Microsecond)
	rec2.IdempotencyKey = "k1"

	if _, _, err := d.Enqueue(ctx, rec2); err != nil {
		t.Fatalf("Enqueue scheduled: %v", err)
	}

	var runAt2 time.Time
	var idemKey2 *string
	row2 := pool.QueryRow(ctx, `SELECT run_at, idempotency_key FROM th_jobs WHERE id = $1`, rec2.ID)
	if err := row2.Scan(&runAt2, &idemKey2); err != nil {
		t.Fatalf("scan2: %v", err)
	}

	got := runAt2.UTC().Truncate(time.Microsecond)
	want := rec2.RunAt.UTC().Truncate(time.Microsecond)

	if !got.Equal(want) {
		t.Fatalf("run_at mismatch: got %v want %v", got, want)
	}
	if idemKey2 == nil || *idemKey2 != "k1" {
		t.Fatalf("idempotency_key mismatch: got %v want k1", idemKey2)
	}
}

func TestEnqueue_Idempotency_DedupesAndReturnsExistingID(t *testing.T) {
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

	now := time.Date(2026, 2, 6, 21, 12, 27, 0, time.UTC).Truncate(time.Microsecond)

	rec1 := driver.JobRecord{
		ID:             "idem_job_1",
		Type:           "t",
		Queue:          "default",
		Payload:        []byte(`{"v":1}`),
		RunAt:          time.Time{},
		Timeout:        0,
		CreatedAt:      now,
		Attempts:       0,
		MaxAttempts:    0,
		LastError:      "",
		FailedAt:       time.Time{},
		IdempotencyKey: "k1",
	}

	id1, existed1, err := d.Enqueue(ctx, rec1)
	if err != nil {
		t.Fatalf("Enqueue rec1: %v", err)
	}
	if id1 != rec1.ID || existed1 {
		t.Fatalf("expected first enqueue id=%s existed=false, got id=%s existed=%v", rec1.ID, id1, existed1)
	}

	rec2 := rec1
	rec2.ID = "idem_job_2"
	rec2.Payload = []byte(`{"v":2}`) // should NOT overwrite first payload

	id2, existed2, err := d.Enqueue(ctx, rec2)
	if err != nil {
		t.Fatalf("Enqueue rec2: %v", err)
	}
	if id2 != rec1.ID || !existed2 {
		t.Fatalf("expected dedupe to return id=%s existed=true, got id=%s existed=%v", rec1.ID, id2, existed2)
	}

	var cnt int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM th_jobs WHERE queue=$1 AND idempotency_key=$2`, "default", "k1").Scan(&cnt); err != nil {
		t.Fatalf("count: %v", err)
	}
	if cnt != 1 {
		t.Fatalf("expected 1 row for idempotency key, got %d", cnt)
	}

	var payload []byte
	if err := pool.QueryRow(ctx, `SELECT payload FROM th_jobs WHERE id=$1`, rec1.ID).Scan(&payload); err != nil {
		t.Fatalf("payload: %v", err)
	}
	if string(payload) != string(rec1.Payload) {
		t.Fatalf("payload was overwritten: got %s want %s", string(payload), string(rec1.Payload))
	}
}
