package postgres

import (
	"strings"
	"testing"
)

func TestSplitSQLStatements(t *testing.T) {
	sql := `
-- comment
CREATE TABLE a (id TEXT);
CREATE TABLE b (id TEXT, v TEXT DEFAULT 'x;y');
/* block
comment */
CREATE TABLE c (id TEXT);
`
	stmts := splitSQLStatements(sql)
	if len(stmts) != 3 {
		t.Fatalf("expected 3 statements, got %d: %#v", len(stmts), stmts)
	}
}

func TestSplitSQLStatements_001Init(t *testing.T) {
	sql := `
-- 001_init.sql
-- Initial schema for TaskHarbor Postgres driver (Milestone 5)

-- Track applied migrations
CREATE TABLE IF NOT EXISTS th_schema_migrations (
  version    TEXT PRIMARY KEY,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Jobs table (single-table model for jobs + leases + DLQ)
CREATE TABLE IF NOT EXISTS th_jobs (
  id               TEXT PRIMARY KEY,
  type             TEXT NOT NULL,
  queue            TEXT NOT NULL,
  payload          BYTEA NOT NULL,

  -- Scheduling: RunAt == zero in Go means runnable now. We store that as NULL.
  run_at           TIMESTAMPTZ NULL,

  -- Duration stored as nanoseconds (time.Duration is int64 nanos).
  timeout_nanos    BIGINT NOT NULL DEFAULT 0 CHECK (timeout_nanos >= 0),

  created_at       TIMESTAMPTZ NOT NULL,

  attempts         INT NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  max_attempts     INT NOT NULL DEFAULT 0 CHECK (max_attempts >= 0),

  last_error       TEXT NOT NULL DEFAULT '',
  failed_at        TIMESTAMPTZ NULL,

  -- State machine:
  -- ready: eligible when due (run_at NULL or run_at <= now)
  -- inflight: leased, not eligible unless lease is expired (reclaim)
  -- dlq: terminal DLQ
  -- done: terminal success (kept for observability/telemetry)
  status           TEXT NOT NULL DEFAULT 'ready'
                   CHECK (status IN ('ready','inflight','dlq','done')),

  lease_token      TEXT NULL,
  lease_expires_at TIMESTAMPTZ NULL,

  dlq_reason       TEXT NULL,
  dlq_failed_at    TIMESTAMPTZ NULL,

  -- Stored in milestone 5; uniqueness/enforcement is milestone 6.
  idempotency_key  TEXT NULL,

  -- Consistency constraints
  CONSTRAINT th_jobs_lease_pair_chk
    CHECK ((lease_token IS NULL) = (lease_expires_at IS NULL)),

  CONSTRAINT th_jobs_inflight_requires_lease_chk
    CHECK (status <> 'inflight' OR (lease_token IS NOT NULL AND lease_expires_at IS NOT NULL)),

  -- If not inflight, leases must be cleared (prevents leaked leases in terminal/ready states)
  CONSTRAINT th_jobs_non_inflight_has_no_lease_chk
    CHECK (status = 'inflight' OR (lease_token IS NULL AND lease_expires_at IS NULL)),

  CONSTRAINT th_jobs_dlq_requires_metadata_chk
    CHECK (status <> 'dlq' OR (dlq_failed_at IS NOT NULL))
);

-- No indexes in milestone 5 (correctness first).
-- Indexes for reserve performance + lease reclaim land in milestone 6.
`

	stmts := splitSQLStatements(sql)
	if len(stmts) != 2 {
		t.Fatalf("expected 2 statements, got %d: %#v", len(stmts), stmts)
	}

	if !strings.Contains(stmts[0], "CREATE TABLE IF NOT EXISTS th_schema_migrations") {
		t.Fatalf("first stmt does not look like schema_migrations create: %s", stmts[0])
	}
	if !strings.Contains(stmts[1], "CREATE TABLE IF NOT EXISTS th_jobs") {
		t.Fatalf("second stmt does not look like th_jobs create: %s", stmts[1])
	}
}
