# Postgres Driver

This doc covers how to run TaskHarbor using the Postgres driver, including migrations, env vars, tests, examples, and useful psql queries for debugging and quick analytics.

This driver stores jobs in Postgres and uses SQL queries to implement TaskHarbor semantics.

## Schema overview

The driver stores jobs in a single table (th_jobs) with columns for:

- id (primary key)
- queue
- type
- payload (bytea)
- run_at (timestamp with time zone, nullable)
- timeout (interval, nullable)
- created_at
- attempts
- max_attempts
- last_error
- failed_at
- status (ready, inflight, done, dlq)
- lease_token (text, nullable)
- lease_expires_at (timestamp with time zone, nullable)
- idempotency_key (text, nullable)

## Key points

- run_at is NULL for runnable-now jobs (RunAt.IsZero in Go).
- inflight jobs have (lease_token, lease_expires_at).
- dlq and done are terminal states.
- lease validation uses the provided now time, not database NOW().
- when a lease is reclaimed (lease_expires_at <= now during Reserve), run_at is cleared (set back to NULL) so the job is treated as runnable now.

## Reserve

Reserve uses a single transaction to:

- reclaim expired inflight jobs
- promote due scheduled jobs (run_at <= now)
- pick one runnable job (ready state)
- set it inflight with a new lease token and expiry

## Idempotency

- When idempotency_key is provided, it is deduped per queue.
- Duplicate enqueue for the same (queue, idempotency_key) returns the existing job id and existed=true.
- Jobs without an idempotency key are not deduped.
- Dedupe is based on (queue, idempotency_key) only. If a duplicate enqueue supplies a different payload/type/run_at, we still return the existing job and do not create a second runnable copy.

## Terminal idempotency

- Ack is idempotent once the job is done.
- Fail is idempotent once the job is terminal (dlq or done).

## Performance notes

- Reserve is designed to be O(log n) with indexes on queue, status, and run_at.
- Cleanup of terminal jobs (done/dlq) is not automatic; you can add retention policies externally.

## Environment variables

TASKHARBOR_DSN
Used by examples and local runs.

TASKHARBOR_TEST_DSN
Used by Postgres integration tests.

Recommended: put both in a repo-root .env file for local dev:

```bash
TASKHARBOR_DSN=postgres://taskharbor:taskharbor@localhost:5432/taskharbor?sslmode=disable
TASKHARBOR_TEST_DSN=postgres://taskharbor:taskharbor@localhost:5432/taskharbor_test?sslmode=disable
```

## Running Postgres locally (Docker)

If you have a docker-compose.yml in the repo:

```bash
docker compose up -d
```

Verify connectivity:

```bash
psql "$TASKHARBOR_DSN" -c "select 1"
```

## Migrations

The Postgres driver ships with embedded SQL migrations.

In code, run:

- postgres.ApplyMigrations(ctx, pool)

This is used by:

- integration tests (at start)
- examples/basic-postgres (before worker runs)

Migrations are safe to run multiple times.

To see which versions have been applied:

```sql
SELECT version FROM th_schema_migrations ORDER BY version;
```

## Schema overview

The Postgres driver uses a single-table job model.

Key points

- run_at is NULL for runnable-now jobs (RunAt.IsZero in Go).
- inflight jobs have (lease_token, lease_expires_at).
- dlq and done are terminal states.
- lease validation uses the provided now time, not database NOW().

Timestamp precision

- Postgres TIMESTAMPTZ stores microsecond precision.
- Tests should compare times at microsecond precision (use Truncate(time.Microsecond)).

Idempotency

- When idempotency_key is provided, it is deduped per queue.
- Duplicate enqueue for the same (queue, idempotency_key) returns the existing job id.
- Jobs without an idempotency key are not deduped.

## Running the Postgres demo

A basic Postgres example mirrors the memory example but uses Postgres storage.

From repo root:

- Ensure TASKHARBOR_DSN is set (or present in .env).
- Run:

```bash
go run ./examples/basic-postgres
```

The demo:

- applies migrations
- enqueues an immediate job
- enqueues a few scheduled jobs
- runs a worker and prints handler output

## Running the stress example (optional)

```bash
go run ./examples/stress-postgres
```

Use this to sanity-check behavior under load and to eyeball reserve behavior.

## Running tests

From repo root, with TASKHARBOR_TEST_DSN set:

```bash
go test ./...
go test -race ./...
```

Test hygiene notes

- Use DELETE FROM th_jobs between tests (TRUNCATE can take heavier locks).
- Prefer fixed timestamps in tests for determinism.
- When asserting run_at/failed_at values, compare with Truncate(time.Microsecond).

## CI

CI should:

- start a Postgres service container
- set TASKHARBOR_TEST_DSN
- run go test ./... (and optionally -race)

If CI failures happen only on timestamps, it is usually microsecond rounding. Normalize in tests.

## Debugging and analytics queries (psql)

Show migrations:

```sql
SELECT version FROM th_schema_migrations ORDER BY version;
```

Show indexes on th_jobs:

```sql
SELECT indexname, indexdef
FROM pg_indexes
WHERE tablename = 'th_jobs'
ORDER BY indexname;
```

Quick queue health summary:

```sql
SELECT
  queue,
  COUNT(*) FILTER (WHERE status='ready' AND run_at IS NULL) AS ready_now,
  COUNT(*) FILTER (WHERE status='ready' AND run_at IS NOT NULL AND run_at > now()) AS scheduled_future,
  COUNT(*) FILTER (WHERE status='ready' AND run_at IS NOT NULL AND run_at <= now()) AS scheduled_due,
  COUNT(*) FILTER (WHERE status='inflight') AS inflight,
  COUNT(*) FILTER (WHERE status='inflight' AND lease_expires_at <= now()) AS inflight_expired,
  COUNT(*) FILTER (WHERE status='done') AS done,
  COUNT(*) FILTER (WHERE status='dlq') AS dlq
FROM th_jobs
GROUP BY queue
ORDER BY queue;
```

Oldest runnable jobs (roughly what reserve will pick):

```sql
SELECT id, type, status, run_at, created_at, lease_expires_at
FROM th_jobs
WHERE queue = 'default'
  AND status NOT IN ('done','dlq')
  AND (
    (status='ready' AND (run_at IS NULL OR run_at <= now()))
    OR
    (status='inflight' AND lease_expires_at <= now())
  )
ORDER BY created_at ASC, run_at ASC NULLS LAST, id ASC
LIMIT 20;
```

Find by idempotency key:

```sql
SELECT id, queue, type, status, created_at
FROM th_jobs
WHERE queue='default' AND idempotency_key='YOUR_KEY_HERE';
```

Inspect DLQ:

```sql
SELECT id, type, queue, dlq_reason, dlq_failed_at, attempts, max_attempts
FROM th_jobs
WHERE status='dlq'
ORDER BY dlq_failed_at DESC
LIMIT 50;
```

Lease debugging (what is currently inflight):

```sql
SELECT id, queue, type, lease_token, lease_expires_at
FROM th_jobs
WHERE status='inflight'
ORDER BY lease_expires_at ASC
LIMIT 50;
```

Reserve query plan debugging (optional)

If you want to see whether Postgres is using the reserve-related indexes, run an EXPLAIN against the reserve statement (or a close approximation) after you have a non-trivial number of rows:

```sql
EXPLAIN (ANALYZE, BUFFERS)
WITH cte AS (
  SELECT id
  FROM th_jobs
  WHERE queue = 'default'
    AND status NOT IN ('done','dlq')
    AND (
      (status = 'ready' AND (run_at IS NULL OR run_at <= now()))
      OR
      (status = 'inflight' AND lease_expires_at <= now())
    )
  ORDER BY
    CASE
      WHEN status = 'ready' AND run_at IS NULL THEN 0
      WHEN status = 'inflight' AND lease_expires_at <= now() THEN 1
      ELSE 2
    END,
    created_at ASC,
    run_at ASC NULLS LAST,
    id ASC
  FOR UPDATE SKIP LOCKED
  LIMIT 1
)
UPDATE th_jobs j
SET status='inflight', lease_token='x', lease_expires_at=now() + interval '30 seconds'
FROM cte
WHERE j.id = cte.id
RETURNING j.id;
```

If you just added indexes or inserted lots of rows, running this helps the planner:

```sql
ANALYZE th_jobs;
```
