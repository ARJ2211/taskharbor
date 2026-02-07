# Postgres Driver

This doc covers how to run TaskHarbor using the Postgres driver, including migrations, env vars, tests, and the demo example.

## What you get (Milestone 5)

- Durable job storage in Postgres
- Scheduling via run_at
- Leases (visibility timeouts) and crash recovery
- ExtendLease heartbeat support
- Ack / Retry / Fail (DLQ) with strict lease validation
- Integration tests that mirror memory driver behavior
- A runnable Postgres demo example (basic worker + scheduled jobs)

Not included until Milestone 6:

- Idempotency dedupe enforcement (unique constraint behavior)
- Indexes and performance tuning
- Fully idempotent ack/fail (no-op semantics under repeats)

## Environment variables

- TASKHARBOR_DSN
  Used by examples and local runs.

- TASKHARBOR_TEST_DSN
  Used by Postgres integration tests.

Recommended: put both in a repo-root .env file for local dev:

TASKHARBOR_DSN=postgres://taskharbor:taskharbor@localhost:5432/taskharbor?sslmode=disable
TASKHARBOR_TEST_DSN=postgres://taskharbor:taskharbor@localhost:5432/taskharbor_test?sslmode=disable

## Running Postgres locally (Docker)

If you have a docker-compose.yml in the repo, run:

docker compose up -d

Then verify:

psql $TASKHARBOR_DSN -c "select 1"

## Migrations

The Postgres driver ships with embedded SQL migrations.

In code, run:

- postgres.ApplyMigrations(ctx, pool)

This is used by:

- integration tests (at start)
- examples/basic_postgres (before worker runs)

Migrations are safe to run multiple times.

## Schema overview

The Postgres driver uses a single-table job model.

Key points:

- run_at is NULL for runnable-now jobs (RunAt.IsZero in Go).
- inflight jobs have (lease_token, lease_expires_at).
- dlq and done are terminal states.
- lease validation uses the provided now time, not database NOW().

Timestamp precision:

- Postgres TIMESTAMPTZ stores microsecond precision.
- Tests should compare times at microsecond precision (use Truncate(time.Microsecond)).

## Running the Postgres demo

A basic Postgres example mirrors the memory example but uses Postgres storage.

From repo root:

- Ensure TASKHARBOR_DSN is set (or present in .env).
- Run:

go run ./examples/basic_postgres

The demo:

- applies migrations
- enqueues an immediate job
- enqueues a few scheduled jobs
- runs a worker and prints handler output

Note:

- examples cannot import taskharbor/internal/\* (Go internal package rules).
- the example loads .env via godotenv.Load() instead of envutil.

## Running tests

From repo root, with TASKHARBOR_TEST_DSN set:

go test ./...
go test -race ./...

Test hygiene notes:

- Use DELETE FROM th_jobs between tests (TRUNCATE can take heavier locks).
- Prefer fixed timestamps in tests for determinism.
- When asserting run_at/failed_at values, compare with Truncate(time.Microsecond).

## CI

CI should:

- start a Postgres service container
- set TASKHARBOR_TEST_DSN
- run go test ./... (and optionally -race)

If CI failures happen only on timestamps, it is usually microsecond rounding. Normalize in tests.
