# Redis Driver

This doc covers how to run TaskHarbor using the Redis driver, including env vars, data model, tests, and the demo example.

## What you get

- Durable job storage in Redis (hashes + lists + sorted sets)
- Scheduling via run_at (scheduled zset)
- Leases (visibility timeouts) and crash recovery (reclaim expired inflight)
- ExtendLease heartbeat support
- Ack / Retry / Fail (DLQ) with strict lease validation
- Atomic operations via Lua scripts (enqueue, reserve, ack, extend, retry, fail)
- Jobs kept on ack (status=done) for inspect/observability; parity with Postgres
- Integration tests split by operation (enqueue, reserve, ack, retry, fail, extend_lease, e2e)
- A runnable Redis demo example (basic worker, scheduled jobs, retry, DLQ)

Options:

- **KeyPrefix** — all keys are prefixed so one Redis instance can serve multiple apps (default `taskharbor`).
- **DB** — Redis logical DB 0–15 (e.g. tests use DB 14 to avoid clobbering other data).
- **RedisClientOption** — tune the underlying client (timeouts, pool size, TLS) when using `New()`.

## Environment variables

- **REDIS_ADDR**  
  Redis address (host:port). Used by examples and integration tests.  
  Example: `localhost:6379`.

Recommended for local dev (e.g. in a repo-root `.env`):

```bash
REDIS_ADDR=localhost:6379
```

## Running Redis locally (Docker)

If you have a `docker-compose.yml` in the repo:

```bash
docker compose up -d redis
```

Or run a one-off container:

```bash
docker run -d -p 6379:6379 --name taskharbor-redis redis:7-alpine
```

Verify:

```bash
redis-cli -h localhost -p 6379 ping
# PONG
```

## Data model

The Redis driver uses a key-per-job hash plus per-queue structures. All keys are under a configurable **prefix** (default `taskharbor`).

**Per job**

- One hash key: `{prefix}:job:{id}`  
  Fields: type, queue, payload, run_at_nano, timeout_nano, created_at_nano, attempts, max_attempts, last_error, failed_at_nano, idempotency_key, status, lease_token, lease_expires_at_nano (and for dlq: dlq_reason, dlq_failed_at_nano).

**Per queue** (e.g. `default`)

- **ready** — list; job IDs that can run now. Reserve LPOPs from here.
- **scheduled** — sorted set; score = run_at (nanos). Due jobs are promoted to ready.
- **inflight** — sorted set; score = lease expiry (nanos). Used to reclaim expired leases.
- **dlq** — list; job IDs that have permanently failed.

Status values on the job hash: `ready`, `inflight`, `done`, `dlq`. Timestamps and timeouts are stored as nano strings to avoid Lua float precision issues.

All multi-step operations (enqueue, reserve, ack, extend, retry, fail) are implemented as **Lua scripts**, so they run atomically in Redis.

## Running the Redis demo

A basic Redis example mirrors the memory/Postgres examples but uses Redis storage.

From repo root:

1. Ensure Redis is running and **REDIS_ADDR** is set (defaults to `localhost:6379` if unset).
2. Run:

```bash
go run ./examples/basic-redis
```

The demo:

- Connects to Redis
- Enqueues an immediate job and a scheduled job
- Enqueues a “RetryMe” job (fails once, then succeeds) and a “WillDLQ” job (always fails)
- Uses a retry policy with exponential backoff
- Runs a worker and prints handler output, then shuts down after a few seconds

## Running tests

From repo root, with **REDIS_ADDR** set (e.g. `localhost:6379`):

```bash
# PowerShell
$env:REDIS_ADDR = "localhost:6379"
go test ./taskharbor/driver/redis/... -v

# Bash
REDIS_ADDR=localhost:6379 go test ./taskharbor/driver/redis/... -v
```

If **REDIS_ADDR** is not set, Redis tests are skipped (so the rest of the suite can run without Redis).

Test layout (aligned with the Postgres driver):

- `redis_helpers_test.go` — `testRedis(t)` helper; unique queue per test (DB 14, prefix `th-test`).
- `redis_enqueue_test.go` — New (invalid addr), validation, enqueue+reserve, closed driver.
- `redis_reserve_test.go` — No jobs, invalid lease, schedule promotion, no double deliver, reclaim expired.
- `redis_ack_test.go` — Ack success, lease mismatch rejected.
- `redis_retry_test.go` — Retry moves job back to scheduled.
- `redis_fail_test.go` — Fail moves job to DLQ.
- `redis_extend_lease_test.go` — Extend prevents reclaim; expired returns error.
- `redis_e2e_test.go` — Enqueue → Reserve → Ack flow.

## CI

CI should:

- Start a Redis service container (e.g. `redis:7-alpine` on port 6379).
- Set **REDIS_ADDR** (e.g. `localhost:6379`).
- Run `go test ./taskharbor/driver/redis/...` (and optionally `./...` for the full suite).

If Redis is not available, Redis tests will skip and other packages will still run.
