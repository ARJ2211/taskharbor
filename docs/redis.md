# Redis driver

This driver stores jobs in Redis and uses Lua scripts so the critical operations are atomic.

Durability depends on your Redis setup (AOF/RDB, replication, etc.). The conformance suite focuses on behavior (leases, reclaim, idempotency, scheduling), not on Redis durability guarantees.

Options

- KeyPrefix: all keys are prefixed so one Redis instance can serve multiple apps (default taskharbor).
- DB: Redis logical DB (e.g. tests may use a non-zero DB to avoid clobbering other data).
- RedisClientOption: tune the underlying go-redis client when using New() (timeouts, pool size, TLS, etc.).

## Environment variables

- REDIS_ADDR
  Redis address (host:port). Used by examples and integration tests.
  Example: localhost:6379

Recommended for local dev (repo-root .env):

```bash
REDIS_ADDR=localhost:6379
```

## Running Redis locally (Docker)

If you have a docker-compose.yml in the repo:

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

All keys are prefixed (opts.prefix) so multiple TaskHarbor instances can share one Redis.

Per job

- {prefix}:job:{id} (hash)
    - type, queue, payload
    - run_at_nano
    - timeout_nano
    - created_at_nano
    - attempts, max_attempts
    - last_error, failed_at_nano
    - idempotency_key
    - status: ready | scheduled | inflight | done | dlq
    - lease_token
    - lease_expires_at_nano
    - dlq_reason, dlq_failed_at_nano (only for DLQ jobs)

Per queue

- {prefix}:queue:{queue}:ready (list)
    - job IDs runnable now (FIFO)

- {prefix}:queue:{queue}:scheduled (sorted set)
    - score: runAt Unix seconds
    - member: %09d:{jobID} where %09d is nanoseconds within that second
    - Why this shape: Redis ZSET scores are doubles; using a seconds score avoids precision loss, and the member prefix provides nanosecond ordering inside the current second (needed by the conformance timing tests).

- {prefix}:queue:{queue}:inflight (sorted set)
    - score: lease expiry as Unix milliseconds
    - member: job ID
    - The job hash also stores lease_expires_at_nano for precise lease validation in ack/retry/fail.

- {prefix}:queue:{queue}:dlq (list)
    - job IDs that failed permanently

Idempotency mapping

- {prefix}:idem:{queue}:{idempotency_key} (string)
    - value: job ID
    - Used to dedupe enqueues by (queue, idempotency_key)

## Enqueue idempotency and the existed flag

If JobRecord.IdempotencyKey is non-empty:

- First enqueue for (queue, idempotency_key):
    - creates the job
    - returns (jobID, existed=false)

- Duplicate enqueue for the same (queue, idempotency_key):
    - returns (existingJobID, existed=true)
    - does not create a second runnable copy
    - does not mutate the existing job record

If IdempotencyKey is empty, existed is always false.

## Reclaim behavior

Reclaim is performed during Reserve:

- expired inflight jobs are moved back to runnable state
- run_at is cleared as part of reclaim, so reclaimed jobs are runnable immediately

## Lua scripts

All scripts are invoked with EVAL so each operation is atomic.

### scriptEnqueue

Goal: atomically create a job record and place it into ready or scheduled, with optional idempotency dedupe.

Steps (in one script):

1. If idempotency_key is set, check {prefix}:idem:{queue}:{key}.
    - If present, return {existingJobID, 1}.
2. Write {prefix}:job:{id} hash fields (status=ready or status=scheduled).
3. If ready: RPUSH job ID to {prefix}:queue:{queue}:ready.
4. If scheduled: ZADD {prefix}:queue:{queue}:scheduled with:
    - score = runAt seconds
    - member = %09d:{id} where %09d is sub-second nanoseconds
5. If idempotency_key is set, SET the mapping to the new job ID.
6. Return {jobID, 0}.

Return shape

- {jobID, existedFlag} where existedFlag is 0 or 1.

### scriptReserve

Goal: in one atomic step:

- reclaim expired inflight jobs
- promote due scheduled jobs into ready
- pop one ready job and lease it

Inputs

- now_sec, now_sub (nanoseconds within the second), now_ms
- token, exp_ms, exp_nano

Steps:

1. Reclaim expired inflight
    - find members in inflight with score <= now_ms
    - for each:
        - ZREM inflight entry
        - if job status is inflight:
            - set job status=ready, clear lease fields, clear run_at_nano
            - RPUSH to ready

2. Promote scheduled jobs
    - promote all entries with score < now_sec (past seconds)
    - promote entries with score == now_sec only if member nanos <= now_sub
    - promotion does:
        - ZREM scheduled member
        - set job status=ready and clear run_at_nano
        - RPUSH job ID to ready

3. LPOP one job ID from ready
    - if none, return nil

4. Lease it
    - set job status=inflight, set lease_token, set lease_expires_at_nano, clear run_at_nano
    - ZADD inflight with score=exp_ms and member=jobID

Returns

- job ID (string) if reserved
- nil if nothing runnable

### scriptExtendLease

Goal: extend a valid inflight lease.

Checks (must all pass):

- job status is inflight
- lease_token matches
- lease_expires_at_nano is still > now (not expired)

On success:

- update lease_expires_at_nano in the job hash
- bump the inflight ZSET score to new_exp_ms so reclaim sees the new expiry

Returns

- 1 on success
- 0 on failure (caller classifies into JobNotInflight / LeaseMismatch / LeaseExpired)

### scriptAck

Goal: complete a job (terminal state: done) and remove it from inflight.

Checks:

- status is inflight
- token matches
- lease not expired

On success:

- set status=done, clear lease fields
- ZREM from inflight

Returns

- 1 success
- 2 not inflight (includes already done/dlq/ready/scheduled)
- 3 token mismatch
- 4 lease expired

Ack terminal idempotency

- the Go wrapper treats Ack on an already-done job as success (nil).

### scriptRetry

Goal: record a failure attempt and re-queue the job (ready or scheduled).

Checks:

- status is inflight
- token matches
- lease not expired

On success:

- update attempts, last_error, failed_at_nano
- clear lease fields
- remove from inflight
- enqueue again:
    - if run_at_nano == 0: RPUSH ready
    - else: ZADD scheduled with (run_at_sec, run_at_member)
- status becomes ready or scheduled accordingly

Returns

- 1 on success
- 0 on failure (caller classifies into JobNotInflight / LeaseMismatch / LeaseExpired)

### scriptFail

Goal: move a job to the DLQ (terminal state: dlq).

Checks:

- status is inflight
- token matches
- lease not expired

On success:

- set status=dlq
- store dlq_reason and dlq_failed_at_nano
- clear lease fields
- remove from inflight
- RPUSH job ID to the DLQ list

Returns

- 1 on success
- 0 on failure (caller classifies into JobNotInflight / LeaseMismatch / LeaseExpired)

Fail terminal idempotency

- the Go wrapper treats Fail on an already-dlq (or already-done) job as success (nil).

## Running the Redis demo

From repo root:

1. Ensure Redis is running and REDIS_ADDR is set (defaults may be used by examples).
2. Run:

```bash
go run ./examples/basic-redis
```

## Running tests

From repo root, with REDIS_ADDR set:

```bash
# Bash
REDIS_ADDR=localhost:6379 go test ./taskharbor/driver/redis/... -v

# PowerShell
$env:REDIS_ADDR = "localhost:6379"
go test ./taskharbor/driver/redis/... -v
```

If REDIS_ADDR is not set, Redis tests may be skipped (depending on the test helper).

## Notes

- Scheduling precision: the scheduled ZSET layout is intentionally designed to avoid Redis ZSET score precision issues while still allowing nanosecond-level due checks in Lua.
- Reclaim happens during Reserve. If you need faster reclaim under low traffic, call Reserve more frequently or run an external ticker that triggers Reserve regularly.
