package postgres

/*
QEnqueue inserts a new job row into th_jobs.

What it does
- Writes the stable JobRecord fields into Postgres so the worker can later Reserve/lease it.
- Uses NULL for run_at when the job should be runnable immediately (Go time.Time{}).

Columns written
- id, type, queue, payload: identity + routing + bytes payload.
- run_at:
  - NULL means runnable now
  - non-NULL means scheduled (Reserve must wait until run_at <= now)

- timeout_nanos: stored as int64 nanoseconds (time.Duration).
- created_at: stable insertion time (used for FIFO ordering).
- attempts, max_attempts: retry bookkeeping.
- last_error, failed_at: failure bookkeeping (failed_at is NULL when zero time in Go).
- status: set to "ready" at insert.
- idempotency_key: stored (uniqueness enforced in milestone 6).

Important invariants
- Enqueue never sets leases or DLQ fields.
- Scheduling is represented only by run_at being NULL vs non-NULL.
*/
const QEnqueue = `
INSERT INTO th_jobs (
	id,
	type,
	queue,
	payload,
	run_at,
	timeout_nanos,
	created_at,
	attempts,
	max_attempts,
	last_error,
	failed_at,
	status,
	idempotency_key
) VALUES (
	$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13
)
`

/*
QReserve atomically selects the next runnable job and leases it.

High-level behavior
- Finds one candidate row for the queue that is runnable at the provided 'now' time.
- Locks that row with FOR UPDATE SKIP LOCKED so concurrent workers never lease the same job.
- Updates the row to inflight and sets a fresh lease token + lease expiry.
- Reclaims expired inflight jobs and makes them run immediately (run_at becomes NULL), matching memory semantics.

Candidate selection (cte)
- Filters by queue.
- Ignores terminal states: status NOT IN ('done','dlq').
- Eligible rows are either:
 1. Ready and due:
    - status = 'ready'
    - run_at IS NULL (immediate) OR run_at <= now (scheduled but due)
 2. Inflight but lease has expired (reclaim):
    - status = 'inflight'
    - lease_expires_at <= now

Ordering (priority)
- Priority 0: ready + run_at IS NULL (immediate FIFO)
- Priority 1: inflight + expired lease (reclaim)
- Priority 2: ready + run_at <= now (scheduled due)
Then tie-breakers:
- created_at ASC (FIFO)
- run_at ASC NULLS LAST (scheduled due earlier first)
- id ASC (stable ordering)

Atomic lease update (UPDATE ... FROM cte)
- Sets:
  - status = 'inflight'
  - lease_token = $3
  - lease_expires_at = $4

- Reclaim rule:
  - run_at = NULL only when the row was previously inflight
  - this makes reclaimed jobs runnable immediately after reclaim (parity with memory reclaim behavior)

Return values
- Returns the full row fields needed to build:
  - driver.JobRecord (id/type/queue/payload/run_at/timeout/created_at/attempts/max_attempts/last_error/failed_at/idempotency_key)
  - driver.Lease (lease_token, lease_expires_at)
*/
const QReserve = `
WITH cte AS (
  SELECT id
  FROM th_jobs
  WHERE queue = $1
    AND status NOT IN ('done', 'dlq')
    AND (
      (status = 'ready' AND (run_at IS NULL OR run_at <= $2))
      OR
      (status = 'inflight' AND lease_expires_at <= $2)
    )
  ORDER BY
    CASE
      WHEN status = 'ready' AND run_at IS NULL THEN 0
      WHEN status = 'inflight' AND lease_expires_at <= $2 THEN 1
      ELSE 2
    END,
    created_at ASC,
    run_at ASC NULLS LAST,
    id ASC
  FOR UPDATE SKIP LOCKED
  LIMIT 1
)
UPDATE th_jobs j
SET
  status = 'inflight',
  lease_token = $3,
  lease_expires_at = $4,
  run_at = CASE WHEN j.status = 'inflight' THEN NULL ELSE j.run_at END
FROM cte
WHERE j.id = cte.id
RETURNING
  j.id,
  j.type,
  j.queue,
  j.payload,
  j.run_at,
  j.timeout_nanos,
  j.idempotency_key,
  j.created_at,
  j.attempts,
  j.max_attempts,
  j.last_error,
  j.failed_at,
  j.lease_token,
  j.lease_expires_at
`

/*
QExtendLeaseAtomic extends a lease in a single atomic statement.

Why we do this:
- It avoids BEGIN/tx + FOR UPDATE, which can block and (in our case) led to hangs.
- The check and the update happen together, so it's still correct:
  - job must be inflight
  - token must match
  - lease must still be valid at the provided "now"

- If any of those conditions fail, 0 rows are affected (no partial updates).

Important:
  - We use the caller-provided "now" instead of database NOW() to keep semantics
    consistent across drivers and tests.
*/
const QExtendLeaseAtomic = `
UPDATE th_jobs
SET lease_expires_at = $1
WHERE id = $2
  AND status = 'inflight'
  AND lease_token = $3
  AND lease_expires_at > $4
RETURNING lease_expires_at
`

/*
QLeaseState fetches the current lease-related state for a job.

Why we do this:
- QExtendLeaseAtomic returns no rows when it doesn't extend the lease.
- We still want to return the same errors as the memory driver:
  - not inflight
  - expired (takes precedence over mismatch)
  - token mismatch
  - This query is used only to classify that "0 rows updated" case without relying on a transaction.
*/
const QLeaseState = `
SELECT status, lease_token, lease_expires_at
FROM th_jobs
WHERE id = $1
`
