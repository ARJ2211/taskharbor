# Driver Contract

This is the stable contract between TaskHarbor core and any driver implementation.

Drivers may differ in storage and performance, but they must match behavior semantics.

## Core invariants

- At-least-once delivery.
- Jobs are runnable when run_at is zero/NULL or run_at <= now.
- Reserve is exclusive for the duration of a valid lease.
- Expired leases are reclaimable and jobs become runnable again.
- Ack marks a job successful (terminal).
- Fail moves a job to DLQ (terminal).
- Retry reschedules a job with updated attempts + error.

## Interface

### Enqueue(record) (id string, existed bool, err error)

- Store a job in ready or scheduled form depending on run_at.

Idempotency and existed semantics

- If record.IdempotencyKey is empty:
    - The driver must always create a new job.
    - Return (record.ID, false, nil) on success.

- If record.IdempotencyKey is non-empty:
    - The driver must dedupe by the tuple (record.Queue, record.IdempotencyKey).
    - If a mapping already exists for that tuple, Enqueue must be a no-op and return:
        - (existingJobID, true, nil).
    - If no mapping exists, the driver must create the job and atomically create the mapping, then return:
        - (record.ID, false, nil).

Notes

- Dedupe is queue-scoped: the same idempotency key may be reused in different queues.
- Dedupe is based on (queue, idempotency_key) only. If a duplicate enqueue supplies different payload/type/run_at,
  the driver still returns the existing job and must not create a second runnable copy.

### Reserve(queue, now, lease_for)

- Return ok=false when no runnable job exists.
- Must reclaim expired inflight jobs (lazy reclaim during Reserve is acceptable).
- Parity rule with memory driver: when reclaiming an expired inflight job, treat it as runnable now (run_at becomes zero/NULL).

Reclaim semantics

- When a lease has expired and the job is reclaimed:
    - The job becomes runnable immediately.
    - run_at must be cleared.
    - lease token and lease expiry must be cleared.

### Ack(id, token, now)

- Mark an inflight job successful (terminal: done or delete).
- Must validate token and expiry at now.
- If token mismatches or lease expired, return a lease error and do not mutate state.

Terminal idempotency

- If the job is already in the terminal done state, Ack must return nil.

### ExtendLease(id, token, now, lease_for)

- Must validate token and not expired before extending.
- Should return ErrLeaseMismatch / ErrLeaseExpired / ErrJobNotInflight accordingly.

Retry(id, token, now, update)

- Must validate token and not expired.
- Must update attempts and last_error metadata.
- If update.run_at is zero, job becomes runnable immediately.
- If update.run_at is non-zero, job becomes scheduled for that time.

### Fail(id, token, now, reason)

- Move an inflight job to DLQ (terminal).
- Must validate token and expiry at now before mutating state.
- Core decides when Fail should be used (max attempts exceeded or unrecoverable).

Terminal idempotency

- If the job is already terminal (dlq or done), Fail must return nil.

## Notes on timestamps

For deterministic behavior and cross-driver parity:

- Drivers should use the provided now parameter for lease validation and scheduling decisions.
- Backends like Postgres store TIMESTAMPTZ at microsecond precision, so tests should compare times at microsecond precision.

Idempotency and the existed flag

- If JobRecord.IdempotencyKey is non-empty, Enqueue MUST behave as a dedupe operation scoped to (queue, idempotency_key).
- On the first enqueue for a given (queue, idempotency_key), the driver creates the job and returns (jobID, existed=false).
- On a duplicate enqueue with the same (queue, idempotency_key), the driver MUST NOT create a second runnable copy. It returns (existingJobID, existed=true).
- A duplicate enqueue MUST NOT mutate the existing job record (payload/type/run_at/etc.). It is strictly a lookup/dedupe.
- If IdempotencyKey is empty, existed MUST be false.

What this enables: callers can safely retry an enqueue request (network retry, client timeout, etc.) without risking duplicate runnable jobs.

Terminal idempotency

- Ack: once a job is marked done, subsequent Ack calls for that job MUST succeed (return nil), even though the job is no longer inflight.
- Fail: once a job is moved to the DLQ (or is already done), subsequent Fail calls MUST succeed (return nil).
- Retry: retrying a job that is already done or in the DLQ MUST return ErrJobNotInflight.

## Error expectations

Drivers should return:

- ErrJobNotInflight when Ack/Retry/Fail/ExtendLease is called on a job that is not inflight
- ErrLeaseMismatch when token does not match the active lease token
- ErrLeaseExpired when the lease is expired at the provided now time

Terminal idempotency exceptions

- Ack on a done job returns nil.
- Fail on a dlq or done job returns nil.

Drivers must not silently succeed on invalid lease operations (wrong token or expired lease) unless the job is already terminal.
