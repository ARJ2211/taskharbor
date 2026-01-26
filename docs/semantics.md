# Semantics

This document defines TaskHarbor runtime semantics. These rules are what users can rely on.

## Delivery model

- At-least-once processing.
- Exactly-once is not guaranteed.
- Handlers should be idempotent, especially for side effects (emails, payments, writes).

## Scheduling (run_at)

- Jobs with run_at in the future are scheduled and must not be reserved before run_at.
- After run_at, jobs become runnable.
- Timing is best-effort. There is no hard real-time guarantee.

## Retries and backoff

- Each job has attempts and max_attempts.
- attempts is the number of recorded failures so far.
- On a failure, core increments attempts and either:
    - schedules a retry by setting run_at to a future time (backoff), or
    - moves the job to DLQ.
- Backoff policy decides the next run_at (default: exponential + jitter).
- A failure is dead-lettered when attempts reaches max_attempts.
- Handlers can mark a failure as unrecoverable so it goes to DLQ immediately.

## Timeouts and cancellation

- Each job may define a timeout.
- Worker executes handler with a context that is cancelled on timeout or shutdown.
- A timeout is treated as a failure (retryable by default).

## Leases and crash recovery

Reserve returns a lease token and lease expiry time.

Rules:

- While a lease is valid, no other worker should receive the job.
- Workers may extend leases for long-running jobs via heartbeat (ExtendLease).
- If a worker crashes or fails to ack, the lease expires and the job becomes runnable again.
- Ack/Retry/Fail must provide the current lease token. If the token is stale or the lease is expired, the driver must reject the operation (no mutation).

What this means in practice:

- A job may execute more than once if a worker crashes after performing side effects but before ack.
- Idempotent handlers are required for correctness.
- Heartbeats reduce duplicate processing for long-running handlers, but do not eliminate at-least-once behavior.

## Dead-letter queue (DLQ)

- DLQ stores jobs that exceeded max_attempts (or are explicitly dead-lettered).
- DLQ entries retain failure info for inspection.
- Jobs can be requeued from DLQ (CLI supports later milestones).

## Idempotency key

- Enqueue may include idempotency_key.
- If the same key is enqueued again, driver returns the existing job id and does not create a duplicate.
- Idempotency_key only dedupes enqueue, not execution side effects.
- Use it to prevent duplicate job creation (double submits, retries at API layer).

## Workflows (overview)

Workflows are composed of jobs but are durable:

- Chain: linear A then B then C.
- Group: parallel fanout.
- Chord: group + finalizer when all complete.

Workflow state is persisted (especially on Postgres) so it resumes after crashes.
