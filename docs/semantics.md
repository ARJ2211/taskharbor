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
- A failure increments attempts and schedules a retry if attempts < max_attempts.
- Backoff policy decides next run_at (default: exponential + jitter).
- When attempts reaches max_attempts, the job is moved to DLQ.


## Timeouts and cancellation

- Each job may define a timeout.
- Worker executes handler with a context that is cancelled on timeout or shutdown.
- A timeout is treated as a failure (retryable by default).


## Leases and crash recovery

- Reserve returns a lease token and lease expiry time.
- While lease is valid, no other worker should receive the job.
- If worker crashes or fails to ack, lease expires and job becomes runnable again.
- Workers may extend leases for long-running jobs via heartbeat.

Implication
- A job may execute more than once if a worker crashes after performing side effects but before ack.
- Idempotent handlers are required.


## Idempotency key

- Enqueue may include idempotency_key.
- If the same key is enqueued again, driver returns the existing job id and does not create a duplicate.
- Idempotency_key only dedupes enqueue, not execution side effects.
- Use it to prevent duplicate job creation (double submits, retries at API layer).


## Dead-letter queue (DLQ)

- DLQ stores jobs that exceeded max_attempts (or are explicitly dead-lettered).
- DLQ entries retain failure info for inspection.
- Jobs can be requeued from DLQ (CLI supports later milestones).


## Workflows (overview)

Workflows are composed of jobs but are durable:
- Chain: linear A then B then C.
- Group: parallel fanout.
- Chord: group + finalizer when all complete.

Workflow state is persisted (especially on Postgres) so it resumes after crashes.
