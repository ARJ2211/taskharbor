# Driver Contract

This document defines the driver contract TaskHarbor drivers must implement.

Goal
- Drivers provide storage + reservation primitives.
- Core provides semantics (retries, leases, DLQ, idempotency, timeouts).
- Every driver must behave consistently, verified via conformance tests.


## Required concepts

Job
- Identified by JobID (string or UUID).
- Has: name, queue, payload bytes, run_at, attempts, max_attempts, timeout, idempotency_key, timestamps.

States
- scheduled: run_at is in the future
- ready: runnable now
- leased: reserved by a worker until lease_expires_at
- done: completed successfully (may be deleted or archived)
- dlq: permanently failed (max attempts exceeded or explicitly dead-lettered)

Lease
- A driver must ensure a leased job is not delivered to another worker until the lease expires.
Semantics
- reserve creates a lease with a token and expiry time
- ack must require the correct lease token (or equivalent protection)
- extend lease updates the expiry time for a valid token
- when a lease expires, the job becomes reservable again


## Interface (conceptual)

Enqueue
- Create a job in scheduled or ready state depending on run_at.
- If idempotency_key is set and already exists, return the existing job id and do not create a duplicate.

Reserve
- Return the next runnable job for a set of queues, and lease it.
- Must be atomic: two workers must not lease the same job at once.
- Reserve should prefer earliest runnable jobs (best effort ordering, not strict).

Ack
- Mark a leased job successful.
- Must be safe if called multiple times (idempotent) for the same lease token.

Fail
- Record failure for a leased job.
- Core will decide whether this failure triggers a retry or DLQ.
- Driver must support updating attempts and scheduling next run_at for retry.
- Must be safe if called multiple times (idempotent) for the same lease token.

ExtendLease
- Extend the lease expiry for a leased job with a valid token.
- If token is invalid or lease already expired, return a specific error.

DLQ operations
- Driver must store DLQ entries with: job data, failure reason, last error, timestamp.
- Requeue from DLQ must be supported (CLI uses this later).

Query / Inspect
- Driver should support inspection APIs for CLI:
  - get job by id
  - list jobs by state/queue (paged)


## Error expectations

Drivers should provide typed errors for:
- ErrNotFound
- ErrLeaseLost (token invalid / lease expired)
- ErrDuplicateKey (idempotency_key conflict, if surfaced)

Core will translate errors into user-facing behavior.


## Conformance expectations

A driver is acceptable when it passes conformance tests for:
- scheduling (run_at)
- retries + backoff + DLQ
- leases + reclaim on expiry
- idempotency_key behavior
- concurrency safety (no double execution during valid lease)
