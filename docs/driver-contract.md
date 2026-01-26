# Driver Contract

This document defines what a TaskHarbor driver must implement, and what the
core runtime (client/worker) is responsible for.

Principle

- Drivers provide storage + primitive state transitions.
- Core provides semantics (retries, backoff, timeouts, panic recovery, leases).
- Drivers should stay dumb so new backends can be contributed safely.

## Job record

At minimum, a driver persists these fields:

- id (string)
- type (string)
- queue (string)
- payload (bytes)
- run_at (time): earliest time the job is eligible to run
- timeout (duration)
- attempts (int): number of recorded failures so far
- max_attempts (int): maximum total executions allowed (0 means unset; core may default)
- last_error (string)
- failed_at (time)

## States

These are conceptual; drivers may store them however they want.

- ready: runnable now (run_at is zero or <= now)
- scheduled: run_at is in the future
- inflight: reserved by a worker and protected by a lease
- dlq: dead-lettered jobs

## Leases

A lease represents temporary ownership of an inflight job by a worker.

- Reserve returns a lease token + expiry time.
- While the lease is valid, no other worker should be able to reserve the same job.
- A worker may extend a lease for long-running jobs via ExtendLease.
- If a worker crashes (or never acks), the lease will expire and the job becomes reservable again.

The driver is responsible for enforcing leases. Core uses them by heartbeating and by passing the lease token into Ack/Retry/Fail.

## Interface (milestone 4)

Enqueue

- Store a job in ready or scheduled state depending on run_at.

Reserve(queue, now, lease_for)

- Return one runnable job for a queue and move it to inflight.
- Must return (ok=false, nil error) when nothing runnable.
- Must never return a job that is already inflight with a valid lease.
- Must return a lease (token + expiry)s.
- lease_for must be > 0.

ExtendLease(id, token, now, lease_for)

- Extend the existing lease for an inflight job.
- Must validate the token and that the current lease has not expired.
- Must persist the new expiry.
- May keep the same token or rotate it. If rotated, return the new token in the Lease.

Ack(id, token, now)

- Mark an inflight job successful (terminal).
- Must validate token and expiry.
- If token mismatches or lease expired, return a lease error and do not mutate state.

Retry(id, token, now, update)

- Move an inflight job back to ready/scheduled.
- Persist failure metadata (attempts, last_error, failed_at) and the new run_at.
- Must validate token and expiry before mutating state.
- Core decides when to retry and whether to DLQ. Drivers only persist.

Fail(id, token, now, reason)

- Move an inflight job to DLQ (terminal).
- Must validate token and expiry before mutating state.
- Core decides when Fail should be used (max attempts exceeded or unrecoverable).

Close

- Mark driver closed. Further operations should error.

## Error expectations

Drivers should return:

- ErrJobNotInflight when Ack/Retry/Fail/ExtendLease is called on a job that is not inflight
- ErrInvalidLeaseDuration when lease_for <= 0
- ErrLeaseMismatch when token does not match the active lease token
- ErrLeaseExpired when the lease is expired at the provided now time

## Reclaiming expired leases

Drivers must ensure expired inflight jobs become reservable again.

Implementation strategies vary by backend:

- memory driver can reclaim lazily during Reserve by scanning inflight and moving expired jobs back to ready
- postgres driver can reclaim by selecting rows with lease_expires_at <= now, or by letting Reserve treat them as runnable
