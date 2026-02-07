# TaskHarbor Architecture

TaskHarbor is a background job framework for Go with pluggable drivers and an upcoming durable workflow layer.

Core idea: separation of concerns

- Core defines the API and semantics (what “correct job processing” means).
- Drivers implement storage and reservation (how jobs are stored, leased, and transitioned).
- Workflows build on the same semantics (Chain, Group, Chord), using durable state on Postgres.

This keeps application code stable while allowing backend changes.

## Core components

1. Client

- Enqueues jobs with options (queue, run_at, max_attempts, timeout, idempotency_key).
- Encodes payloads via a codec (default JSON).

2. Worker

- Registers handlers for job types.
- Reserves runnable jobs from a driver with a lease.
- Executes handlers with middleware, timeouts, and panic recovery.
- On success: Ack.
- On failure: Retry with backoff (or DLQ when max_attempts is exceeded / unrecoverable).

3. Driver (backend)

- Persists jobs and enforces reservation/leases.
- Provides state transitions that are safe under concurrency (reserve, extend lease, ack, retry, fail).
- Drivers must not implement policy; core owns policy (retry logic, backoff, DLQ decision).

4. Workflow layer (future milestones)

- Provides builders (Chain, Group, Chord).
- Persists workflow state durably (especially on Postgres).
- Drives progression by enqueueing jobs and reacting to completion events.

## Processing model

- Delivery is at-least-once.
- Leases prevent double processing during a valid lease.
- If a worker crashes mid-job, the lease expires and the job becomes runnable again.
- Handlers should be idempotent for side effects (emails, payments, writes).

## Time and determinism

TaskHarbor passes “now” down into driver operations (Reserve/Ack/Retry/Fail/ExtendLease). Drivers should use the provided time instead of calling time.Now internally.

This is what makes:

- tests deterministic (no sleeps),
- behavior consistent across memory and Postgres,
- lease validation unambiguous.

## Driver strategy (v0.1)

Drivers shipped in v0.1:

- memory: local dev + reference semantics
- postgres: durable production driver with migrations

Future drivers must pass conformance tests to ensure consistent semantics.

## Extension points

- Codec: pluggable payload encoding/decoding (JSON default).
- Middleware: wraps handler execution for logging, metrics, tracing, panic recovery, timeouts.
- Retry policy: pluggable backoff; default exponential with jitter.
- Observability: hooks/interfaces so core does not hard-depend on Prometheus or OpenTelemetry.

## Minimal public API shape (conceptual)

- NewClient(driver) -> Client
- NewWorker(driver, opts...) -> Worker
- Worker.Register(type, handler)
- Worker.Run(ctx)
- Client.Enqueue(ctx, JobRequest)

Core is intentionally small and stable. Drivers should not leak policy into user code.
