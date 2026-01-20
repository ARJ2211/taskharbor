# TaskHarbor Architecture

TaskHarbor is a background job framework for Go with pluggable drivers and a durable workflow layer.

The key idea is separation:
- Core defines the API and semantics (what a job system means).
- Drivers implement storage and reservation (how jobs are stored and leased).
- Workflows build on the same semantics (chain, group, chord).

This keeps application code stable while allowing backend changes.


## Core components

1. Client
- Enqueues jobs with options (queue, run_at, max_attempts, timeout, idempotency_key).
- Encodes payloads via a codec (default JSON).

2. Worker
- Registers handlers for job names.
- Reserves runnable jobs from a driver with a lease.
- Executes handlers with timeouts and middleware.
- Acks success or fails with retry/DLQ behavior.

3. Driver (backend)
- Persists jobs and controls reservation/leases.
- Provides atomic reservation so only one worker owns a job during a lease.
- Supports acks, failures, retries, DLQ, and scheduling.

4. Workflow engine
- Provides builders (Chain, Group, Chord).
- Stores workflow state durably (especially on Postgres).
- Drives progression by enqueueing jobs and reacting to completion events.


## Processing model

- Delivery is at-least-once.
- Leases prevent double processing during a valid lease.
- If a worker crashes mid-job, the lease expires and the job becomes runnable again.
- Users must write idempotent handlers (or use idempotency keys + external dedupe) for side-effecting work.


## Driver strategy (v0.1)

Drivers shipped in v0.1:
- memory: local dev + tests + reference semantics
- postgres: production driver with durable state

Future drivers are possible, but must pass conformance tests to ensure consistent semantics.


## Boundaries and extension points

- Codec: pluggable payload encoding/decoding (JSON default).
- Middleware: wraps handler execution for logging, metrics, tracing, panic recovery, timeouts.
- Retry policy: pluggable backoff; default exponential with jitter.
- Observability: hooks/interfaces so core does not hard-depend on Prometheus or OpenTelemetry.


## Minimal public API shape (conceptual)

- Open(driverConfig) -> Backend
- Backend.Client() -> Client
- Backend.Worker(opts) -> Worker
- Worker.Handle(name, handler)
- Worker.Run(ctx)
- Client.Enqueue(ctx, name, payload, opts)

This is intentionally small and stable. Drivers should not leak into user code.
