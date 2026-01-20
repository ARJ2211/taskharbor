# TaskHarbor
Pluggable background jobs and workflows for Go.

This document is the project plan and roadmap for TaskHarbor. It is intentionally detailed so development can run milestone-by-milestone without losing scope control.

Date: 2026-01-20
Owner: ARJ2211


## 1) Problem we are solving

Go has strong job queue libraries, but each library is its own world:
- Your application code ends up coupled to one queue API and one backend model.
- Moving from one backend to another (or one library to another) often requires rewriting your job layer.
- Workflow patterns (chain, fanout, join) are frequently implemented ad-hoc per project.

TaskHarbor aims to make background jobs in Go feel like databases in Go:
- Application code depends on one stable API.
- Backends are pluggable via drivers.
- Core semantics are consistent across drivers.
- Simple workflows are first-class and durable.


## 2) Goals

G1. Stable core API
- A small, stable package surface that user code depends on.
- Minimal churn across releases.

G2. Pluggable drivers
- A driver contract that backends implement.
- Drivers can be added by contributors without changing core semantics.

G3. Production-grade reliability semantics
- Retries with backoff, max attempts, dead-letter queue (DLQ).
- Leases / visibility timeouts to handle worker crashes.
- Idempotency keys to prevent duplicate enqueues.
- Timeouts and cancellation.

G4. Simple workflow primitives
- Chain: A then B then C
- Group: run N tasks in parallel
- Chord: run a finalizer after a group completes
- Durable state and crash-safe resumption

G5. Operator and developer UX
- CLI for running workers, enqueueing, inspecting jobs, DLQ operations.
- Observability hooks (logs, metrics, tracing).


## 3) Non-goals (explicit)

NG1. Not a full workflow engine like Temporal
- No complex event-sourcing history replay.
- No long-running saga engine with external signals, timers, and advanced orchestration.

NG2. Not competing on “yet another queue implementation” features
- The differentiator is the standard API + driver contract + consistent semantics + workflows.
- We will not chase every feature from existing queue libraries.

NG3. No dashboard UI in the first 3 months
- CLI + hooks first. UI can be later.

NG4. No “support every backend” in v0.1
- First drivers: memory and Postgres.
- Additional drivers are later milestones or community contributions.


## 4) Guiding principles

P1. Small core, strong semantics
- Core defines behavior; drivers provide storage/locking.

P2. Test-driven correctness
- Driver conformance tests enforce consistent behavior.
- Integration tests validate production drivers.

P3. Predictable failure modes
- Documented guarantees and documented limitations.

P4. Boring defaults, configurable edges
- Default JSON encoding, default backoff, default structured logs.
- Extensible via options and middleware.

P5. Easy to contribute
- Clear driver contract.
- Driver template and conformance suite.
- “Good first issue” path.


## 5) Key terminology

Job
- A unit of background work: name, payload, options (retries, timeout, schedule).

Task payload
- Encoded bytes representing the job input. Default codec is JSON.

Handler
- Function registered for a job name. Worker invokes it.

Backend / driver
- Implementation of the storage + reservation mechanics (memory, Postgres, etc.).

Reserve
- Operation that returns the next runnable job plus a lease.

Lease (visibility timeout)
- A time window during which a reserved job is owned by a worker.
- If the worker crashes, the lease expires and the job becomes reservable again.

Ack
- Mark job success; remove or finalize it.

Fail
- Mark job failure; schedule retry or move to DLQ.

DLQ
- Dead-letter queue for jobs that exceeded max attempts (or are explicitly dead-lettered).

Workflow run
- A durable execution state for a composed set of jobs (chain/group/chord).


## 6) High-level architecture

Core packages
- taskharbor: public API (Client, Worker, Backend, middleware, retry policies, options, codec)
- taskharbor/flow: workflow builder and workflow engine
- taskharbor/driver/*: backend drivers

Drivers
- driver/memory: reference driver for tests and local dev
- driver/postgres: production driver

Observability
- hooks and middleware in core (logger interface, metrics interface, tracing interface)
- optional adapters (Prometheus, OpenTelemetry) as separate packages to avoid heavy deps


## 7) Repository structure (target)

.
├─ cmd/taskharbor/                 CLI
├─ taskharbor/                     core public API
│  ├─ client.go
│  ├─ worker.go
│  ├─ types.go
│  ├─ options.go
│  ├─ middleware.go
│  ├─ retry.go
│  ├─ codec.go
│  ├─ errors.go
│  └─ internal/                    internal helpers (not exported)
├─ taskharbor/flow/                workflow engine and builders
├─ taskharbor/driver/memory/
├─ taskharbor/driver/postgres/
├─ examples/
├─ docs/
├─ .github/workflows/
└─ ROADMAP.md                      this file


## 8) Development workflow per milestone

Each milestone will follow this loop:
1. Confirm scope and “definition of done”
2. Implement core + driver changes
3. Add unit tests and driver conformance tests
4. Add integration tests (if driver milestone)
5. Provide a small demo command or example
6. Update docs with any new semantics
7. Tag milestone completion in ROADMAP.md

When asking for milestone review (each week), include:
- file tree for relevant folders
- go test ./... output
- go test -race ./... output (when concurrency is involved)
- integration test output for Postgres (when introduced)
- short list of blockers (2–3 bullets)


## 9) Milestones (12-week plan)

Milestone 1 (Week 1): Documentation and scaffolding
Objective
- Lock down semantics and project structure before writing significant code.

Must deliver
- docs/architecture.md
- docs/driver-contract.md
- docs/semantics.md (retries, leases, idempotency, DLQ, scheduling)
- ROADMAP.md (this file)
- CONTRIBUTING.md, CODE_OF_CONDUCT.md, LICENSE
- CI baseline: go test + golangci-lint (or at minimum gofmt + go test)

Implementation tasks
- Write the driver contract with method semantics and required invariants.
- Define “at-least-once” processing model and what users must do for idempotency.
- Define how timeouts, cancellations, and retries interact.
- Decide default codec and how users override it.
- Decide error taxonomy for retryable vs non-retryable failures.

Acceptance criteria
- A new contributor can read the docs and understand:
  - what TaskHarbor guarantees
  - how to implement a driver
  - how workflows are intended to work (high-level)

Demo
- None (docs-only)

Risks to watch
- Over-specifying too early. Keep contract minimal but strict.


Milestone 2 (Week 2): Core API + memory driver v0
Objective
- Achieve end-to-end enqueue + worker execution using memory driver.
- Validate the public API shape early.

Must deliver
- Core API: Backend, Client, Worker, Handler registration, Job definition, Options
- Memory driver: enqueue, schedule (run_at), reserve, ack, fail
- Basic middleware support (chainable)
- Unit tests for “happy path” and “scheduled job”

Implementation tasks
- Define Job model: id, name, payload, run_at, attempts, max_attempts, timeout, idempotency_key, metadata.
- Define Handler signature: func(ctx context.Context, payload []byte) error
- Implement Worker loop with configurable concurrency.
- Implement memory storage with:
  - ready queue
  - scheduled min-heap by run_at
  - in-flight tracking
  - DLQ store
  - idempotency map (key -> job id)

Tests
- Enqueue -> processed exactly once.
- Scheduled job does not run before run_at, does run after.
- Worker shutdown is graceful (does not drop in-progress jobs in memory driver model).

Acceptance criteria
- go test ./... passes
- go test -race ./... passes
- A minimal example can run in under 30 lines of Go.

Demo command idea
- go run ./examples/basic-email (added later) or a simple main in /cmd sandbox (optional).


Milestone 3 (Week 3): Reliability v0 (retries, backoff, DLQ, timeouts, panic recovery)
Objective
- Define and implement failure semantics in core, verified via tests.

Must deliver
- Retry policy interface and default exponential backoff with jitter
- Max attempts enforcement and DLQ move
- Per-job timeout and context cancellation
- Panic recovery middleware

Implementation tasks
- Add error classification:
  - retryable errors (default)
  - non-retryable errors (explicit)
- Determine how “timeout exceeded” is treated (retryable by default).
- Implement DLQ record with failure reason and last error.

Tests
- A job that fails N times ends in DLQ with attempts = N.
- Backoff scheduling increases delay over attempts.
- Timeout cancels handler; failure is recorded.
- Panic in handler does not crash worker; job is retried or DLQed.

Acceptance criteria
- Memory driver passes reliability tests deterministically (no flaky timing).


Milestone 4 (Week 4): Lease model + crash recovery
Objective
- Make job reservation safe under worker crashes.

Must deliver
- Lease/visibility timeout semantics in core
- Reserve returns a lease token and lease expiry
- Extend lease (heartbeat) API
- Reclaim logic: expired leases make job reservable again

Implementation tasks
- Define “reserve” contract:
  - reserved job must not be delivered to another worker during valid lease
  - after lease expiry, job becomes eligible for reserve
- Implement in memory driver:
  - in-flight map includes lease expiry and token
  - periodic reaper that moves expired jobs back to ready/scheduled

Tests
- Reserve job, do not ack, wait for lease expiry, ensure job is reservable again.
- Two workers do not process the same job concurrently under normal operation.
- Lease extend prevents reclaim during long-running job.

Acceptance criteria
- go test -race covers lease code.
- The lease model is documented in docs/semantics.md.


Milestone 5 (Week 5): Postgres driver v0 (durable jobs)
Objective
- Provide a production-grade driver backed by Postgres.

Must deliver
- Postgres schema and migrations
- Enqueue, schedule, reserve with lease, ack, fail, DLQ
- Integration tests running against Postgres

Implementation tasks
- Decide schema tables:
  - jobs
  - dlq
  - optional queues table (or enum) for stats
- Implement reserve pattern (transaction + SKIP LOCKED or equivalent) with lease expiry.
- Ensure scheduled jobs become runnable when run_at <= now.

Tests
- Integration: enqueue and process.
- Integration: retry and DLQ.
- Integration: lease expiry reclaim.
- Concurrency: multiple workers reserve without duplication.

Acceptance criteria
- CI can run Postgres integration tests reliably.
- Schema is documented in docs/driver-postgres.md.


Milestone 6 (Week 6): Postgres hardening (idempotency, reaper, performance)
Objective
- Make Postgres driver safe and predictable under real load and failures.

Must deliver
- Idempotency keys using a unique constraint strategy
- Reaper loop for stuck leases (optional if lease expiry is query-driven, but must be covered)
- Indexes and query tuning basics
- Stress tests (lightweight)

Implementation tasks
- Define idempotency behavior:
  - Same key returns existing job id (or no-op) without enqueueing duplicates.
- Add indexes for runnable queries and lease expiry.
- Ensure fail/ack are idempotent operations.

Tests
- Idempotency: enqueue same key twice -> only one job exists.
- Crash simulation: reserve, kill worker, later worker picks up after lease expiry.
- High-volume: enqueue many jobs; reserve remains performant (basic benchmark optional).

Acceptance criteria
- Driver passes conformance suite (introduced later).
- Performance is “reasonable” with indexes present (documented expectations).


Milestone 7 (Week 7): Driver conformance test suite
Objective
- Create a single behavior test suite that every driver must pass.

Must deliver
- package taskharbor/conformance (or similar)
- memory driver runs conformance tests
- postgres driver runs conformance tests

Implementation tasks
- Define conformance test categories:
  - enqueue and run_at scheduling
  - retry and DLQ
  - lease reclaim
  - idempotency (if supported in driver; for v0.1 it must be supported in both)
  - shutdown safety

Acceptance criteria
- Adding a new driver means “implement interface + pass conformance”.

Note
- This milestone is key for collaboration. It prevents semantic drift across drivers.


Milestone 8 (Week 8): CLI v0
Objective
- Make TaskHarbor usable without writing custom scripts.

Must deliver
- cmd/taskharbor CLI with:
  - worker run --driver=postgres|memory --dsn=... --queues=... --concurrency=...
  - enqueue --job=name --json='{}' --run-at=... --queue=...
  - inspect --id=...
  - list --queue=... --state=ready|scheduled|inflight|dlq
  - dlq list, dlq requeue, job retry

Implementation tasks
- Decide config format (flags first; optional config file later).
- Implement safe payload printing (redaction options).

Acceptance criteria
- You can do a full local run with Postgres using only CLI commands.


Milestone 9 (Week 9): Workflows v0 (Chain)
Objective
- Add durable workflows with minimal complexity.

Must deliver
- flow.Chain builder
- Durable workflow run state stored in driver (or a workflow store interface implemented by Postgres driver first)
- Crash-safe resumption

Implementation tasks
- Define workflow data model:
  - workflow_run: id, status, created_at, updated_at
  - workflow_node: node id, type, job name, payload ref, status, attempts, dependencies
- Engine behavior:
  - Start chain -> enqueue first node
  - On node success -> enqueue next
  - On node failure -> apply retry policy per node; fail workflow if node DLQed (v0)

Acceptance criteria
- A 3-step chain continues correctly after worker restart mid-chain.


Milestone 10 (Week 10): Workflows v1 (Group + Chord)
Objective
- Support fanout and join patterns.

Must deliver
- flow.Group builder
- flow.Chord builder: chord(group, finalizer)
- Join tracking and finalizer scheduling once all group nodes succeed

Implementation tasks
- Decide how to represent “results”
  - store small results directly, or store references only (preferred)
- Ensure join is idempotent:
  - finalizer is enqueued exactly once even if multiple workers observe completion.

Tests
- Group runs all children and waits.
- If one child retries, chord waits.
- After all succeed, finalizer runs exactly once.

Acceptance criteria
- Example workflow: CSV import (fanout chunks, join commit) works reliably.


Milestone 11 (Week 11): Observability hooks (logs, metrics, tracing)
Objective
- Provide production visibility without forcing dependencies.

Must deliver
- Logging interface and default logger (structured)
- Metrics interface and optional Prometheus adapter package
- Tracing interface and optional OpenTelemetry adapter package
- Core emits events: enqueue, reserve, start, success, fail, retry, dlq

Implementation tasks
- Keep interfaces minimal.
- Avoid hard dependency on Prometheus or OTel in core.

Acceptance criteria
- Users can hook their own logger/metrics/tracer without forking core loops.


Milestone 12 (Week 12): Examples, docs finalization, and v0.1.0 release
Objective
- Make it easy to adopt and easy to contribute.

Must deliver
- examples:
  - basic email job
  - csv import chain + chord
  - image thumbnail fanout + chord
- docs:
  - quickstart.md
  - workflows.md
  - postgres-driver.md
  - reliability.md (guarantees, failure modes, recommended patterns)
- Release:
  - CHANGELOG.md
  - version tag v0.1.0
  - stable API statement and upgrade notes

Acceptance criteria
- Someone new can follow the quickstart and run:
  - Postgres worker
  - enqueue jobs
  - see retries and DLQ
  - run a workflow example


## 10) Success criteria for the 3-month window

- Memory + Postgres drivers implemented and passing conformance tests.
- Clear semantics documentation (leases, retries, DLQ, idempotency, scheduling).
- CLI v0 usable for real development.
- Workflows (chain, group, chord) functional and durable on Postgres.
- Observability hooks in place.
- v0.1.0 release published with examples and contributor docs.


## 11) Backlog ideas (post v0.1.0)

- Redis driver (native) or adapter driver that wraps an existing Redis queue library
- Web dashboard
- Cron/periodic jobs
- Priority queues per job
- Rate limiting per queue/tenant
- Multi-tenant namespaces
- Result backend options
- Better payload storage abstraction (blob store integration)
- Exactly-once processing options (where possible) and stronger idempotency helpers
