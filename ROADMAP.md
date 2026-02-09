# TaskHarbor Roadmap

Pluggable background jobs and simple workflows for Go.

Date: 2026-01-20
Owner: ARJ2211

## What we are building

TaskHarbor is a stable Go API for background jobs plus a driver system underneath it.

- Your app code uses one API (client, worker, job handlers).
- Drivers provide storage and reservation (memory, Postgres first).
- Core semantics are consistent across drivers (retries, leases, DLQ, idempotency).
- Workflows are first-class and durable (chain, group, chord).

## What we are not building

- Not a full workflow engine like Temporal.
- Not another one-off queue library that competes only on features.
- Not every backend in v0.1.0 (start with memory + Postgres).
- Not a web dashboard in the first release (CLI + hooks first).

## Core guarantees (v0.1.0)

- Processing model: at-least-once.
- Leases (visibility timeouts) prevent double processing during a valid lease.
- Retries use a defined backoff policy; max attempts moves to DLQ.
- Idempotency keys prevent duplicate enqueue for the same key.
- Scheduled jobs run after run_at (best-effort timing, not real-time exact).

## Target repo structure

```text
├── .github
│   └── workflows
│       └── go.yml
├── cmd
│   └── taskharbor
├── conformance
├── docker
│   └── postgres-init
│       └── 001_create_test_db.sql
├── docs
│   ├── architecture.md
│   ├── driver-contract.md
│   ├── postgres.md
│   ├── retry-policy.md
│   └── semantics.md
├── examples
│   ├── basic
│   │   └── main.go
│   ├── basic-postgres
│   │   └── main.go
│   └── stress-postgres
│       └── main.go
├── public
│   └── data flow.png
├── taskharbor
│   ├── driver
│   │   ├── memory
│   │   │   ├── memory.go
│   │   │   ├── memory_lease_test.go
│   │   │   └── memory_test.go
│   │   ├── postgres
│   │   │   ├── migrations
│   │   │   │   └── 001_init.sql
│   │   │   ├── migrations.go
│   │   │   ├── migrations_test.go
│   │   │   ├── options.go
│   │   │   ├── postgres.go
│   │   │   ├── postgres_ack_test.go
│   │   │   ├── postgres_e2e_test.go
│   │   │   ├── postgres_enqueue_test.go
│   │   │   ├── postgres_extend_lease_test.go
│   │   │   ├── postgres_fail_test.go
│   │   │   ├── postgres_reserve_test.go
│   │   │   ├── postgres_retry_test.go
│   │   │   └── queries.go
│   │   └── driver.go
│   ├── flow
│   ├── internal
│   │   └── envutil
│   │       └── dotenv.go
│   ├── client.go
│   ├── client_test.go
│   ├── clock.go
│   ├── codec.go
│   ├── codec_test.go
│   ├── middleware.go
│   ├── options.go
│   ├── options_test.go
│   ├── recover.go
│   ├── retry.go
│   ├── retry_test.go
│   ├── timeout.go
│   ├── types.go
│   ├── unrecoverable.go
│   ├── worker.go
│   ├── worker_crash_recovery_test.go
│   ├── worker_heartbeat_lease_test.go
│   ├── worker_panic_test.go
│   ├── worker_retry_test.go
│   ├── worker_schedule_test.go
│   ├── worker_test.go
│   └── worker_timeout_test.go
├── .env.example
├── .gitignore
├── CONTRIBUTING.md
├── DATAFLOW.md
├── LICENSE
├── README.md
├── ROADMAP.md
├── docker-compose.yml
├── go.mod
└── go.sum
```

## Milestones (12 weeks)

Each milestone should end with:

- go test ./...
- go test -race ./... (when concurrency is touched)
- milestone checkbox marked done in this file

Milestone 1: Docs + scaffolding (C)

- Deliver: docs/architecture.md, docs/driver-contract.md, docs/semantics.md
- Deliver: LICENSE, CONTRIBUTING.md, CODE_OF_CONDUCT.md, basic CI
- Done when: a new dev can understand guarantees, driver rules, and workflow intent.

Milestone 2: Core API + memory driver v0 (C)

- Deliver: Client, Worker, Backend, handler registry, options, codec interface
- Deliver: memory driver enqueue, schedule(run_at), reserve, ack, fail, DLQ storage
- Tests: enqueue executes, schedule timing, graceful shutdown
- Done when: end-to-end run works with memory driver.

Milestone 3: Reliability v0 (C)

- Deliver: retry policy + exponential backoff with jitter
- Deliver: timeout per job + context cancellation
- Deliver: panic recovery middleware
- Tests: fails retry then DLQ, timeout path, panic path
- Done when: failures behave deterministically.

Milestone 4: Leases + crash recovery (C)

- Deliver: reserve returns lease token + expiry
- Deliver: extend lease API (heartbeat)
- Deliver: reclaim expired leases (job becomes runnable again)
- Tests: reclaim works, no double processing during valid lease
- Done when: crash simulation is safe.

Milestone 5: Postgres driver v0 (C)

- Deliver: schema + migrations
- Deliver: enqueue, schedule, reserve with lease, ack, fail, DLQ
- Tests: integration tests using Postgres (local docker and CI)
- Done when: same core demo works on Postgres.

Milestone 6: Postgres hardening

- Deliver: idempotency keys via unique constraint behavior
- Deliver: indexes for runnable queries + lease expiry
- Deliver: safe, idempotent ack/fail operations
- Tests: dedupe, lease reclaim, concurrency reserve
- Done when: correctness and basic performance are acceptable.

Milestone 7: Conformance test suite

- Deliver: conformance package with reusable tests
- Deliver: memory and Postgres drivers pass conformance
- Done when: a new driver can be validated by running the suite.

Milestone 8: CLI v0

- Deliver: worker run, enqueue, list, inspect
- Deliver: dlq list, dlq requeue, job retry
- Done when: full local dev flow can be done via CLI.

Milestone 9: Workflows v0 (Chain)

- Deliver: flow.Chain builder
- Deliver: durable workflow state model (run + nodes)
- Deliver: crash-safe chain progression
- Tests: chain continues after worker restart
- Done when: chain is reliable on Postgres.

Milestone 10: Workflows v1 (Group + Chord)

- Deliver: flow.Group fanout
- Deliver: flow.Chord join + finalizer exactly-once scheduling
- Tests: finalizer runs once, chord waits through retries
- Done when: fanout/join works in real example (CSV import or thumbnails).

Milestone 11: Observability hooks

- Deliver: logging interface + default logger
- Deliver: metrics hooks (optional Prometheus adapter package)
- Deliver: tracing hooks (optional OpenTelemetry adapter package)
- Done when: users can plug in observability without patching core.

Milestone 12: Examples + release v0.1.0

- Deliver: 3 examples (email, CSV import workflow, thumbnail fanout)
- Deliver: docs/quickstart.md, docs/workflows.md, docs/reliability.md, docs/postgres.md
- Deliver: CHANGELOG.md, tag v0.1.0
- Done when: new user can run Postgres worker, enqueue jobs, see retries/DLQ, run a workflow example.

## Post v0.1.0 backlog

- Redis driver or adapter driver
- Periodic jobs
- Priority queues
- Rate limiting per queue or tenant
- Web UI dashboard (optional)
