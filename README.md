# TaskHarbor

## TaskHarbor is a Go background jobs and simple workflows framework with a strict semantics contract and pluggable drivers.

What TaskHarbor is focusing on
- Stable core API: define handlers, enqueue jobs, run workers, without coupling your app to a specific backend.
- Pluggable drivers: start with in-memory for tests/local, Postgres for production; more drivers can be added later.
- Consistent semantics across drivers: retries, backoff, scheduling (run_at), leases/visibility timeouts, idempotency keys, and DLQ behavior are part of the contract.
- Driver conformance suite: every driver must pass the same behavior tests so semantics don’t drift.
- Durable workflow primitives: chain, group, chord with crash-safe resumption (especially with the Postgres driver).
- Go-native execution model: context cancellation, middleware hooks, predictable failure handling, minimal magic.
- Operator-friendly: CLI-first dev and ops flow (inspect, retry, DLQ requeue) with optional observability hooks later.

## How TaskHarbor differs from existing options

| Project | Primary model | Backends | Workflow story | What TaskHarbor does differently |
|--------|---------------|----------|----------------|----------------------------------|
| TaskHarbor (this) | Framework + semantics contract | Memory + Postgres first (drivers) | Chain, Group, Chord designed to be durable + crash-safe | Stable API + driver contract + conformance tests to keep semantics consistent across drivers |
| Asynq | Redis-first task queue | Redis | Scheduling + retries; workflows are not the core product | TaskHarbor targets backend portability + workflow durability with a consistent contract |
| taskq | Job queue library with multiple backends | Redis, SQS, IronMQ, memory | Some composition patterns exist, but not a durable workflow contract | TaskHarbor’s focus is strict semantics + conformance + durable chain/group/chord |
| River | Postgres-first job system | Postgres | Primarily jobs; workflow layer is not the main abstraction | TaskHarbor aims for a standard API that can support multiple drivers, not just Postgres |
| Faktory | Separate job server + language clients | Faktory server | Server-centric orchestration | TaskHarbor is a Go-native framework with drivers and workflow primitives, not a standalone job server |
| Neoq | Queue-agnostic job library | Multiple (queue adapters) | Jobs-first; workflows not the main pitch | TaskHarbor emphasizes a strict semantics contract + conformance suite + durable workflows |
| Temporal | Durable workflow platform | Temporal service | Best-in-class workflows (signals/timers/history) | Different category: TaskHarbor is intentionally smaller and simpler than Temporal |

Notes
- TaskHarbor is at-least-once by design. Handlers must be idempotent.
- The differentiator is not “another queue”. It’s consistent semantics across drivers plus durable chain/group/chord workflows backed by conformance tests.


Non-goals (by design)
- Not a Temporal-style workflow engine.
- Not trying to out-feature every existing queue library.
- Not shipping every backend on day one.

Status
- Early development. See ROADMAP.md for milestones and guarantees.

