# TaskHarbor CLI (v0)

This document explains the TaskHarbor CLI added in **Milestone 8**. It is meant for local development and quick sanity checks across drivers. The CLI is intentionally simple:

- It uses the same core **Client/Worker APIs** your application would use.
- It connects to a driver backend (`memory`, `postgres`, `redis`).
- It relies on the optional `driver.Admin` interface for list/inspect/DLQ operations.

---

## How to Run It

From the repo root:

**1. Run directly with `go run`:**

```bash
go run ./cmd/taskharbor --help
```

**2. Or install the binary locally:**

```bash
go install ./cmd/taskharbor
taskharbor --help
```

---

## Global Flags and Environment Variables

Global flags apply **before** the command name.

| Flag           | Values / Example              | Default   | Description                                     |
| -------------- | ----------------------------- | --------- | ----------------------------------------------- |
| `--driver`     | `memory`, `postgres`, `redis` | `memory`  | Backend driver to connect to                    |
| `--queue`      | any string                    | `default` | Queue name                                      |
| `--json`       | —                             | —         | Print JSON output (useful for scripting)        |
| `--verbose`    | —                             | —         | Extra logs (currently minimal)                  |
| `--dsn`        | `postgres://...`              | —         | Postgres DSN (required for `--driver postgres`) |
| `--redis-addr` | `host:port`                   | —         | Redis address (required for `--driver redis`)   |

**Environment variables (used as defaults):**

| Variable                         | Purpose        |
| -------------------------------- | -------------- |
| `TH_DRIVER`                      | Default driver |
| `TH_PG_DSN` or `TH_POSTGRES_DSN` | Postgres DSN   |
| `TH_REDIS_ADDR`                  | Redis address  |

**Examples:**

```bash
TH_DRIVER=postgres TH_PG_DSN='postgres://...' go run ./cmd/taskharbor list
TH_DRIVER=redis TH_REDIS_ADDR='localhost:6379' go run ./cmd/taskharbor list
```

---

## Important Note About the Memory Driver

The memory driver lives only inside a **single process**. That means:

- If you run `worker run` in terminal 1 and `enqueue` in terminal 2 using `--driver memory`, they will **not** see each other.
- For multi-terminal testing, use **Postgres** or **Redis**.

The memory driver is still useful for:

- Unit tests
- Quick single-process experiments
- Debugging worker behavior without external services

---

## Command Overview

| Command            | Description                                    |
| ------------------ | ---------------------------------------------- |
| `worker run`       | Start a long-running worker process            |
| `enqueue`          | Add a job to the queue                         |
| `list`             | List jobs for a queue                          |
| `inspect <id>`     | Show a single job record and its state         |
| `dlq list`         | List dead-letter queue jobs                    |
| `dlq requeue <id>` | Move a DLQ job back into the runnable pipeline |
| `job retry <id>`   | Alias for `dlq requeue`                        |

Most commands open a connection to the driver, do one thing, print output, and exit. `worker run` is the only long-running command.

---

## `worker run`

Starts a worker process and registers a small set of built-in handlers so you can test the pipeline without writing app code.

**Built-in handlers:**

| Handler | Behavior                                                                         |
| ------- | -------------------------------------------------------------------------------- |
| `echo`  | Prints basic job info + payload preview, returns success                         |
| `fail`  | Always returns an error — useful to force retries and DLQ                        |
| `sleep` | Sleeps for a duration derived from the payload, returns success unless cancelled |

**Basic usage:**

```bash
taskharbor worker run
```

**Common flags:**

| Flag                   | Example | Description                                  |
| ---------------------- | ------- | -------------------------------------------- |
| `--concurrency`        | `5`     | Max concurrent jobs                          |
| `--poll-interval`      | `200ms` | How often to poll when no jobs are available |
| `--lease-duration`     | `30s`   | Visibility timeout / lease duration          |
| `--heartbeat-interval` | `10s`   | How often to extend the lease                |

**Handler mapping:**

`--register` is repeatable and maps `jobType=builtin`:

```bash
taskharbor worker run --register email=echo --register slow=sleep
```

This lets you enqueue jobs with type `email` or `slow` and have them routed to the built-ins.

**Sleep payload formats:**

```bash
# Plain integer milliseconds
--payload 1500

# JSON number
--payload-json 1500

# JSON object with ms field
--payload-json '{"ms":1500}'

# JSON object with duration string
--payload-json '{"duration":"1.5s"}'
```

> If no valid duration is found, `sleep` defaults to **250ms**.

**Shutdown:**

`Ctrl+C` triggers graceful shutdown by cancelling the worker context.

---

## `enqueue`

Adds a job to the chosen backend.

**Usage:**

```bash
taskharbor enqueue --type <jobType> [flags]
```

**Required:**

| Flag     | Description         |
| -------- | ------------------- |
| `--type` | Job type identifier |

**Optional:**

| Flag                | Example                | Description                                                     |
| ------------------- | ---------------------- | --------------------------------------------------------------- |
| `--queue`           | `my-queue`             | Overrides the global `--queue`                                  |
| `--run-at`          | `2025-01-01T00:00:00Z` | Schedule a job (RFC3339, RFC3339Nano, unix seconds, or unix ms) |
| `--timeout`         | `30s`                  | Per-job timeout                                                 |
| `--max-attempts`    | `3`                    | Attempts before DLQ (`0` = fail immediately on handler error)   |
| `--idempotency-key` | `my-key`               | Deduplication key (driver-enforced)                             |

**Payload options:**

| Flag                    | Description                                         |
| ----------------------- | --------------------------------------------------- |
| `--payload <string>`    | String payload — JSON-encoded internally            |
| `--payload-json <json>` | Raw JSON payload — stored as-is, not double-encoded |

> Use one or the other, not both.
>
> - `--payload "hello"` → stored as JSON string `"hello"`
> - `--payload-json '{"x":1}'` → stored as the raw JSON object bytes

**Output:**

- Default: prints the job `id` on stdout
- With `--json`: prints a JSON object including the `id`

---

## `list`

Lists jobs for a queue. Requires the `driver.Admin` interface, which `memory`, `postgres`, and `redis` all implement.

**Usage:**

```bash
taskharbor list [flags]
```

**Flags:**

| Flag       | Description                                               | Default          |
| ---------- | --------------------------------------------------------- | ---------------- |
| `--queue`  | Queue name                                                | global `--queue` |
| `--status` | `ready`, `scheduled`, `inflight`, `dlq`, `done`, or `all` | —                |
| `--limit`  | Number of jobs to print                                   | `20`             |
| `--cursor` | Pagination cursor from a previous call                    | —                |

**Output:**

- Default: a small tabular view
- With `--json`: `jobs` array + `next_cursor`

**Pagination:**

If there are more results than `--limit`, `list` returns a `next_cursor`. Pass it back with `--cursor` to fetch the next page:

```bash
taskharbor list --limit 5
taskharbor list --limit 5 --cursor <next_cursor>
```

---

## `inspect`

Shows a single job record and its current state.

**Usage:**

```bash
taskharbor inspect <job-id>
```

**Printed fields:**

| Field                             | Notes                                              |
| --------------------------------- | -------------------------------------------------- |
| `job id`, `type`, `queue`         | Identity                                           |
| `state`                           | `ready`, `scheduled`, `inflight`, `dlq`, or `done` |
| `created_at`, `run_at`, `timeout` | Timing                                             |
| `attempts`, `max_attempts`        | Retry tracking                                     |
| `last_error`, `failed_at`         | When applicable                                    |
| `lease token` + `expiry`          | Only when inflight                                 |
| `dlq reason` + `dlq failed time`  | Only when in DLQ                                   |
| `payload preview`                 | Truncated payload                                  |

> **If you see `job not found`:**
>
> - Confirm you used a **job id** (long hex from `enqueue`), not a worker id (short, printed by `worker run`).
> - Confirm you are using the **same backend** (`postgres`/`redis`) as the process that enqueued the job.

---

## `dlq list`

Lists DLQ jobs for a queue. This is essentially `list` filtered to `--status dlq`.

**Usage:**

```bash
taskharbor dlq list [flags]
```

**Flags:**

| Flag       | Description       | Default          |
| ---------- | ----------------- | ---------------- |
| `--queue`  | Queue name        | global `--queue` |
| `--limit`  | Page size         | `20`             |
| `--cursor` | Pagination cursor | —                |

---

## `dlq requeue`

Moves a job from the DLQ back into the runnable pipeline.

**Usage:**

```bash
taskharbor dlq requeue <job-id> [flags]
```

**Flags:**

| Flag               | Description                                               |
| ------------------ | --------------------------------------------------------- |
| `--queue`          | Queue guard — errors if the job is in a different queue   |
| `--run-at`         | Optionally reschedule the job (same formats as `enqueue`) |
| `--reset-attempts` | Resets `attempts`, `last_error`, and `failed_at`          |

**Typical usage:**

```bash
taskharbor dlq requeue <job-id> --reset-attempts
```

---

## `job retry`

An alias for `dlq requeue`. It exists because users often think in terms of retrying a failed job rather than requeuing from DLQ.

**Usage:**

```bash
taskharbor job retry <job-id> [same flags as dlq requeue]
```

---

## Practical Test Recipes

These are copy-paste friendly flows you can run locally. Everything below assumes you are in the repo root.

---

### Postgres: Quick Start

**Terminal 1 — start the worker:**

```bash
export TH_PG_DSN='postgres://taskharbor:taskharbor@localhost:5432/taskharbor_test?sslmode=disable'
go run ./cmd/taskharbor --driver postgres --dsn "$TH_PG_DSN" worker run
```

**Terminal 2 — enqueue and inspect:**

```bash
export TH_PG_DSN='postgres://taskharbor:taskharbor@localhost:5432/taskharbor_test?sslmode=disable'

JOB_ID=$(go run ./cmd/taskharbor --driver postgres --dsn "$TH_PG_DSN" enqueue --type echo --payload hello)
echo "job_id=$JOB_ID"

go run ./cmd/taskharbor --driver postgres --dsn "$TH_PG_DSN" list
go run ./cmd/taskharbor --driver postgres --dsn "$TH_PG_DSN" inspect "$JOB_ID"
```

---

### Redis: Quick Start

**Terminal 1 — start the worker:**

```bash
export REDIS_ADDR='localhost:6379'
go run ./cmd/taskharbor --driver redis --redis-addr "$REDIS_ADDR" worker run
```

**Terminal 2 — enqueue and inspect:**

```bash
export REDIS_ADDR='localhost:6379'

JOB_ID=$(go run ./cmd/taskharbor --driver redis --redis-addr "$REDIS_ADDR" enqueue --type echo --payload hello)
echo "job_id=$JOB_ID"

go run ./cmd/taskharbor --driver redis --redis-addr "$REDIS_ADDR" list
go run ./cmd/taskharbor --driver redis --redis-addr "$REDIS_ADDR" inspect "$JOB_ID"
```

---

### Scheduling Test (`--run-at`)

```bash
export TH_PG_DSN='postgres://taskharbor:taskharbor@localhost:5432/taskharbor_test?sslmode=disable'

# Schedule a job 10 seconds in the future
RUN_AT=$(date -u -v+10S +%Y-%m-%dT%H:%M:%SZ)
JOB_ID=$(go run ./cmd/taskharbor --driver postgres --dsn "$TH_PG_DSN" \
  enqueue --type echo --run-at "$RUN_AT" --payload "scheduled hello")

echo "scheduled job_id=$JOB_ID  run_at=$RUN_AT"

# Confirm it is scheduled
go run ./cmd/taskharbor --driver postgres --dsn "$TH_PG_DSN" list --status scheduled
go run ./cmd/taskharbor --driver postgres --dsn "$TH_PG_DSN" inspect "$JOB_ID"
```

After 10+ seconds:

```bash
# Confirm it moved to ready / done
go run ./cmd/taskharbor --driver postgres --dsn "$TH_PG_DSN" list --status ready
go run ./cmd/taskharbor --driver postgres --dsn "$TH_PG_DSN" list --status done
```

---

### Fail → DLQ → Requeue

> Requires the `fail` handler, which is registered by `worker run` by default.

```bash
export TH_PG_DSN='postgres://taskharbor:taskharbor@localhost:5432/taskharbor_test?sslmode=disable'

# Enqueue a job that will always fail, with max 1 attempt
JOB_ID=$(go run ./cmd/taskharbor --driver postgres --dsn "$TH_PG_DSN" \
  enqueue --type fail --max-attempts 1 --payload boom)
echo "dlq candidate job_id=$JOB_ID"

# Inspect the DLQ
go run ./cmd/taskharbor --driver postgres --dsn "$TH_PG_DSN" dlq list
go run ./cmd/taskharbor --driver postgres --dsn "$TH_PG_DSN" inspect "$JOB_ID"
```

Requeue and reset attempts:

```bash
go run ./cmd/taskharbor --driver postgres --dsn "$TH_PG_DSN" dlq requeue "$JOB_ID" --reset-attempts
```

Confirm it is runnable again:

```bash
go run ./cmd/taskharbor --driver postgres --dsn "$TH_PG_DSN" list
go run ./cmd/taskharbor --driver postgres --dsn "$TH_PG_DSN" inspect "$JOB_ID"
```

---

### Sleep Handler Test

```bash
export TH_PG_DSN='postgres://taskharbor:taskharbor@localhost:5432/taskharbor_test?sslmode=disable'

JOB_ID=$(go run ./cmd/taskharbor --driver postgres --dsn "$TH_PG_DSN" \
  enqueue --type sleep --payload 1500)
echo "sleep job_id=$JOB_ID"

go run ./cmd/taskharbor --driver postgres --dsn "$TH_PG_DSN" inspect "$JOB_ID"
```

---

### JSON Output for Scripting

```bash
export TH_PG_DSN='postgres://taskharbor:taskharbor@localhost:5432/taskharbor_test?sslmode=disable'

go run ./cmd/taskharbor --driver postgres --dsn "$TH_PG_DSN" --json list
go run ./cmd/taskharbor --driver postgres --dsn "$TH_PG_DSN" --json inspect <job-id>
```

---

### Pagination Demo

Enqueue 12 jobs:

```bash
export TH_PG_DSN='postgres://taskharbor:taskharbor@localhost:5432/taskharbor_test?sslmode=disable'

for i in $(seq 1 12); do
  go run ./cmd/taskharbor --driver postgres --dsn "$TH_PG_DSN" \
    enqueue --type echo --payload "msg $i" >/dev/null
done
```

Fetch the first page:

```bash
go run ./cmd/taskharbor --driver postgres --dsn "$TH_PG_DSN" list --limit 5
```

Copy the `next_cursor` from the output and fetch the next page:

```bash
go run ./cmd/taskharbor --driver postgres --dsn "$TH_PG_DSN" list --limit 5 --cursor <next_cursor>
```

---

## Common Mistakes and Troubleshooting

**1. `inspect` says `job not found`**

- You may have passed a **worker id**. `worker run` prints a short worker id; `enqueue` prints the long hex **job id**.
- You may be using the `memory` driver across different terminal processes. Switch to `postgres` or `redis`.

**2. `list` is empty but `enqueue` returned an id**

- With the `memory` driver, the id exists only inside that command's process.
- With `postgres`/`redis`, verify the DSN/address is **identical** across all commands.

**3. `driver does not support admin operations`**

- `list`, `inspect`, and `dlq` require the `driver.Admin` interface.
- `memory`, `postgres`, and `redis` all implement it. A future driver might not.

**4. Postgres/Redis connection errors**

- Confirm the service is running.
- Verify your DSN or `host:port`.
- Check firewall rules or Docker port mappings.

---
