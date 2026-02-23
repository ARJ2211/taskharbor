package app

import (
	"fmt"
	"io"
)

func printRootUsage(w io.Writer) {
	fmt.Fprintln(w, `TaskHarbor CLI

Usage:
  taskharbor [global flags] <command> [args]

Global flags:
  --driver   memory|postgres|redis   (default: memory)
  --queue    queue name             (default: default)
  --json     JSON output
  --verbose  verbose logs
  --help, -h show help
  --dsn         postgres DSN (for --driver postgres)
  --redis-addr  redis addr host:port (for --driver redis)

Commands:
  worker run
  enqueue
  list
  inspect <job_id>
  dlq list
  dlq requeue <job_id>
  job retry <job_id>

Examples:
  taskharbor --help
  taskharbor worker run --help
  taskharbor enqueue --help`)
}

func printWorkerUsage(w io.Writer) {
	fmt.Fprintln(w, `Usage:
  taskharbor [global flags] worker <subcommand>

Subcommands:
  run

Example:
  taskharbor worker run --help`)
}

func printWorkerRunUsage(w io.Writer) {
	fmt.Fprintln(w, `Usage:
  taskharbor [global flags] worker run [flags]

Flags:
  --concurrency         max concurrent jobs
  --poll-interval       e.g. 200ms
  --lease-duration      e.g. 30s
  --heartbeat-interval  e.g. 10s
  --register            repeatable mapping: jobType=builtin (builtins: echo, fail, sleep)

Examples:
  taskharbor worker run
  taskharbor worker run --concurrency 8
  taskharbor worker run --register email=echo --register slow=sleep`)
}

func printEnqueueUsage(w io.Writer) {
	fmt.Fprintln(w, `Usage:
  taskharbor [global flags] enqueue [flags]

Flags:
  --type              required job type
  --queue             queue name (default: global --queue or "default")
  --run-at            RFC3339/RFC3339Nano or unix seconds (or unix ms)
  --timeout           e.g. 30s
  --max-attempts      retries before DLQ (0 means fail immediately on handler error)
  --idempotency-key   idempotency key
  --payload           string payload (JSON-encoded)
  --payload-json      raw JSON payload (not double-encoded)

Examples:
  taskharbor enqueue --type echo --payload hello
  taskharbor enqueue --type echo --payload-json {"msg":"hi"}
  taskharbor enqueue --type echo --run-at 1772000000 --payload hi
  taskharbor --driver postgres --dsn $TH_PG_DSN enqueue --type echo --idempotency-key user:123 --payload hi`)
}

func printListUsage(w io.Writer) {
	fmt.Fprintln(w, `Usage:
  taskharbor [global flags] list [flags]

Notes:
  This will be implemented in issue #119.`)
}

func printInspectUsage(w io.Writer) {
	fmt.Fprintln(w, `Usage:
  taskharbor [global flags] inspect <job_id> [flags]

Notes:
  This will be implemented in issue #119.`)
}

func printDLQUsage(w io.Writer) {
	fmt.Fprintln(w, `Usage:
  taskharbor [global flags] dlq <subcommand>

Subcommands:
  list
  requeue <job_id>

Examples:
  taskharbor dlq list --help
  taskharbor dlq requeue <job_id>`)
}

func printDLQListUsage(w io.Writer) {
	fmt.Fprintln(w, `Usage:
  taskharbor [global flags] dlq list [flags]

Notes:
  This will be implemented in issue #119.`)
}

func printDLQRequeueUsage(w io.Writer) {
	fmt.Fprintln(w, `Usage:
  taskharbor [global flags] dlq requeue <job_id> [flags]

Notes:
  This will be implemented in issue #119.`)
}

func printJobUsage(w io.Writer) {
	fmt.Fprintln(w, `Usage:
  taskharbor [global flags] job <subcommand>

Subcommands:
  retry <job_id>

Example:
  taskharbor job retry <job_id>`)
}

func printJobRetryUsage(w io.Writer) {
	fmt.Fprintln(w, `Usage:
  taskharbor [global flags] job retry <job_id> [flags]

Notes:
  This will be implemented in issue #119.`)
}
