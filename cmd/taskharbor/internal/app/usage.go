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

Notes:
  This will be implemented in issue #113.`)
}

func printEnqueueUsage(w io.Writer) {
	fmt.Fprintln(w, `Usage:
  taskharbor [global flags] enqueue [flags]

Notes:
  This will be implemented in issue #114.`)
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
