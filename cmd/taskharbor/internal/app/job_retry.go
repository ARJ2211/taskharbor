package app

import (
	"flag"
	"fmt"
	"io"
	"strings"
)

func runJobRetry(g GlobalFlags, argv []string, stdout, stderr io.Writer) int {
	// Alias to: dlq requeue <id>
	// Supports: --queue, --run-at, --reset-attempts
	var (
		help  bool
		h     bool
		queue string
		runAt string
		reset bool
	)

	fs := flag.NewFlagSet("taskharbor job retry", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	fs.StringVar(&queue, "queue", "", "queue guard (defaults to global --queue)")
	fs.StringVar(&runAt, "run-at", "", "schedule time (RFC3339/RFC3339Nano) or unix seconds (or unix ms)")
	fs.BoolVar(&reset, "reset-attempts", false, "reset attempts/last_error/failed_at")

	fs.BoolVar(&help, "help", false, "show help")
	fs.BoolVar(&h, "h", false, "show help")

	if err := fs.Parse(argv); err != nil {
		fmt.Fprintln(stderr, "error:", err)
		printJobRetryUsage(stderr)
		return 2
	}
	if help || h {
		printJobRetryUsage(stdout)
		return 0
	}

	args := fs.Args()
	if len(args) != 1 {
		fmt.Fprintln(stderr, "error: job retry requires exactly 1 arg: <job_id>")
		printJobRetryUsage(stderr)
		return 2
	}
	id := strings.TrimSpace(args[0])
	if id == "" {
		fmt.Fprintln(stderr, "error: job_id cannot be empty")
		return 2
	}

	// Reuse dlq requeue implementation
	requeueArgs := []string{id}
	if queue != "" {
		requeueArgs = append([]string{"--queue", queue}, requeueArgs...)
	}
	if runAt != "" {
		requeueArgs = append([]string{"--run-at", runAt}, requeueArgs...)
	}
	if reset {
		requeueArgs = append([]string{"--reset-attempts"}, requeueArgs...)
	}

	return runDLQRequeue(g, requeueArgs, stdout, stderr)
}
