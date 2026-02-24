package app

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	drv "github.com/ARJ2211/taskharbor/taskharbor/driver"
)

func runInspect(g GlobalFlags, argv []string, stdout, stderr io.Writer) int {
	var help bool
	var h bool

	fs := flag.NewFlagSet("taskharbor inspect", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.BoolVar(&help, "help", false, "show help")
	fs.BoolVar(&h, "h", false, "show help")

	if err := fs.Parse(argv); err != nil {
		fmt.Fprintln(stderr, "error:", err)
		printInspectUsage(stderr)
		return 2
	}
	if help || h {
		printInspectUsage(stdout)
		return 0
	}

	args := fs.Args()
	if len(args) != 1 {
		fmt.Fprintln(stderr, "error: inspect requires exactly 1 arg: <job_id>")
		printInspectUsage(stderr)
		return 2
	}
	id := strings.TrimSpace(args[0])
	if id == "" {
		fmt.Fprintln(stderr, "error: job_id cannot be empty")
		return 2
	}

	ctx := context.Background()
	ah, err := openAdmin(ctx, g)
	if err != nil {
		fmt.Fprintln(stderr, "error:", err)
		return 1
	}
	defer func() { _ = ah.Close() }()

	info, err := ah.Admin.Inspect(ctx, id, time.Now().UTC())
	if err != nil {
		fmt.Fprintln(stderr, "error:", err)
		return 1
	}

	if g.JSON {
		enc := json.NewEncoder(stdout)
		enc.SetEscapeHTML(false)
		_ = enc.Encode(info)
		return 0
	}

	printJobInfo(stdout, info)
	return 0
}

func printJobInfo(w io.Writer, info drv.JobInfo) {
	r := info.Record

	fmt.Fprintln(w, "id:", r.ID)
	fmt.Fprintln(w, "type:", r.Type)
	fmt.Fprintln(w, "queue:", r.Queue)
	fmt.Fprintln(w, "state:", info.State)

	if !r.CreatedAt.IsZero() {
		fmt.Fprintln(w, "created_at:", r.CreatedAt.UTC().Format(time.RFC3339Nano))
	}
	if !r.RunAt.IsZero() {
		fmt.Fprintln(w, "run_at:", r.RunAt.UTC().Format(time.RFC3339Nano))
	}
	if r.Timeout > 0 {
		fmt.Fprintln(w, "timeout:", r.Timeout.String())
	}
	fmt.Fprintln(w, "attempts:", r.Attempts)
	fmt.Fprintln(w, "max_attempts:", r.MaxAttempts)

	if r.LastError != "" {
		fmt.Fprintln(w, "last_error:", r.LastError)
	}
	if !r.FailedAt.IsZero() {
		fmt.Fprintln(w, "failed_at:", r.FailedAt.UTC().Format(time.RFC3339Nano))
	}

	if info.Lease != nil {
		fmt.Fprintln(w, "lease_token:", string(info.Lease.Token))
		if !info.Lease.ExpiresAt.IsZero() {
			fmt.Fprintln(w, "lease_expires_at:", info.Lease.ExpiresAt.UTC().Format(time.RFC3339Nano))
		}
	}

	if info.DLQ != nil {
		if info.DLQ.Reason != "" {
			fmt.Fprintln(w, "dlq_reason:", info.DLQ.Reason)
		}
		if !info.DLQ.FailedAt.IsZero() {
			fmt.Fprintln(w, "dlq_failed_at:", info.DLQ.FailedAt.UTC().Format(time.RFC3339Nano))
		}
	}

	if len(r.Payload) > 0 {
		fmt.Fprintln(w, "payload_bytes:", len(r.Payload))
		fmt.Fprintln(w, "payload_preview:", string(r.Payload))
	}
}
