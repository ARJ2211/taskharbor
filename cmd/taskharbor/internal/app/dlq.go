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

func runDLQList(g GlobalFlags, argv []string, stdout, stderr io.Writer) int {
	var (
		help   bool
		h      bool
		queue  string
		limit  int
		cursor string
	)

	fs := flag.NewFlagSet("taskharbor dlq list", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	fs.StringVar(&queue, "queue", "", "queue name (defaults to global --queue)")
	fs.IntVar(&limit, "limit", 20, "page size")
	fs.StringVar(&cursor, "cursor", "", "pagination cursor")

	fs.BoolVar(&help, "help", false, "show help")
	fs.BoolVar(&h, "h", false, "show help")

	if err := fs.Parse(argv); err != nil {
		fmt.Fprintln(stderr, "error:", err)
		printDLQListUsage(stderr)
		return 2
	}
	if help || h {
		printDLQListUsage(stdout)
		return 0
	}
	if len(fs.Args()) != 0 {
		fmt.Fprintln(stderr, "error: unexpected args:", strings.Join(fs.Args(), " "))
		printDLQListUsage(stderr)
		return 2
	}

	if strings.TrimSpace(queue) == "" {
		queue = g.Queue
	}
	queue = strings.TrimSpace(queue)
	if queue == "" {
		fmt.Fprintln(stderr, "error: queue is required (use --queue or global --queue)")
		return 2
	}

	ctx := context.Background()
	ah, err := openAdmin(ctx, g)
	if err != nil {
		fmt.Fprintln(stderr, "error:", err)
		return 1
	}
	defer func() { _ = ah.Close() }()

	page, err := ah.Admin.List(ctx, drv.ListRequest{
		Queue:  queue,
		State:  drv.StateDLQ,
		Now:    time.Now().UTC(),
		Limit:  limit,
		Cursor: cursor,
	})
	if err != nil {
		fmt.Fprintln(stderr, "error:", err)
		return 1
	}

	if g.JSON {
		enc := json.NewEncoder(stdout)
		enc.SetEscapeHTML(false)
		_ = enc.Encode(map[string]any{
			"queue":       queue,
			"status":      "dlq",
			"jobs":        page.Jobs,
			"next_cursor": page.NextCursor,
		})
		return 0
	}

	printJobSummaryTable(stdout, page.Jobs)
	if page.NextCursor != "" {
		fmt.Fprintln(stdout)
		fmt.Fprintln(stdout, "next_cursor:", page.NextCursor)
	}
	return 0
}

func runDLQRequeue(g GlobalFlags, argv []string, stdout, stderr io.Writer) int {
	var (
		help  bool
		h     bool
		queue string
		runAt string
		reset bool
	)

	fs := flag.NewFlagSet("taskharbor dlq requeue", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	fs.StringVar(&queue, "queue", "", "queue guard (defaults to global --queue)")
	fs.StringVar(&runAt, "run-at", "", "schedule time (RFC3339/RFC3339Nano) or unix seconds (or unix ms)")
	fs.BoolVar(&reset, "reset-attempts", false, "reset attempts/last_error/failed_at")

	fs.BoolVar(&help, "help", false, "show help")
	fs.BoolVar(&h, "h", false, "show help")

	if err := fs.Parse(argv); err != nil {
		fmt.Fprintln(stderr, "error:", err)
		printDLQRequeueUsage(stderr)
		return 2
	}
	if help || h {
		printDLQRequeueUsage(stdout)
		return 0
	}

	args := fs.Args()
	if len(args) != 1 {
		fmt.Fprintln(stderr, "error: dlq requeue requires exactly 1 arg: <job_id>")
		printDLQRequeueUsage(stderr)
		return 2
	}
	id := strings.TrimSpace(args[0])
	if id == "" {
		fmt.Fprintln(stderr, "error: job_id cannot be empty")
		return 2
	}

	if strings.TrimSpace(queue) == "" {
		queue = g.Queue
	}
	queue = strings.TrimSpace(queue)

	runAtTime, err := parseRunAt(runAt)
	if err != nil {
		fmt.Fprintln(stderr, "error:", err)
		return 2
	}

	ctx := context.Background()
	ah, err := openAdmin(ctx, g)
	if err != nil {
		fmt.Fprintln(stderr, "error:", err)
		return 1
	}
	defer func() { _ = ah.Close() }()

	err = ah.Admin.RequeueDLQ(ctx, id, time.Now().UTC(), drv.RequeueOptions{
		Queue:         queue,
		RunAt:         runAtTime,
		ResetAttempts: reset,
	})
	if err != nil {
		fmt.Fprintln(stderr, "error:", err)
		return 1
	}

	if g.JSON {
		enc := json.NewEncoder(stdout)
		enc.SetEscapeHTML(false)
		_ = enc.Encode(map[string]any{"ok": true, "id": id})
		return 0
	}

	fmt.Fprintln(stdout, "ok:", id)
	return 0
}
