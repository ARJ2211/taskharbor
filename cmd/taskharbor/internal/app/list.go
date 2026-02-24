package app

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"

	drv "github.com/ARJ2211/taskharbor/taskharbor/driver"
)

func runList(g GlobalFlags, argv []string, stdout, stderr io.Writer) int {
	var (
		help   bool
		h      bool
		queue  string
		state  string
		limit  int
		cursor string
	)

	fs := flag.NewFlagSet("taskharbor list", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	fs.StringVar(&queue, "queue", "", "queue name (defaults to global --queue)")
	fs.StringVar(&state, "status", "", "ready|scheduled|inflight|dlq|done|all")
	fs.IntVar(&limit, "limit", 20, "page size")
	fs.StringVar(&cursor, "cursor", "", "pagination cursor")

	fs.BoolVar(&help, "help", false, "show help")
	fs.BoolVar(&h, "h", false, "show help")

	if err := fs.Parse(argv); err != nil {
		fmt.Fprintln(stderr, "error:", err)
		printListUsage(stderr)
		return 2
	}
	if help || h {
		printListUsage(stdout)
		return 0
	}
	if len(fs.Args()) != 0 {
		fmt.Fprintln(stderr, "error: unexpected args:", strings.Join(fs.Args(), " "))
		printListUsage(stderr)
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

	st, err := parseState(state)
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

	page, err := ah.Admin.List(ctx, drv.ListRequest{
		Queue:  queue,
		State:  st,
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
			"status":      string(st),
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

func parseState(s string) (drv.JobState, error) {
	s = strings.TrimSpace(strings.ToLower(s))
	if s == "" || s == "all" {
		return "", nil
	}
	switch s {
	case "ready":
		return drv.StateReady, nil
	case "scheduled":
		return drv.StateScheduled, nil
	case "inflight":
		return drv.StateInflight, nil
	case "dlq":
		return drv.StateDLQ, nil
	case "done":
		return drv.StateDone, nil
	default:
		return "", fmt.Errorf("invalid --status %q (expected ready|scheduled|inflight|dlq|done|all)", s)
	}
}

func printJobSummaryTable(w io.Writer, jobs []drv.JobSummary) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tTYPE\tSTATE\tATTEMPTS\tRUN_AT\tLEASE_EXPIRES\tDLQ_REASON")
	for _, j := range jobs {
		runAt := "-"
		if !j.RunAt.IsZero() {
			runAt = j.RunAt.UTC().Format(time.RFC3339)
		}
		lease := "-"
		if !j.LeaseExpiresAt.IsZero() {
			lease = j.LeaseExpiresAt.UTC().Format(time.RFC3339)
		}
		at := fmt.Sprintf("%d/%d", j.Attempts, j.MaxAttempts)
		reason := j.DLQReason
		if reason == "" {
			reason = "-"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			j.ID, j.Type, j.State, at, runAt, lease, reason,
		)
	}
	_ = tw.Flush()
}
