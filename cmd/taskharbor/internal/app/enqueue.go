package app

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/ARJ2211/taskharbor/cmd/taskharbor/internal/backend"
	th "github.com/ARJ2211/taskharbor/taskharbor"
)

func runEnqueue(g GlobalFlags, argv []string, stdout, stderr io.Writer) int {
	var (
		help bool
		h    bool

		jobType string
		queue   string
		runAt   string

		timeout     time.Duration
		maxAttempts int
		idKey       string

		payloadStr  string
		payloadJSON string
	)

	fs := flag.NewFlagSet("taskharbor enqueue", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	fs.StringVar(&jobType, "type", "", "job type (required)")
	fs.StringVar(&queue, "queue", "", "queue name (default: global --queue or DefaultQueue)")
	fs.StringVar(&runAt, "run-at", "", "schedule time (RFC3339/RFC3339Nano) or unix seconds (or unix ms)")
	fs.DurationVar(&timeout, "timeout", 0, "job timeout (e.g. 30s)")
	fs.IntVar(&maxAttempts, "max-attempts", 0, "max attempts before DLQ (0 means fail immediately on handler error)")
	fs.StringVar(&idKey, "idempotency-key", "", "idempotency key")

	fs.StringVar(&payloadStr, "payload", "", "payload as string (will be JSON-encoded)")
	fs.StringVar(&payloadJSON, "payload-json", "", "payload as raw JSON (not double-encoded)")

	fs.BoolVar(&help, "help", false, "show help")
	fs.BoolVar(&h, "h", false, "show help")

	if err := fs.Parse(argv); err != nil {
		fmt.Fprintln(stderr, "error:", err)
		printEnqueueUsage(stderr)
		return 2
	}
	if help || h {
		printEnqueueUsage(stdout)
		return 0
	}
	if len(fs.Args()) != 0 {
		fmt.Fprintln(stderr, "error: unexpected args:", strings.Join(fs.Args(), " "))
		printEnqueueUsage(stderr)
		return 2
	}

	if strings.TrimSpace(jobType) == "" {
		fmt.Fprintln(stderr, "error: --type is required")
		printEnqueueUsage(stderr)
		return 2
	}
	if payloadStr != "" && payloadJSON != "" {
		fmt.Fprintln(stderr, "error: use only one of --payload or --payload-json")
		return 2
	}

	effectiveQueue := strings.TrimSpace(queue)
	if effectiveQueue == "" {
		effectiveQueue = strings.TrimSpace(g.Queue)
	}
	if effectiveQueue == "" {
		effectiveQueue = th.DefaultQueue
	}

	runAtTime, err := parseRunAt(runAt)
	if err != nil {
		fmt.Fprintln(stderr, "error:", err)
		return 2
	}

	var payload any = nil
	if payloadJSON != "" {
		b := []byte(payloadJSON)
		if !json.Valid(b) {
			fmt.Fprintln(stderr, "error: --payload-json must be valid JSON")
			return 2
		}
		payload = json.RawMessage(b)
	} else if payloadStr != "" {
		payload = payloadStr
	}

	ctx := context.Background()
	hnd, err := backend.Open(ctx, backend.Config{
		Driver:      g.Driver,
		PostgresDSN: g.PostgresDSN,
		RedisAddr:   g.RedisAddr,
	})
	if err != nil {
		fmt.Fprintln(stderr, "error:", err)
		return 1
	}
	defer func() { _ = hnd.Close() }()

	client := th.NewClient(hnd.Driver)

	req := th.JobRequest{
		Type:           jobType,
		Payload:        payload,
		Queue:          effectiveQueue,
		RunAt:          runAtTime,
		Timeout:        timeout,
		IdempotencyKey: idKey,
		MaxAttempts:    maxAttempts,
	}

	id, err := client.Enqueue(ctx, req)
	if err != nil {
		fmt.Fprintln(stderr, "error:", err)
		return 1
	}

	if g.JSON {
		out := map[string]any{
			"id":           id,
			"type":         jobType,
			"queue":        effectiveQueue,
			"max_attempts": maxAttempts,
			"idempotency":  idKey,
			"timeout_millis": func() any {
				if timeout <= 0 {
					return nil
				}
				return timeout.Milliseconds()
			}(),
			"run_at": func() any {
				if runAtTime.IsZero() {
					return nil
				}
				return runAtTime.Format(time.RFC3339Nano)
			}(),
		}
		enc := json.NewEncoder(stdout)
		enc.SetEscapeHTML(false)
		_ = enc.Encode(out)
		return 0
	}

	fmt.Fprintln(stdout, id)
	return 0
}

func parseRunAt(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, nil
	}

	// unix seconds or millis
	if isDigits(s) {
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid --run-at: %w", err)
		}
		if n < 0 {
			return time.Time{}, fmt.Errorf("invalid --run-at: must be >= 0")
		}
		// heuristic: 13+ digits -> ms
		if n >= 1_000_000_000_000 {
			return time.Unix(0, n*int64(time.Millisecond)).UTC(), nil
		}
		return time.Unix(n, 0).UTC(), nil
	}

	// RFC3339 / RFC3339Nano
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t.UTC(), nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t.UTC(), nil
	}

	return time.Time{}, fmt.Errorf("invalid --run-at: use RFC3339 (with timezone) or unix seconds")
}

func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}
