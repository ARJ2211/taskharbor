package app

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ARJ2211/taskharbor/cmd/taskharbor/internal/backend"
	th "github.com/ARJ2211/taskharbor/taskharbor"
)

func runWorkerRun(g GlobalFlags, argv []string, stdout, stderr io.Writer) int {
	var (
		help bool
		h    bool

		concurrency int
		poll        time.Duration
		lease       time.Duration
		heartbeat   time.Duration

		reg multiString
	)
	fs := flag.NewFlagSet("taskharbor worker run", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	fs.IntVar(&concurrency, "concurrency", 0, "max concurrent jobs (default: taskharbor default)")
	fs.DurationVar(&poll, "poll-interval", 0, "poll interval when no jobs (e.g. 200ms)")
	fs.DurationVar(&lease, "lease-duration", 0, "lease duration (e.g. 30s)")
	fs.DurationVar(&heartbeat, "heartbeat-interval", 0, "lease heartbeat interval (e.g. 10s)")
	fs.Var(&reg, "register", "map jobType to builtin handler (repeatable). format: jobType=builtin. builtins: echo,fail,sleep")

	fs.BoolVar(&help, "help", false, "show help")
	fs.BoolVar(&h, "h", false, "show help")

	if err := fs.Parse(argv); err != nil {
		fmt.Fprintln(stderr, "error:", err)
		printWorkerRunUsage(stderr)
		return 2
	}
	if help || h {
		printWorkerRunUsage(stdout)
		return 0
	}
	if len(fs.Args()) != 0 {
		fmt.Fprintln(stderr, "error: unexpected args:", strings.Join(fs.Args(), " "))
		printWorkerRunUsage(stderr)
		return 2
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Ctrl+C / SIGTERM -> graceful shutdown
	sigc := make(chan os.Signal, 2)
	signal.Notify(sigc, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigc)
	go func() {
		<-sigc
		cancel()
	}()

	bh, err := backend.Open(ctx, backend.Config{
		Driver:      g.Driver,
		PostgresDSN: g.PostgresDSN,
		RedisAddr:   g.RedisAddr,
	})
	if err != nil {
		fmt.Fprintln(stderr, "error:", err)
		return 1
	}
	defer func() { _ = bh.Close() }()

	opts := make([]th.Option, 0, 6)
	opts = append(opts, th.WithDefaultQueue(g.Queue))
	if concurrency > 0 {
		opts = append(opts, th.WithConcurrency(concurrency))
	}
	if poll > 0 {
		opts = append(opts, th.WithPollInterval(poll))
	}
	if lease > 0 {
		opts = append(opts, th.WithLeaseDuration(lease))
	}
	if heartbeat > 0 {
		opts = append(opts, th.WithHeartbeatInterval(heartbeat))
	}

	worker := th.NewWorker(bh.Driver, opts...)

	// Example Builtins
	builtins := map[string]th.Handler{
		"echo":  echoHandler(stdout),
		"fail":  failHandler(),
		"sleep": sleepHandler(stdout),
	}

	// Always register the default names.
	for name, fn := range builtins {
		_ = worker.Register(name, fn)
	}

	// Apply --register mappings (jobType=builtin)
	for _, m := range reg {
		jobType, builtin, ok := strings.Cut(m, "=")
		jobType = strings.TrimSpace(jobType)
		if !ok {
			// allow shorthand
			builtin = jobType
		}
		builtin = strings.TrimSpace(builtin)

		if jobType == "" || builtin == "" {
			fmt.Fprintln(stderr, "error: invalid --register:", m)
			return 2
		}
		fn, ok := builtins[builtin]
		if !ok {
			fmt.Fprintln(stderr, "error: unknown builtin for --register:", builtin)
			return 2
		}
		if err := worker.Register(jobType, fn); err != nil {
			fmt.Fprintln(stderr, "error:", err)
			return 1
		}
	}

	fmt.Fprintf(stdout, "worker started id=%s driver=%s queue=%s\n", worker.ID(), strings.ToLower(g.Driver), g.Queue)

	if err := worker.Run(ctx); err != nil {
		fmt.Fprintln(stderr, "error:", err)
		return 1
	}

	fmt.Fprintln(stdout, "worker stopped")
	return 0
}

func echoHandler(w io.Writer) th.Handler {
	return func(ctx context.Context, job th.Job) error {
		_ = ctx
		fmt.Fprintf(w, "echo id=%s type=%s queue=%s payload=%s\n", job.ID, job.Type, job.Queue, string(job.Payload))
		return nil
	}
}

func failHandler() th.Handler {
	return func(ctx context.Context, job th.Job) error {
		_ = ctx
		_ = job
		return errors.New("fail handler: requested failure")
	}
}

func sleepHandler(w io.Writer) th.Handler {
	return func(ctx context.Context, job th.Job) error {
		d := parseSleepDuration(job.Payload)
		if d <= 0 {
			d = 250 * time.Millisecond
		}
		fmt.Fprintf(w, "sleep id=%s duration=%s\n", job.ID, d)
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			return nil
		}
	}
}

func parseSleepDuration(payload []byte) time.Duration {
	s := strings.TrimSpace(string(payload))
	if s == "" {
		return 0
	}

	// Try plain int millis: "1500" or `"1500"`
	s = strings.Trim(s, "\"")
	if n, err := strconv.Atoi(s); err == nil {
		return time.Duration(n) * time.Millisecond
	}

	// Try JSON number or JSON object: {"ms": 1500} or {"duration":"1.5s"}
	var num int
	if err := json.Unmarshal(payload, &num); err == nil {
		return time.Duration(num) * time.Millisecond
	}

	var obj struct {
		MS       int    `json:"ms"`
		Duration string `json:"duration"`
	}
	if err := json.Unmarshal(payload, &obj); err == nil {
		if obj.Duration != "" {
			if d, err := time.ParseDuration(obj.Duration); err == nil {
				return d
			}
		}
		if obj.MS > 0 {
			return time.Duration(obj.MS) * time.Millisecond
		}
	}

	return 0
}
