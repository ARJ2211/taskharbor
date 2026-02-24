package app

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	th "github.com/ARJ2211/taskharbor/taskharbor"
	"github.com/ARJ2211/taskharbor/taskharbor/driver"
)

type GlobalFlags struct {
	Driver  string
	Queue   string
	JSON    bool
	Verbose bool

	PostgresDSN string
	RedisAddr   string
}

func envOr(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func Run(argv []string, stdout, stderr io.Writer) int {
	var g GlobalFlags
	var help bool
	var h bool

	fs := flag.NewFlagSet("taskharbor", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	driverList := ""
	for _, d := range driver.ImplementedDrivers {
		driverList += d + "|"
	}
	driverList = driverList[:len(driverList)-1]

	fs.StringVar(&g.Driver, "driver", "memory", fmt.Sprintf("drivers: %s", driverList))
	fs.StringVar(&g.Queue, "queue", th.DefaultQueue, "queue name")
	fs.BoolVar(&g.JSON, "json", false, "output JSON")
	fs.BoolVar(&g.Verbose, "verbose", false, "verbose logs")
	fs.BoolVar(&help, "help", false, "show help")
	fs.BoolVar(&h, "h", false, "show help")

	fs.StringVar(&g.PostgresDSN, "dsn", envOr("TH_PG_DSN", envOr("TH_POSTGRES_DSN", "")), "postgres DSN (for --driver postgres)")
	fs.StringVar(&g.RedisAddr, "redis-addr", envOr("TH_REDIS_ADDR", ""), "redis addr host:port (for --driver redis)")

	if err := fs.Parse(argv); err != nil {
		fmt.Println(stderr, "error: ", err)
		printRootUsage(stderr)
		return 2
	}

	args := fs.Args()
	if help || h || len(argv) == 0 {
		printRootUsage(stdout)
		return 0
	}

	cmd := args[0]
	cmdArgs := args[1:]

	switch cmd {
	case "worker":
		return runWorker(g, cmdArgs, stdout, stderr)
	case "enqueue":
		return runEnqueue(g, cmdArgs, stdout, stderr)
	case "list":
		return runList(g, cmdArgs, stdout, stderr)
	case "inspect":
		return runInspect(g, cmdArgs, stdout, stderr)
	case "dlq":
		return runDLQ(g, cmdArgs, stdout, stderr)
	case "job":
		return runJob(g, cmdArgs, stdout, stderr)
	case "help":
		printRootUsage(stdout)
		return 0
	default:
		fmt.Fprintln(stderr, "error: unknown command:", cmd)
		printRootUsage(stderr)
		return 2
	}
}

func runWorker(g GlobalFlags, argv []string, stdout, stderr io.Writer) int {
	var help bool
	var h bool

	fs := flag.NewFlagSet("taskharbor worker", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.BoolVar(&help, "help", false, "show help")
	fs.BoolVar(&h, "h", false, "show help")

	if err := fs.Parse(argv); err != nil {
		fmt.Fprintln(stderr, "error:", err)
		printWorkerUsage(stderr)
		return 2
	}

	args := fs.Args()
	if help || h || len(args) == 0 {
		printWorkerUsage(stdout)
		return 0
	}

	sub := args[0]
	subArgs := args[1:]

	switch sub {
	case "run":
		return runWorkerRun(g, subArgs, stdout, stderr)
	default:
		fmt.Fprintln(stderr, "error: unknown subcommand: worker", sub)
		printWorkerUsage(stderr)
		return 2
	}
}

func runDLQ(g GlobalFlags, argv []string, stdout, stderr io.Writer) int {
	var help bool
	var h bool

	fs := flag.NewFlagSet("taskharbor dlq", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.BoolVar(&help, "help", false, "show help")
	fs.BoolVar(&h, "h", false, "show help")

	if err := fs.Parse(argv); err != nil {
		fmt.Fprintln(stderr, "error:", err)
		printDLQUsage(stderr)
		return 2
	}

	args := fs.Args()
	if help || h || len(args) == 0 {
		printDLQUsage(stdout)
		return 0
	}

	sub := args[0]
	subArgs := args[1:]

	switch sub {
	case "list":
		return runDLQList(g, subArgs, stdout, stderr)
	case "requeue":
		return runDLQRequeue(g, subArgs, stdout, stderr)
	default:
		fmt.Fprintln(stderr, "error: unknown subcommand: dlq", sub)
		printDLQUsage(stderr)
		return 2
	}
}

func runJob(g GlobalFlags, argv []string, stdout, stderr io.Writer) int {
	var help bool
	var h bool

	fs := flag.NewFlagSet("taskharbor job", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.BoolVar(&help, "help", false, "show help")
	fs.BoolVar(&h, "h", false, "show help")

	if err := fs.Parse(argv); err != nil {
		fmt.Fprintln(stderr, "error:", err)
		printJobUsage(stderr)
		return 2
	}

	args := fs.Args()
	if help || h || len(args) == 0 {
		printJobUsage(stdout)
		return 0
	}

	sub := args[0]
	subArgs := args[1:]

	switch sub {
	case "retry":
		return runJobRetry(g, subArgs, stdout, stderr)
	default:
		fmt.Fprintln(stderr, "error: unknown subcommand: job", sub)
		printJobUsage(stderr)
		return 2
	}
}
