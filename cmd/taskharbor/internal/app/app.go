package app

import (
	"flag"
	"fmt"
	"io"
	"strings"

	th "github.com/ARJ2211/taskharbor/taskharbor"
	"github.com/ARJ2211/taskharbor/taskharbor/driver"
)

type GlobalFlags struct {
	Driver  string
	Queue   string
	JSON    bool
	Verbose bool
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

func runWorkerRun(_ GlobalFlags, argv []string, stdout, stderr io.Writer) int {
	var help bool
	var h bool

	fs := flag.NewFlagSet("taskharbor worker run", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
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

	fmt.Fprintln(stderr, "not implemented yet (issue #113)")
	return 1
}

func runEnqueue(_ GlobalFlags, argv []string, stdout, stderr io.Writer) int {
	var help bool
	var h bool

	fs := flag.NewFlagSet("taskharbor enqueue", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
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

	fmt.Fprintln(stderr, "not implemented yet (issue #114)")
	return 1
}

func runList(_ GlobalFlags, argv []string, stdout, stderr io.Writer) int {
	var help bool
	var h bool

	fs := flag.NewFlagSet("taskharbor list", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
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

	fmt.Fprintln(stderr, "not implemented yet (issue #119)")
	return 1
}

func runInspect(_ GlobalFlags, argv []string, stdout, stderr io.Writer) int {
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

	fmt.Fprintln(stderr, "not implemented yet (issue #119)")
	return 1
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

func runDLQList(_ GlobalFlags, argv []string, stdout, stderr io.Writer) int {
	var help bool
	var h bool

	fs := flag.NewFlagSet("taskharbor dlq list", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
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

	fmt.Fprintln(stderr, "not implemented yet (issue #119)")
	return 1
}

func runDLQRequeue(_ GlobalFlags, argv []string, stdout, stderr io.Writer) int {
	var help bool
	var h bool

	fs := flag.NewFlagSet("taskharbor dlq requeue", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
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

	fmt.Fprintln(stderr, "not implemented yet (issue #119)")
	return 1
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

func runJobRetry(_ GlobalFlags, argv []string, stdout, stderr io.Writer) int {
	var help bool
	var h bool

	fs := flag.NewFlagSet("taskharbor job retry", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
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

	fmt.Fprintln(stderr, "not implemented yet (issue #119)")
	return 1
}
