package app

import (
	"flag"
	"fmt"
	"io"

	th "github.com/ARJ2211/taskharbor/taskharbor"
	"github.com/ARJ2211/taskharbor/taskharbor/driver"
)

type GlobalFlags struct {
	Driver  string
	Queue   string
	JSON    bool
	Verbose bool
}

func Run(argv []string, stdout, stderr io.Writer) {
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
}

func runWorker() {

}

func runEnqueue() {

}

func runList() {

}

func runInspect() {

}

func runDlqList() {

}

func runDlqRetry() {

}

func runJobRetry() {

}
