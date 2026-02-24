package main

import (
	"os"

	"github.com/ARJ2211/taskharbor/cmd/taskharbor/internal/app"
)

func main() {
	os.Exit(app.Run(os.Args[1:], os.Stdout, os.Stderr))
}
