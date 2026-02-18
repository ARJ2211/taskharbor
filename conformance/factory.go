package conformance

import (
	"testing"

	"github.com/ARJ2211/taskharbor/taskharbor/driver"
)

type Factory struct {
	Name    string                           // Used just for logging
	New     func(t *testing.T) driver.Driver // Creates a fresh driver for testing
	Cleanup func(t *testing.T)               // Cleanup after test cases
}

type Case struct {
	Name string
	Fn   func(t *testing.T, d driver.Driver)
}
