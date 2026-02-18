package memory

import (
	"testing"

	"github.com/ARJ2211/taskharbor/conformance"
	"github.com/ARJ2211/taskharbor/taskharbor/driver"
)

func TestConformance(t *testing.T) {
	conformance.Run(t, conformance.Factory{
		Name: "memory",
		New: func(t *testing.T) driver.Driver {
			memDriver := New()
			return memDriver
		},
	}, conformance.All()...)
}
