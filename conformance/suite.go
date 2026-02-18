package conformance

import (
	"testing"
)

/*
This function runs the various conformance tests for
the driver
*/
func Run(t *testing.T, f Factory, cases ...Case) {
	t.Helper()

	if f.New == nil {
		t.Fatalf("conformance/factory: f.New is required")
	}

	for _, tc := range cases {
		run := func(t *testing.T) {
			t.Helper()

			d := f.New(t)
			if d == nil {
				t.Fatalf("conformance/factory: f.New did not produce a driver")
			}

			t.Cleanup(func() { _ = d.Close() })

			if f.Cleanup != nil {
				t.Cleanup(func() { f.Cleanup(t) })
			}

			tc.Fn(t, d) // Run the test
		}

		t.Run(tc.Name, run)

	}
}

/*
This is a convenience to keep driver wappers clean,
comes later in #93
*/
func All() []Case {
	out := make([]Case, 0, 8)

	out = append(out, enqueueScheduleCases()...) // Need to add them here...
	out = append(out, reserveLeaseCases()...)
	out = append(out, extendLeaseCases()...)
	out = append(out, ackCases()...)
	out = append(out, retryFailDLQCases()...)
	out = append(out, errorContractCases()...)

	return out
}
