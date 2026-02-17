package conformance

import (
	"fmt"
	"regexp"
	"sync/atomic"
	"testing"
	"time"
)

var queueSeq uint64

/*
Helper function to create a unique queue name
*/
func uniqueQueueName(t *testing.T) string {
	t.Helper()

	n := atomic.AddUint64(&queueSeq, 1)
	base := fmt.Sprintf("%s_%d", t.Name(), n)

	re := regexp.MustCompile(`[^a-zA-Z0-9_]+`) // Keep it backend friendly
	base = re.ReplaceAllString(base, "_")
	if len(base) > 63 {
		base = base[:63]
	}

	return "q_" + base
}

/*
Helper function to convert timestamps
to a microsecond precision. This is for
postgres since it stores timestamps with
microsecond precision.
*/
func nowMicros(t time.Time) time.Time {
	u := t.UTC().UnixMicro()
	return time.UnixMicro(u).UTC()
}

/*
Helper function to keep error messages
consistent
*/
func mustNoErr(t *testing.T, err error, msg string) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", msg, err)
	}
}

func mustErrIs(t *testing.T, err error, target error, msg string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: expected error %v, got nil", msg, target)
	}
}
