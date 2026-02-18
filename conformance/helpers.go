package conformance

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ARJ2211/taskharbor/taskharbor/driver"
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
Helper function to create a unique JobID
*/
func uniqueJobID(t *testing.T) string {
	t.Helper()
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return "job_" + hex.EncodeToString(b)
}

func fixedNow() time.Time {
	// stable, UTC, already microsecond-aligned
	return time.Date(2030, 1, 2, 3, 4, 5, 123456000, time.UTC)
}

/*
Helper function to create and return a JobRecord
(Driver content after getting a job request)
Remaining params can be defaulted here ...
*/
func newRecord(
	t *testing.T,
	queue string,
	runAt time.Time,
	createdAt time.Time,
) driver.JobRecord {
	t.Helper()

	rec := driver.JobRecord{
		ID:             uniqueJobID(t),
		Type:           "test",
		Payload:        []byte("{x:1}"),
		Queue:          queue,
		RunAt:          runAt,
		Timeout:        0,
		IdempotencyKey: "",
		CreatedAt:      createdAt,
		Attempts:       0,
		MaxAttempts:    3, // Default for now
		LastError:      "",
		FailedAt:       time.Time{},
	}

	return rec
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
	if !errors.Is(err, target) {
		t.Fatalf("%s: expected error %v, got %v", msg, target, err)
	}
}

/*
Helper function to create a context to run the tests in
*/
func bg() context.Context { return context.Background() }
