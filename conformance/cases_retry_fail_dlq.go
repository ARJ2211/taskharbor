package conformance

import (
	"testing"
	"time"

	"github.com/ARJ2211/taskharbor/taskharbor/driver"
)

func retryFailDLQCases() []Case {
	cases := []Case{
		{
			Name: "retry_persists_failure_metadata_and_respects_runAt_schedule",
			Fn: func(t *testing.T, d driver.Driver) {
				q := uniqueQueueName(t)
				now := fixedNow()
				leaseFor := 20 * time.Second

				rec := newRecord(t, q, time.Time{}, now)
				_, existed, err := d.Enqueue(bg(), rec)
				mustNoErr(t, err, "enqueue ready job")
				if existed {
					t.Fatalf("enqueue ready job: expected existed=false, got true")
				}

				got, lease, ok, err := d.Reserve(bg(), q, now, leaseFor)
				mustNoErr(t, err, "reserve job")
				if !ok {
					t.Fatalf("reserve job: expected ok=true, got false")
				}
				if got.ID != rec.ID {
					t.Fatalf("reserve job: expected id=%s, got %s", rec.ID, got.ID)
				}

				nextRunAt := now.Add(1 * time.Minute)
				failedAt := now.Add(5 * time.Second)
				upd := driver.RetryUpdate{
					RunAt:     nextRunAt,
					Attempts:  got.Attempts + 1,
					LastError: "handler error: boom",
					FailedAt:  failedAt,
				}

				retryNow := now.Add(1 * time.Second)
				mustNoErr(t, d.Retry(bg(), got.ID, lease.Token, retryNow, upd), "retry job")

				// Before nextRunAt it must not be reservable.
				before := now.Add(30 * time.Second)
				_, _, ok, err = d.Reserve(bg(), q, before, leaseFor)
				mustNoErr(t, err, "reserve before nextRunAt")
				if ok {
					t.Fatalf("reserve before nextRunAt: expected ok=false, got true")
				}

				// At/after nextRunAt it must be reservable.
				at := nextRunAt
				got2, lease2, ok, err := d.Reserve(bg(), q, at, leaseFor)
				mustNoErr(t, err, "reserve at/after nextRunAt")
				if !ok {
					t.Fatalf("reserve at/after nextRunAt: expected ok=true, got false")
				}
				if got2.ID != rec.ID {
					t.Fatalf("reserve at/after nextRunAt: expected id=%s, got %s", rec.ID, got2.ID)
				}

				// Failure metadata should persist across retry.
				if got2.Attempts != upd.Attempts {
					t.Fatalf("retry attempts: expected %d, got %d", upd.Attempts, got2.Attempts)
				}
				if got2.LastError != upd.LastError {
					t.Fatalf("retry last_error: expected %q, got %q", upd.LastError, got2.LastError)
				}
				if nowMicros(got2.FailedAt) != nowMicros(upd.FailedAt) {
					t.Fatalf("retry failed_at: expected %v, got %v", upd.FailedAt, got2.FailedAt)
				}

				mustNoErr(t, d.Ack(bg(), got2.ID, lease2.Token, at.Add(1*time.Second)), "ack after retry")
			},
		},
		{
			Name: "retry_with_zero_runAt_makes_job_runnable_immediately",
			Fn: func(t *testing.T, d driver.Driver) {
				q := uniqueQueueName(t)
				now := fixedNow()
				leaseFor := 20 * time.Second

				rec := newRecord(t, q, time.Time{}, now)
				_, _, err := d.Enqueue(bg(), rec)
				mustNoErr(t, err, "enqueue ready job")

				got, lease, ok, err := d.Reserve(bg(), q, now, leaseFor)
				mustNoErr(t, err, "reserve job")
				if !ok {
					t.Fatalf("reserve job: expected ok=true, got false")
				}

				upd := driver.RetryUpdate{
					RunAt:     time.Time{}, // runnable now
					Attempts:  got.Attempts + 1,
					LastError: "handler error: immediate",
					FailedAt:  now.Add(2 * time.Second),
				}

				retryNow := now.Add(1 * time.Second)
				mustNoErr(t, d.Retry(bg(), got.ID, lease.Token, retryNow, upd), "retry job immediate")

				soon := now.Add(3 * time.Second)
				got2, lease2, ok, err := d.Reserve(bg(), q, soon, leaseFor)
				mustNoErr(t, err, "reserve immediately after retry")
				if !ok {
					t.Fatalf("reserve immediately after retry: expected ok=true, got false")
				}
				if got2.ID != rec.ID {
					t.Fatalf("reserve immediately after retry: expected id=%s, got %s", rec.ID, got2.ID)
				}

				mustNoErr(t, d.Ack(bg(), got2.ID, lease2.Token, soon.Add(1*time.Second)), "ack after immediate retry")
			},
		},
		{
			Name: "fail_moves_job_to_dlq_and_it_is_not_reservable",
			Fn: func(t *testing.T, d driver.Driver) {
				q := uniqueQueueName(t)
				now := fixedNow()
				leaseFor := 20 * time.Second

				rec := newRecord(t, q, time.Time{}, now)
				_, _, err := d.Enqueue(bg(), rec)
				mustNoErr(t, err, "enqueue ready job")

				got, lease, ok, err := d.Reserve(bg(), q, now, leaseFor)
				mustNoErr(t, err, "reserve job")
				if !ok {
					t.Fatalf("reserve job: expected ok=true, got false")
				}

				failNow := now.Add(1 * time.Second)
				mustNoErr(t, d.Fail(bg(), got.ID, lease.Token, failNow, "unrecoverable"), "fail job to dlq")

				// Decide + enforce: Fail repeated on terminal state is a no-op success.
				mustNoErr(t, d.Fail(bg(), got.ID, lease.Token, failNow.Add(1*time.Second), "unrecoverable"), "fail is idempotent on dlq")

				// DLQ jobs must not be reservable by normal Reserve.
				later := now.Add(10 * time.Minute)
				_, _, ok, err = d.Reserve(bg(), q, later, leaseFor)
				mustNoErr(t, err, "reserve after dlq")
				if ok {
					t.Fatalf("reserve after dlq: expected ok=false, got true")
				}
			},
		},
	}

	return cases
}
