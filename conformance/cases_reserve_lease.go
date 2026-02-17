package conformance

import (
	"testing"
	"time"

	"github.com/ARJ2211/taskharbor/taskharbor/driver"
)

/*
Returns a list of type Case with
all the tests related to reserve
operation of drivers.
*/
func reserveLeaseCases() []Case {
	cases := []Case{
		{
			Name: "reserve_is_exclusive_during_valid_lease",
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

				got1, lease1, ok, err := d.Reserve(bg(), q, now, leaseFor)
				mustNoErr(t, err, "reserve first time")
				if !ok {
					t.Fatalf("reserve first time: expected ok=true, got false")
				}
				if got1.ID != rec.ID {
					t.Fatalf("reserve first time: expected id=%s, got %s", rec.ID, got1.ID)
				}

				mid := now.Add(leaseFor / 2)
				_, _, ok, err = d.Reserve(bg(), q, mid, leaseFor)
				mustNoErr(t, err, "reserve during valid lease")
				if ok {
					t.Fatalf("reserve during valid lease: expected ok=false, got true")
				}

				mustNoErr(t, d.Ack(bg(), got1.ID, lease1.Token, now.Add(1*time.Second)), "ack original lease")
			},
		},
		{
			Name: "expired_lease_is_reclaimed_and_job_is_reservable_again",
			Fn: func(t *testing.T, d driver.Driver) {
				q := uniqueQueueName(t)
				now := fixedNow()
				leaseFor := 10 * time.Second

				rec := newRecord(t, q, time.Time{}, now)
				_, existed, err := d.Enqueue(bg(), rec)
				mustNoErr(t, err, "enqueue ready job")
				if existed {
					t.Fatalf("enqueue ready job: expected existed=false, got true")
				}

				got1, _, ok, err := d.Reserve(bg(), q, now, leaseFor)
				mustNoErr(t, err, "reserve first time")
				if !ok {
					t.Fatalf("reserve first time: expected ok=true, got false")
				}
				if got1.ID != rec.ID {
					t.Fatalf("reserve first time: expected id=%s, got %s", rec.ID, got1.ID)
				}

				expiredNow := now.Add(leaseFor + 1*time.Millisecond)
				got2, lease2, ok, err := d.Reserve(bg(), q, expiredNow, leaseFor)
				mustNoErr(t, err, "reserve after expiry (should reclaim)")
				if !ok {
					t.Fatalf("reserve after expiry: expected ok=true, got false")
				}
				if got2.ID != rec.ID {
					t.Fatalf("reserve after expiry: expected id=%s, got %s", rec.ID, got2.ID)
				}

				mustNoErr(t, d.Ack(bg(), got2.ID, lease2.Token, expiredNow.Add(1*time.Second)), "ack reclaimed job")
			},
		},
		{
			Name: "reclaim_clears_runAt_job_becomes_runnable_now",
			Fn: func(t *testing.T, d driver.Driver) {
				q := uniqueQueueName(t)
				now := fixedNow()
				due := now.Add(1 * time.Minute)
				leaseFor := 10 * time.Second

				// Create as scheduled so RunAt is definitely non-zero in storage.
				rec := newRecord(t, q, due, now)
				_, existed, err := d.Enqueue(bg(), rec)
				mustNoErr(t, err, "enqueue scheduled job")
				if existed {
					t.Fatalf("enqueue scheduled job: expected existed=false, got true")
				}

				// Reserve at due (job becomes inflight).
				got1, _, ok, err := d.Reserve(bg(), q, due, leaseFor)
				mustNoErr(t, err, "reserve at due")
				if !ok {
					t.Fatalf("reserve at due: expected ok=true, got false")
				}
				if got1.ID != rec.ID {
					t.Fatalf("reserve at due: expected id=%s, got %s", rec.ID, got1.ID)
				}

				// Let the lease expire and re-reserve; reclaimed job must be runnable now,
				// which means RunAt is cleared/zero.
				expiredNow := due.Add(leaseFor + 1*time.Millisecond)
				got2, lease2, ok, err := d.Reserve(bg(), q, expiredNow, leaseFor)
				mustNoErr(t, err, "reserve after expiry (should reclaim)")
				if !ok {
					t.Fatalf("reserve after expiry: expected ok=true, got false")
				}
				if got2.ID != rec.ID {
					t.Fatalf("reserve after expiry: expected id=%s, got %s", rec.ID, got2.ID)
				}
				if !got2.RunAt.IsZero() {
					t.Fatalf("reclaim should clear run_at: expected zero RunAt, got %v", got2.RunAt)
				}

				mustNoErr(t, d.Ack(bg(), got2.ID, lease2.Token, expiredNow.Add(1*time.Second)), "ack reclaimed scheduled job")
			},
		},
	}

	return cases
}
