package conformance

import (
	"testing"
	"time"

	"github.com/ARJ2211/taskharbor/taskharbor/driver"
)

/*
Returns a list of type Case with
all the tests related to extend
lease operation of drivers.
*/
func extendLeaseCases() []Case {
	cases := []Case{
		{
			Name: "extendlease_with_correct_token_extends_expiry",
			Fn: func(t *testing.T, d driver.Driver) {
				q := uniqueQueueName(t)
				now := fixedNow()

				leaseFor1 := 10 * time.Second
				leaseFor2 := 20 * time.Second

				rec := newRecord(t, q, time.Time{}, now)
				_, existed, err := d.Enqueue(bg(), rec)
				mustNoErr(t, err, "enqueue ready job")
				if existed {
					t.Fatalf("enqueue ready job: expected existed=false, got true")
				}

				got, lease1, ok, err := d.Reserve(bg(), q, now, leaseFor1)
				mustNoErr(t, err, "reserve job")
				if !ok {
					t.Fatalf("reserve job: expected ok=true, got false")
				}
				if got.ID != rec.ID {
					t.Fatalf("reserve job: expected id=%s, got %s", rec.ID, got.ID)
				}

				extendNow := now.Add(5 * time.Second)
				lease2, err := d.ExtendLease(bg(), got.ID, lease1.Token, extendNow, leaseFor2)
				mustNoErr(t, err, "extendlease with correct token")

				if !lease2.ExpiresAt.After(extendNow) {
					t.Fatalf("extendlease: expected expiry after extendNow=%v, got %v", extendNow, lease2.ExpiresAt)
				}
				if !lease2.ExpiresAt.After(lease1.ExpiresAt) {
					t.Fatalf("extendlease: expected expiry after previous expiry=%v, got %v", lease1.ExpiresAt, lease2.ExpiresAt)
				}

				ackNow := extendNow.Add(1 * time.Second)
				mustNoErr(t, d.Ack(bg(), got.ID, lease2.Token, ackNow), "ack using returned token (handles rotation)")
			},
		},
		{
			Name: "extendlease_with_wrong_token_returns_lease_mismatch",
			Fn: func(t *testing.T, d driver.Driver) {
				q := uniqueQueueName(t)
				now := fixedNow()

				leaseFor := 10 * time.Second

				rec := newRecord(t, q, time.Time{}, now)
				_, _, err := d.Enqueue(bg(), rec)
				mustNoErr(t, err, "enqueue ready job")

				got, lease, ok, err := d.Reserve(bg(), q, now, leaseFor)
				mustNoErr(t, err, "reserve job")
				if !ok {
					t.Fatalf("reserve job: expected ok=true, got false")
				}

				bad := driver.LeaseToken("bad-token")
				_, err = d.ExtendLease(bg(), got.ID, bad, now.Add(1*time.Second), leaseFor)
				mustErrIs(t, err, driver.ErrLeaseMismatch, "extendlease wrong token")

				// ack with correct token while still valid
				mustNoErr(t, d.Ack(bg(), got.ID, lease.Token, now.Add(2*time.Second)), "ack cleanup")
			},
		},
		{
			Name: "extendlease_after_expiry_returns_lease_expired",
			Fn: func(t *testing.T, d driver.Driver) {
				q := uniqueQueueName(t)
				now := fixedNow()

				leaseFor := 5 * time.Second

				rec := newRecord(t, q, time.Time{}, now)
				_, _, err := d.Enqueue(bg(), rec)
				mustNoErr(t, err, "enqueue ready job")

				got, lease, ok, err := d.Reserve(bg(), q, now, leaseFor)
				mustNoErr(t, err, "reserve job")
				if !ok {
					t.Fatalf("reserve job: expected ok=true, got false")
				}

				expiredNow := now.Add(leaseFor + 1*time.Millisecond)
				_, err = d.ExtendLease(bg(), got.ID, lease.Token, expiredNow, leaseFor)
				mustErrIs(t, err, driver.ErrLeaseExpired, "extendlease after expiry")

				// cleanup: reclaim by reserving again after expiry, then ack
				got2, lease2, ok, err := d.Reserve(bg(), q, expiredNow, 10*time.Second)
				mustNoErr(t, err, "reserve after expiry reclaim")
				if !ok {
					t.Fatalf("reserve after expiry reclaim: expected ok=true, got false")
				}
				if got2.ID != got.ID {
					t.Fatalf("reserve after expiry reclaim: expected id=%s, got %s", got.ID, got2.ID)
				}

				mustNoErr(t, d.Ack(bg(), got2.ID, lease2.Token, expiredNow.Add(1*time.Second)), "ack reclaimed job")
			},
		},
		{
			Name: "extendlease_when_not_inflight_returns_job_not_inflight",
			Fn: func(t *testing.T, d driver.Driver) {
				q := uniqueQueueName(t)
				now := fixedNow()

				rec := newRecord(t, q, time.Time{}, now)
				_, _, err := d.Enqueue(bg(), rec)
				mustNoErr(t, err, "enqueue ready job")

				_, err = d.ExtendLease(bg(), rec.ID, driver.LeaseToken("whatever"), now, 10*time.Second)
				mustErrIs(t, err, driver.ErrJobNotInflight, "extendlease not inflight")
			},
		},
	}

	return cases
}
