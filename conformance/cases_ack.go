package conformance

import (
	"testing"
	"time"

	"github.com/ARJ2211/taskharbor/taskharbor/driver"
)

/*
Returns a list of type Case with
all the tests related to ack
operation of drivers.
*/
func ackCases() []Case {
	cases := []Case{
		{
			Name: "ack_with_correct_token_marks_done_and_is_idempotent",
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

				got, lease, ok, err := d.Reserve(bg(), q, now, leaseFor)
				mustNoErr(t, err, "reserve job")
				if !ok {
					t.Fatalf("reserve job: expected ok=true, got false")
				}
				if got.ID != rec.ID {
					t.Fatalf("reserve job: expected id=%s, got %s", rec.ID, got.ID)
				}

				ackNow := now.Add(1 * time.Second)
				mustNoErr(t, d.Ack(bg(), got.ID, lease.Token, ackNow), "ack with correct token")

				// Terminal idempotency rule: once done, repeated Ack is a no-op success.
				mustNoErr(t, d.Ack(bg(), got.ID, lease.Token, ackNow.Add(1*time.Second)), "ack is idempotent after done")
			},
		},
		{
			Name: "ack_with_wrong_token_returns_lease_mismatch",
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
				err = d.Ack(bg(), got.ID, bad, now.Add(1*time.Second))
				mustErrIs(t, err, driver.ErrLeaseMismatch, "ack wrong token")

				// cleanup
				mustNoErr(t, d.Ack(bg(), got.ID, lease.Token, now.Add(2*time.Second)), "ack cleanup")
			},
		},
		{
			Name: "ack_after_expiry_returns_lease_expired",
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
				err = d.Ack(bg(), got.ID, lease.Token, expiredNow)
				mustErrIs(t, err, driver.ErrLeaseExpired, "ack after expiry")

				// cleanup: reclaim by reserving again, then ack with new token
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
			Name: "ack_when_not_inflight_returns_job_not_inflight",
			Fn: func(t *testing.T, d driver.Driver) {
				q := uniqueQueueName(t)
				now := fixedNow()

				rec := newRecord(t, q, time.Time{}, now)
				_, _, err := d.Enqueue(bg(), rec)
				mustNoErr(t, err, "enqueue ready job")

				err = d.Ack(bg(), rec.ID, driver.LeaseToken("whatever"), now.Add(1*time.Second))
				mustErrIs(t, err, driver.ErrJobNotInflight, "ack not inflight")
			},
		},
	}

	return cases
}
