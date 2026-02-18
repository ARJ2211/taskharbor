package conformance

import (
	"testing"
	"time"

	"github.com/ARJ2211/taskharbor/taskharbor/driver"
)

func errorContractCases() []Case {
	cases := []Case{
		{
			Name: "reserve_with_invalid_lease_duration_returns_err_invalid_lease_duration",
			Fn: func(t *testing.T, d driver.Driver) {
				q := uniqueQueueName(t)
				now := fixedNow()

				_, _, _, err := d.Reserve(bg(), q, now, 0)
				mustErrIs(t, err, driver.ErrInvalidLeaseDuration, "reserve leaseFor=0")

				_, _, _, err = d.Reserve(bg(), q, now, -1*time.Second)
				mustErrIs(t, err, driver.ErrInvalidLeaseDuration, "reserve leaseFor<0")
			},
		},
		{
			Name: "extendlease_with_invalid_lease_duration_returns_err_invalid_lease_duration",
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

				_, err = d.ExtendLease(bg(), got.ID, lease.Token, now.Add(1*time.Second), 0)
				mustErrIs(t, err, driver.ErrInvalidLeaseDuration, "extendlease leaseFor=0")

				_, err = d.ExtendLease(bg(), got.ID, lease.Token, now.Add(1*time.Second), -1*time.Second)
				mustErrIs(t, err, driver.ErrInvalidLeaseDuration, "extendlease leaseFor<0")

				mustNoErr(t, d.Ack(bg(), got.ID, lease.Token, now.Add(2*time.Second)), "ack cleanup")
			},
		},
		{
			Name: "retry_with_wrong_token_returns_err_lease_mismatch",
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

				upd := driver.RetryUpdate{
					RunAt:     now.Add(1 * time.Minute),
					Attempts:  got.Attempts + 1,
					LastError: "boom",
					FailedAt:  now.Add(1 * time.Second),
				}

				err = d.Retry(bg(), got.ID, driver.LeaseToken("bad-token"), now.Add(2*time.Second), upd)
				mustErrIs(t, err, driver.ErrLeaseMismatch, "retry wrong token")

				mustNoErr(t, d.Ack(bg(), got.ID, lease.Token, now.Add(3*time.Second)), "ack cleanup")
			},
		},
		{
			Name: "retry_after_expiry_returns_err_lease_expired",
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

				upd := driver.RetryUpdate{
					RunAt:     now.Add(1 * time.Minute),
					Attempts:  got.Attempts + 1,
					LastError: "boom",
					FailedAt:  now.Add(1 * time.Second),
				}

				expiredNow := now.Add(leaseFor + 1*time.Millisecond)
				err = d.Retry(bg(), got.ID, lease.Token, expiredNow, upd)
				mustErrIs(t, err, driver.ErrLeaseExpired, "retry after expiry")

				// cleanup: reclaim by reserving again, then ack
				got2, lease2, ok, err := d.Reserve(bg(), q, expiredNow, 10*time.Second)
				mustNoErr(t, err, "reserve after expiry reclaim")
				if !ok {
					t.Fatalf("reserve after expiry reclaim: expected ok=true, got false")
				}
				if got2.ID != got.ID {
					t.Fatalf("reserve after expiry reclaim: expected id=%s, got %s", got.ID, got2.ID)
				}

				mustNoErr(t, d.Ack(bg(), got2.ID, lease2.Token, expiredNow.Add(1*time.Second)), "ack cleanup")
			},
		},
		{
			Name: "retry_when_not_inflight_returns_err_job_not_inflight",
			Fn: func(t *testing.T, d driver.Driver) {
				q := uniqueQueueName(t)
				now := fixedNow()

				rec := newRecord(t, q, time.Time{}, now)
				_, _, err := d.Enqueue(bg(), rec)
				mustNoErr(t, err, "enqueue ready job")

				upd := driver.RetryUpdate{
					RunAt:     now.Add(1 * time.Minute),
					Attempts:  1,
					LastError: "boom",
					FailedAt:  now.Add(1 * time.Second),
				}

				err = d.Retry(bg(), rec.ID, driver.LeaseToken("whatever"), now.Add(2*time.Second), upd)
				mustErrIs(t, err, driver.ErrJobNotInflight, "retry not inflight")
			},
		},
		{
			Name: "fail_with_wrong_token_returns_err_lease_mismatch",
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

				err = d.Fail(bg(), got.ID, driver.LeaseToken("bad-token"), now.Add(1*time.Second), "nope")
				mustErrIs(t, err, driver.ErrLeaseMismatch, "fail wrong token")

				mustNoErr(t, d.Ack(bg(), got.ID, lease.Token, now.Add(2*time.Second)), "ack cleanup")
			},
		},
		{
			Name: "fail_after_expiry_returns_err_lease_expired",
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
				err = d.Fail(bg(), got.ID, lease.Token, expiredNow, "nope")
				mustErrIs(t, err, driver.ErrLeaseExpired, "fail after expiry")

				// cleanup: reclaim by reserving again, then ack
				got2, lease2, ok, err := d.Reserve(bg(), q, expiredNow, 10*time.Second)
				mustNoErr(t, err, "reserve after expiry reclaim")
				if !ok {
					t.Fatalf("reserve after expiry reclaim: expected ok=true, got false")
				}
				if got2.ID != got.ID {
					t.Fatalf("reserve after expiry reclaim: expected id=%s, got %s", got.ID, got2.ID)
				}

				mustNoErr(t, d.Ack(bg(), got2.ID, lease2.Token, expiredNow.Add(1*time.Second)), "ack cleanup")
			},
		},
		{
			Name: "fail_when_not_inflight_returns_err_job_not_inflight",
			Fn: func(t *testing.T, d driver.Driver) {
				q := uniqueQueueName(t)
				now := fixedNow()

				rec := newRecord(t, q, time.Time{}, now)
				_, _, err := d.Enqueue(bg(), rec)
				mustNoErr(t, err, "enqueue ready job")

				err = d.Fail(bg(), rec.ID, driver.LeaseToken("whatever"), now.Add(1*time.Second), "nope")
				mustErrIs(t, err, driver.ErrJobNotInflight, "fail not inflight")
			},
		},
		{
			Name: "retry_after_done_returns_err_job_not_inflight",
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

				mustNoErr(t, d.Ack(bg(), got.ID, lease.Token, now.Add(1*time.Second)), "ack job")

				upd := driver.RetryUpdate{
					RunAt:     now.Add(1 * time.Minute),
					Attempts:  got.Attempts + 1,
					LastError: "boom",
					FailedAt:  now.Add(2 * time.Second),
				}

				err = d.Retry(bg(), got.ID, lease.Token, now.Add(3*time.Second), upd)
				mustErrIs(t, err, driver.ErrJobNotInflight, "retry after done")
			},
		},
	}

	return cases
}
