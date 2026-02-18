package conformance

import (
	"testing"
	"time"

	"github.com/ARJ2211/taskharbor/taskharbor/driver"
)

/*
Returns a list of type Case with
all the tests related to enqueue
operation of drivers.
*/
func enqueueScheduleCases() []Case {
	cases := []Case{
		{
			Name: "enqueue_rejects_invalid_record",
			Fn: func(t *testing.T, d driver.Driver) {
				q := uniqueJobID(t)
				now := fixedNow()

				rec := newRecord(t, q, time.Time{}, now)
				rec.ID = "" // INCORRECT EXPECTED HERE

				_, _, err := d.Enqueue(bg(), rec)
				mustErrIs(t, err, driver.ErrJobIDRequired, "enqueue invalid record")
			},
		},
		{
			Name: "enqueue_ready_is_reservable_immediately",
			Fn: func(t *testing.T, d driver.Driver) {
				q := uniqueQueueName(t)
				now := fixedNow()
				leaseFor := 10 * time.Second

				rec := newRecord(t, q, time.Time{}, now)
				storedID, existed, err := d.Enqueue(bg(), rec)

				mustNoErr(t, err, "enqueue ready for job")

				if existed {
					t.Fatalf("enqueue ready job: expected existed=false, got true")
				}
				if storedID != rec.ID {
					t.Fatalf("enqueue ready job: expected storedID=%s, got %s", rec.ID, storedID)
				}

				got, lease, ok, err := d.Reserve(bg(), q, now, leaseFor)
				mustNoErr(t, err, "reserve ready job")
				if !ok {
					t.Fatalf("reserve ready job: expected ok=true, got false")
				}
				if got.ID != rec.ID {
					t.Fatalf("reserve ready job: expected id=%s, got %s", rec.ID, got.ID)
				}

				ackNow := now.Add(1 * time.Second)
				mustNoErr(
					t, d.Ack(bg(),
						got.ID, lease.Token,
						ackNow), "ack reserved job",
				)
			},
		},
		{
			Name: "schedule_blocks_before_due_then_reserves_after_due",
			Fn: func(t *testing.T, d driver.Driver) {
				q := uniqueQueueName(t)
				now := fixedNow()
				due := now.Add(2 * time.Second)
				before := due.Add(-1 * time.Nanosecond)
				leaseFor := 10 * time.Second

				// IMPORTANT: this must be scheduled (RunAt = due)
				rec := newRecord(t, q, due, now)

				_, existed, err := d.Enqueue(bg(), rec)
				mustNoErr(t, err, "enqueue scheduled job")
				if existed {
					t.Fatalf("enqueue scheduled job: expected existed=false")
				}

				// Before due: not reservable
				_, _, ok, err := d.Reserve(bg(), q, before, leaseFor)
				mustNoErr(t, err, "reserve before due")
				if ok {
					t.Fatalf("reserve before due: expected ok=false")
				}

				// At/after due: reservable
				got, lease, ok, err := d.Reserve(bg(), q, due, leaseFor)
				mustNoErr(t, err, "reserve at/after due")
				if !ok {
					t.Fatalf("reserve at/after due: expected ok=true, got false")
				}
				if got.ID != rec.ID {
					t.Fatalf("reserve at/after due: expected id=%s, got %s", rec.ID, got.ID)
				}

				mustNoErr(t, d.Ack(bg(), got.ID, lease.Token, due.Add(1*time.Second)), "ack scheduled job")
			},
		},
		{
			Name: "reserve_prefers_ready_over_future_scheduled",
			Fn: func(t *testing.T, d driver.Driver) {
				q := uniqueQueueName(t)
				now := fixedNow()
				due := now.Add(2 * time.Minute)
				leaseFor := 10 * time.Second

				scheduled := newRecord(t, q, due, now)
				ready := newRecord(t, q, time.Time{}, now)

				_, _, err := d.Enqueue(bg(), scheduled)
				mustNoErr(t, err, "enqueue scheduled")

				_, _, err = d.Enqueue(bg(), ready)
				mustNoErr(t, err, "enqueue ready")

				got1, lease1, ok, err := d.Reserve(bg(), q, now, leaseFor)
				mustNoErr(t, err, "reserve now")
				if !ok {
					t.Fatalf("reserve now: expected ok=true, got false")
				}
				if got1.ID != ready.ID {
					t.Fatalf("reserve now: expected ready id=%s, got %s", ready.ID, got1.ID)
				}
				mustNoErr(t, d.Ack(bg(), got1.ID, lease1.Token, now.Add(1*time.Second)), "ack ready")

				_, _, ok, err = d.Reserve(bg(), q, now, leaseFor)
				mustNoErr(t, err, "reserve now with only future scheduled remaining")
				if ok {
					t.Fatalf("reserve now with only future scheduled remaining: expected ok=false, got true")
				}

				got2, lease2, ok, err := d.Reserve(bg(), q, due, leaseFor)
				mustNoErr(t, err, "reserve after due")
				if !ok {
					t.Fatalf("reserve after due: expected ok=true, got false")
				}
				if got2.ID != scheduled.ID {
					t.Fatalf("reserve after due: expected scheduled id=%s, got %s", scheduled.ID, got2.ID)
				}
				mustNoErr(t, d.Ack(bg(), got2.ID, lease2.Token, due.Add(1*time.Second)), "ack scheduled")
			},
		},
	}

	return cases
}
