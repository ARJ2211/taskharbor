package memory

import (
	"container/heap"
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/ARJ2211/taskharbor/taskharbor/driver"
)

var _ driver.Admin = (*Driver)(nil)

func (d *Driver) Inspect(ctx context.Context, id string, now time.Time) (driver.JobInfo, error) {
	if err := ctx.Err(); err != nil {
		return driver.JobInfo{}, err
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return driver.JobInfo{}, driver.ErrJobNotFound
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return driver.JobInfo{}, ErrDriverClosed
	}

	// inflight (fast path)
	if q, ok := d.inflightIndex[id]; ok {
		qs := d.queues[q]
		if qs != nil {
			if it, ok := qs.inflight[id]; ok {
				return driver.JobInfo{
					Record: it.rec,
					State:  driver.StateInflight,
					Lease: &driver.LeaseInfo{
						Token:     it.lease.Token,
						ExpiresAt: it.lease.ExpiresAt,
					},
				}, nil
			}
		}
	}

	// dlq (fast path)
	if q, ok := d.dlqIndex[id]; ok {
		qs := d.queues[q]
		if qs != nil {
			if item, ok := qs.dlq[id]; ok {
				return driver.JobInfo{
					Record: item.Record,
					State:  driver.StateDLQ,
					DLQ: &driver.DLQInfo{
						Reason:   item.Reason,
						FailedAt: item.FailedAt,
					},
				}, nil
			}
		}
	}

	// done (fast path)
	if _, ok := d.doneIndex[id]; ok {
		if rec, ok2 := d.doneRecords[id]; ok2 {
			return driver.JobInfo{Record: rec, State: driver.StateDone}, nil
		}
		// Shouldn't happen after we store doneRecords, but keep a sane error.
		return driver.JobInfo{}, driver.ErrJobNotFound
	}

	// ready/scheduled scan (no index)
	for _, qs := range d.queues {
		if qs == nil {
			continue
		}
		for _, rec := range qs.runnable {
			if rec.ID == id {
				return driver.JobInfo{Record: rec, State: stateFromRunAt(now, rec.RunAt)}, nil
			}
		}
		for _, rec := range qs.scheduled {
			if rec.ID == id {
				return driver.JobInfo{Record: rec, State: stateFromRunAt(now, rec.RunAt)}, nil
			}
		}
	}

	return driver.JobInfo{}, driver.ErrJobNotFound
}

func (d *Driver) List(ctx context.Context, req driver.ListRequest) (driver.ListPage, error) {
	if err := ctx.Err(); err != nil {
		return driver.ListPage{}, err
	}
	req.Queue = strings.TrimSpace(req.Queue)
	if req.Queue == "" {
		return driver.ListPage{}, fmt.Errorf("queue is required")
	}
	if req.Now.IsZero() {
		req.Now = time.Now().UTC()
	}
	if req.Limit <= 0 {
		req.Limit = 50
	}

	var cur driver.Cursor
	if strings.TrimSpace(req.Cursor) != "" {
		c, err := driver.DecodeCursor(req.Cursor)
		if err != nil {
			return driver.ListPage{}, err
		}
		cur = c
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return driver.ListPage{}, ErrDriverClosed
	}

	qs := d.queues[req.Queue]
	if qs == nil {
		return driver.ListPage{Jobs: nil, NextCursor: ""}, nil
	}

	jobs := make([]driver.JobSummary, 0)

	addIf := func(s driver.JobSummary) {
		if req.State == "" || s.State == req.State {
			jobs = append(jobs, s)
		}
	}

	// runnable
	for _, rec := range qs.runnable {
		addIf(summaryFromRecord(req.Now, rec, nil, nil))
	}
	// scheduled heap contents
	for _, rec := range qs.scheduled {
		addIf(summaryFromRecord(req.Now, rec, nil, nil))
	}
	// inflight
	for _, it := range qs.inflight {
		lease := &driver.LeaseInfo{Token: it.lease.Token, ExpiresAt: it.lease.ExpiresAt}
		addIf(summaryFromRecord(req.Now, it.rec, lease, nil))
	}
	// dlq
	for _, item := range qs.dlq {
		dlq := &driver.DLQInfo{Reason: item.Reason, FailedAt: item.FailedAt}
		addIf(summaryFromRecord(req.Now, item.Record, nil, dlq))
	}
	// done (global map, filter by queue)
	for _, rec := range d.doneRecords {
		if rec.Queue != req.Queue {
			continue
		}
		addIf(summaryFromRecord(req.Now, rec, nil, nil))
	}

	// stable ordering: (created_at asc, id asc)
	sort.Slice(jobs, func(i, j int) bool {
		ai := jobs[i].CreatedAt.UnixNano()
		aj := jobs[j].CreatedAt.UnixNano()
		if ai != aj {
			return ai < aj
		}
		return jobs[i].ID < jobs[j].ID
	})

	// apply cursor filter
	if strings.TrimSpace(req.Cursor) != "" {
		out := jobs[:0]
		for _, s := range jobs {
			a := s.CreatedAt.UnixNano()
			if a > cur.A || (a == cur.A && s.ID > cur.ID) {
				out = append(out, s)
			}
		}
		jobs = out
	}

	if len(jobs) == 0 {
		return driver.ListPage{Jobs: nil, NextCursor: ""}, nil
	}

	if len(jobs) <= req.Limit {
		return driver.ListPage{Jobs: jobs, NextCursor: ""}, nil
	}

	page := jobs[:req.Limit]
	last := page[len(page)-1]
	next := driver.EncodeCursor(driver.Cursor{
		State: req.State,
		A:     last.CreatedAt.UnixNano(),
		ID:    last.ID,
	})

	return driver.ListPage{Jobs: page, NextCursor: next}, nil
}

func (d *Driver) RequeueDLQ(ctx context.Context, id string, now time.Time, opt driver.RequeueOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return driver.ErrJobNotFound
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return ErrDriverClosed
	}

	q, ok := d.dlqIndex[id]
	if !ok {
		// If job exists elsewhere, be explicit.
		if _, ok := d.inflightIndex[id]; ok {
			return driver.ErrJobNotDLQ
		}
		if _, ok := d.doneIndex[id]; ok {
			return driver.ErrJobNotDLQ
		}
		for _, qs := range d.queues {
			if qs == nil {
				continue
			}
			for _, rec := range qs.runnable {
				if rec.ID == id {
					return driver.ErrJobNotDLQ
				}
			}
			for _, rec := range qs.scheduled {
				if rec.ID == id {
					return driver.ErrJobNotDLQ
				}
			}
		}
		return driver.ErrJobNotFound
	}

	if opt.Queue != "" && opt.Queue != q {
		return fmt.Errorf("queue mismatch: job is in %q, not %q", q, opt.Queue)
	}

	qs := d.queues[q]
	if qs == nil {
		return driver.ErrJobNotFound
	}

	item, ok := qs.dlq[id]
	if !ok {
		return driver.ErrJobNotFound
	}

	delete(qs.dlq, id)
	delete(d.dlqIndex, id)

	rec := item.Record
	rec.Queue = q
	rec.RunAt = opt.RunAt // zero => immediate

	if opt.ResetAttempts {
		rec.Attempts = 0
		rec.LastError = ""
		rec.FailedAt = time.Time{}
	}

	if rec.RunAt.IsZero() || !rec.RunAt.After(now) {
		rec.RunAt = time.Time{}
		qs.runnable = append(qs.runnable, rec)
		return nil
	}

	heap.Push(&qs.scheduled, rec)
	return nil
}

func stateFromRunAt(now, runAt time.Time) driver.JobState {
	if runAt.IsZero() || !runAt.After(now) {
		return driver.StateReady
	}
	return driver.StateScheduled
}

func summaryFromRecord(now time.Time, rec driver.JobRecord, lease *driver.LeaseInfo, dlq *driver.DLQInfo) driver.JobSummary {
	s := driver.JobSummary{
		ID:          rec.ID,
		Type:        rec.Type,
		Queue:       rec.Queue,
		RunAt:       rec.RunAt,
		CreatedAt:   rec.CreatedAt,
		Timeout:     rec.Timeout,
		Attempts:    rec.Attempts,
		MaxAttempts: rec.MaxAttempts,
		LastError:   rec.LastError,
		FailedAt:    rec.FailedAt,
		State:       stateFromRunAt(now, rec.RunAt),
	}

	if lease != nil {
		s.State = driver.StateInflight
		s.LeaseExpiresAt = lease.ExpiresAt
	}
	if dlq != nil {
		s.State = driver.StateDLQ
		s.DLQReason = dlq.Reason
		s.DLQFailedAt = dlq.FailedAt
	}

	// done is inferred later via state override in List when needed,
	// but for memory we don’t have a separate “done container” per queue.
	// Inspect sets done explicitly.
	return s
}
