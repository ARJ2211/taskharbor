package memory

import (
	"container/heap"
	"context"
	"errors"
	"sync"
	"time"

	"github.com/ARJ2211/taskharbor/taskharbor/driver"
)

/*
This is the in-memory driver backend for TaskHarbor

Data model used per Queue:
--------------------------
  - runnable: FIFO slice
  - scheduled: min-heap ordered by run-at
  - inflight: map of ongoing Jobs that have not ack'd/failed
  - dql: map of failed jobs
*/
type Driver struct {
	mu            sync.Mutex
	queues        map[string]*queueState
	inflightIndex map[string]string
	closed        bool
}

/*
This is a record stored in the Dead Letter Queue.
*/
type DLQItem struct {
	Record   driver.JobRecord
	Reason   string
	FailedAt time.Time
}

/*
Internal per-queue state.
*/
type queueState struct {
	runnable  []driver.JobRecord
	scheduled scheduledHeap
	inflight  map[string]driver.JobRecord
	dlq       map[string]DLQItem
}

/*
This allows the client to make a new driver
for the in-memory datastore.
*/
func New() *Driver {
	var driver Driver = Driver{
		queues:        make(map[string]*queueState),
		inflightIndex: make(map[string]string),
	}
	return &driver
}

/*
This is required to fetch a queueState or creates it if
it does not exist. Caller must host the mutex lock.
*/
func (d *Driver) getQueueLocked(queue string) *queueState {
	qs, ok := d.queues[queue]
	if ok {
		return qs
	}

	qs = &queueState{
		runnable:  make([]driver.JobRecord, 0),
		scheduled: make(scheduledHeap, 0),
		inflight:  make(map[string]driver.JobRecord),
		dlq:       make(map[string]DLQItem),
	}
	heap.Init(&qs.scheduled)

	d.queues[queue] = qs
	return qs
}

/*
This function is required to promote due scheduled
jobs into runnable. Called must hold the mutex lock.
*/
func (qs *queueState) promoteDueLocked(now time.Time) {
	for qs.scheduled.Len() > 0 {
		next := qs.scheduled[0]
		if next.RunAt.After(now) {
			return
		}
		rec := heap.Pop(&qs.scheduled).(driver.JobRecord)
		qs.runnable = append(qs.runnable, rec)
	}
}

/*
This function is responsible to enqueue a job into
the queue. We need to check the RunAt attribute to
add it to a scheduled heap or a basic FIFO queue.
*/
func (d *Driver) Enqueue(ctx context.Context, rec driver.JobRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if err := rec.Validate(); err != nil {
		return err
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return ErrDriverClosed
	}

	qs := d.getQueueLocked(rec.Queue)

	if rec.RunAt.IsZero() {
		qs.runnable = append(qs.runnable, rec)
		return nil
	}

	heap.Push(&qs.scheduled, rec)
	return nil
}

/*
This function is responsible to return one runnable
job if available. This is a non-blocking operation
ok=false means there is no runnable job currently.
*/
func (d *Driver) Reserve(
	ctx context.Context, queue string, now time.Time) (
	driver.JobRecord, bool, error,
) {
	if err := ctx.Err(); err != nil {
		return driver.JobRecord{}, false, err
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return driver.JobRecord{}, false, ErrDriverClosed
	}

	qs := d.getQueueLocked(queue)
	qs.promoteDueLocked(now)

	if len(qs.runnable) == 0 {
		return driver.JobRecord{}, false, nil
	}

	rec := qs.runnable[0]
	qs.runnable = qs.runnable[1:]

	qs.inflight[rec.ID] = rec
	d.inflightIndex[rec.ID] = queue

	return rec, true, nil
}

/*
This marks a job as completed.
*/
func (d *Driver) Ack(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return ErrDriverClosed
	}

	q, ok := d.inflightIndex[id]
	if !ok {
		return driver.ErrJobNotInflight
	}

	qs, ok := d.queues[q]
	if !ok {
		return driver.ErrJobNotInflight
	}

	if _, ok := qs.inflight[id]; !ok {
		return driver.ErrJobNotInflight
	}

	delete(qs.inflight, id)
	delete(d.inflightIndex, id)

	return nil
}

/*
This moves a reserved job back into the runnable/scheduled queue and updates
the failiure metadata. THIS IS JUST TO TRACK AND RECHEDULE THE RETRY. THE DRIVER
SHOULD NOT IMPLEMENT ANY RETRY POLICIES.
*/
func (d *Driver) Retry(ctx context.Context, id string, upd driver.RetryUpdate) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return ErrDriverClosed
	}

	if d.closed {
		return ErrDriverClosed
	}

	q, ok := d.inflightIndex[id]
	if !ok {
		return driver.ErrJobNotInflight
	}

	qs, ok := d.queues[q]
	if !ok {
		return driver.ErrJobNotInflight
	}

	rec, ok := qs.inflight[id]
	if !ok {
		return driver.ErrJobNotInflight
	}

	delete(qs.inflight, id)
	delete(d.inflightIndex, id)

	rec.RunAt = upd.RunAt
	rec.Attempts = upd.Attempts
	rec.LastError = upd.LastError
	rec.FailedAt = upd.FailedAt

	// ReQueue it based on the runAt. If RunAt is in the PAST
	// promote due locked will move it immidiately during reserve
	// to the queue
	if rec.RunAt.IsZero() {
		qs.runnable = append(qs.runnable, rec)
		return nil
	}
	heap.Push(&qs.scheduled, rec)
	return nil
}

/*
This marks a reserved job as failed and moves it to DLQ storage.
*/
func (d *Driver) Fail(ctx context.Context, id string, reason string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return ErrDriverClosed
	}

	q, ok := d.inflightIndex[id]
	if !ok {
		return driver.ErrJobNotInflight
	}

	qs := d.queues[q]
	if qs == nil {
		return driver.ErrJobNotInflight
	}

	rec, ok := qs.inflight[id]
	if !ok {
		return driver.ErrJobNotInflight
	}

	delete(qs.inflight, id)
	delete(d.inflightIndex, id)

	qs.dlq[id] = DLQItem{
		Record:   rec,
		Reason:   reason,
		FailedAt: time.Now().UTC(),
	}

	return nil
}

/*
Close marks the driver closed. Further operations return ErrDriverClosed.
*/
func (d *Driver) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.closed = true
	return nil
}

/*
DLQItems returns a copy of DLQ items for a queue.
This is mainly useful for tests and debugging.
*/
func (d *Driver) DLQItems(queue string) []DLQItem {
	d.mu.Lock()
	defer d.mu.Unlock()

	qs := d.queues[queue]
	if qs == nil {
		return nil
	}

	out := make([]DLQItem, 0, len(qs.dlq))
	for _, item := range qs.dlq {
		out = append(out, item)
	}
	return out
}

/*
InflightSize returns number of inflight jobs for a queue.
This is mainly useful for tests and debugging.
*/
func (d *Driver) InflightSize(queue string) int {
	d.mu.Lock()
	defer d.mu.Unlock()

	qs := d.queues[queue]
	if qs == nil {
		return 0
	}
	return len(qs.inflight)
}

/*
RunnableSize returns number of runnable jobs for a queue.
This is mainly useful for tests and debugging.
*/
func (d *Driver) RunnableSize(queue string) int {
	d.mu.Lock()
	defer d.mu.Unlock()

	qs := d.queues[queue]
	if qs == nil {
		return 0
	}
	return len(qs.runnable)
}

/*
scheduledHeap is a min-heap ordered by RunAt.
*/
type scheduledHeap []driver.JobRecord

func (h scheduledHeap) Len() int {
	return len(h)
}

func (h scheduledHeap) Less(i, j int) bool {
	ri := h[i]
	rj := h[j]

	if ri.RunAt.Before(rj.RunAt) {
		return true
	}
	if ri.RunAt.After(rj.RunAt) {
		return false
	}

	if ri.CreatedAt.Before(rj.CreatedAt) {
		return true
	}
	if ri.CreatedAt.After(rj.CreatedAt) {
		return false
	}

	return ri.ID < rj.ID
}

func (h scheduledHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *scheduledHeap) Push(x any) {
	*h = append(*h, x.(driver.JobRecord))
}

func (h *scheduledHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// Errors
var (
	ErrDriverClosed = errors.New("driver is closed")
)
