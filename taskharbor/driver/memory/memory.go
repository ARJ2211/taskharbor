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
	return driver.JobRecord{}, true, nil
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
