package memory

import (
	"container/heap"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strings"
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
	idemIndex     map[string]string
	closed        bool
}

// Compile time check that we implement contract.
var _ driver.Driver = (*Driver)(nil)

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
	inflight  map[string]inflightItem
	dlq       map[string]DLQItem
}

/*
Track the inflight items as record + lease.
*/
type inflightItem struct {
	rec   driver.JobRecord
	lease driver.Lease
}

/*
This allows the client to make a new driver
for the in-memory datastore.
*/
func New() *Driver {
	var driver Driver = Driver{
		queues:        make(map[string]*queueState),
		inflightIndex: make(map[string]string),
		idemIndex:     make(map[string]string),
	}
	return &driver
}

/*
This function will create an idem key
using queue + "\x00" + key
*/
func idemComposite(queue, key string) string {
	return queue + "\x00" + key
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
		inflight:  make(map[string]inflightItem),
		dlq:       make(map[string]DLQItem),
	}
	heap.Init(&qs.scheduled)

	d.queues[queue] = qs
	return qs
}

/*
This function will help
issue a new lease token.
*/
func newLeaseToken() (driver.LeaseToken, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return driver.LeaseToken(hex.EncodeToString(b)), nil
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
This function will reclaim explired leases inside
the Reserve function.
*/
func (d *Driver) reclaimExpiredLeaseLocked(qs *queueState, now time.Time) {
	for id, it := range qs.inflight {
		if !it.lease.ExpiresAt.After(now) {
			delete(qs.inflight, id)
			delete(d.inflightIndex, id)

			rec := it.rec
			// Make it run immediately
			rec.RunAt = time.Time{}
			qs.runnable = append(qs.runnable, rec)
		}
	}
}

/*
This function is responsible to enqueue a job into
the queue. We need to check the RunAt attribute to
add it to a scheduled heap or a basic FIFO queue.
*/
func (d *Driver) Enqueue(
	ctx context.Context,
	rec driver.JobRecord,
) (storedID string, existed bool, err error) {
	if err := ctx.Err(); err != nil {
		return "", false, err
	}

	if err := rec.Validate(); err != nil {
		return "", false, err
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return "", false, ErrDriverClosed
	}

	// Dedupe: (queue, idempotency_key)
	key := strings.TrimSpace(rec.IdempotencyKey)
	if key != "" {
		ck := idemComposite(rec.Queue, key)
		if existingID, ok := d.idemIndex[ck]; ok {
			// Duplicate enqueue: return original ID, do NOT enqueue again.
			return existingID, true, nil
		}
		// First time seeing this key: lock it to this job ID forever
		d.idemIndex[ck] = rec.ID
	}

	qs := d.getQueueLocked(rec.Queue)

	if rec.RunAt.IsZero() {
		qs.runnable = append(qs.runnable, rec)
		return rec.ID, false, nil
	}

	heap.Push(&qs.scheduled, rec)
	return rec.ID, false, nil
}

/*
This function is responsible to return one runnable
job if available. This is a non-blocking operation
ok=false means there is no runnable job currently.
*/
func (d *Driver) Reserve(
	ctx context.Context,
	queue string,
	now time.Time,
	leaseFor time.Duration) (
	driver.JobRecord, driver.Lease, bool, error,
) {
	if err := ctx.Err(); err != nil {
		return driver.JobRecord{}, driver.Lease{}, false, err
	}

	if leaseFor <= 0 {
		return driver.JobRecord{}, driver.Lease{}, false, driver.ErrInvalidLeaseDuration
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return driver.JobRecord{}, driver.Lease{}, false, ErrDriverClosed
	}

	qs := d.getQueueLocked(queue)
	d.reclaimExpiredLeaseLocked(qs, now)
	qs.promoteDueLocked(now)

	if len(qs.runnable) == 0 {
		return driver.JobRecord{}, driver.Lease{}, false, nil
	}

	rec := qs.runnable[0]
	qs.runnable = qs.runnable[1:]

	tok, err := newLeaseToken()
	if err != nil {
		return driver.JobRecord{}, driver.Lease{}, false, err
	}

	lease := driver.Lease{
		Token:     tok,
		ExpiresAt: now.Add(leaseFor),
	}

	qs.inflight[rec.ID] = inflightItem{
		rec:   rec,
		lease: lease,
	}
	d.inflightIndex[rec.ID] = queue

	return rec, lease, true, nil
}

/*
This function renews an extended lease for an inflight
job and returns the new lease.
*/
func (d *Driver) ExtendLease(
	ctx context.Context,
	id string,
	token driver.LeaseToken,
	now time.Time,
	leaseFor time.Duration) (driver.Lease, error,
) {
	if err := ctx.Err(); err != nil {
		return driver.Lease{}, err
	}

	if leaseFor <= 0 {
		return driver.Lease{}, driver.ErrInvalidLeaseDuration
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return driver.Lease{}, ErrDriverClosed
	}

	q, ok := d.inflightIndex[id]
	if !ok {
		return driver.Lease{}, driver.ErrJobNotInflight
	}

	qs := d.queues[q]
	if qs == nil {
		return driver.Lease{}, driver.ErrJobNotInflight
	}

	if _, ok := qs.inflight[id]; !ok {
		return driver.Lease{}, driver.ErrJobNotInflight
	}

	_, qs, it, err := d.getInflightLocked(id)

	if err != nil {
		return driver.Lease{}, err
	}

	if err := validateLease(it, token, now); err != nil {
		return driver.Lease{}, err
	}

	it.lease.ExpiresAt = now.Add(leaseFor)
	qs.inflight[id] = it
	return it.lease, nil
}

/*
This marks a job as completed.
*/
func (d *Driver) Ack(
	ctx context.Context,
	id string,
	token driver.LeaseToken,
	now time.Time,
) error {
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

	_, qs, it, err := d.getInflightLocked(id)
	if err != nil {
		return err
	}

	if err := validateLease(it, token, now); err != nil {
		return err
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
func (d *Driver) Retry(
	ctx context.Context,
	id string,
	token driver.LeaseToken,
	now time.Time,
	upd driver.RetryUpdate,
) error {
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

	_, qs, it, err := d.getInflightLocked(id)
	if err != nil {
		return err
	}

	if err := validateLease(it, token, now); err != nil {
		return err
	}

	delete(qs.inflight, id)
	delete(d.inflightIndex, id)

	rec := it.rec
	rec.RunAt = upd.RunAt
	rec.Attempts = upd.Attempts
	rec.LastError = upd.LastError
	rec.FailedAt = upd.FailedAt

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
func (d *Driver) Fail(
	ctx context.Context,
	id string,
	token driver.LeaseToken,
	now time.Time,
	reason string,
) error {
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

	_, qs, it, err := d.getInflightLocked(id)
	if err != nil {
		return err
	}

	if err := validateLease(it, token, now); err != nil {
		return err
	}

	delete(qs.inflight, id)
	delete(d.inflightIndex, id)

	qs.dlq[id] = DLQItem{
		Record:   it.rec,
		Reason:   reason,
		FailedAt: now.UTC(),
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
Validator to get inflight jobs (Locked) and
validate their lease.
*/
func (d *Driver) getInflightLocked(id string) (string, *queueState, inflightItem, error) {
	q, ok := d.inflightIndex[id]
	if !ok {
		return "", nil, inflightItem{}, driver.ErrJobNotInflight
	}
	qs := d.queues[q]
	if qs == nil {
		return "", nil, inflightItem{}, driver.ErrJobNotInflight
	}
	it, ok := qs.inflight[id]
	if !ok {
		return "", nil, inflightItem{}, driver.ErrJobNotInflight
	}
	return q, qs, it, nil
}

func validateLease(it inflightItem, token driver.LeaseToken, now time.Time) error {
	if !it.lease.ExpiresAt.After(now) {
		return driver.ErrLeaseExpired
	}
	if it.lease.Token != token {
		return driver.ErrLeaseMismatch
	}
	return nil
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
