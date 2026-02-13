package taskharbor

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ARJ2211/taskharbor/taskharbor/driver"
	"github.com/ARJ2211/taskharbor/taskharbor/driver/memory"
)

type leaseFailDriver struct {
	inner driver.Driver

	failAfter int

	mu          sync.Mutex
	extendCalls int
}

func (d *leaseFailDriver) Enqueue(ctx context.Context, rec driver.JobRecord) (string, bool, error) {
	return d.inner.Enqueue(ctx, rec)
}

func (d *leaseFailDriver) Reserve(ctx context.Context, queue string, now time.Time, leaseFor time.Duration) (driver.JobRecord, driver.Lease, bool, error) {
	return d.inner.Reserve(ctx, queue, now, leaseFor)
}

func (d *leaseFailDriver) ExtendLease(ctx context.Context, id string, token driver.LeaseToken, now time.Time, leaseFor time.Duration) (driver.Lease, error) {
	d.mu.Lock()
	d.extendCalls++
	calls := d.extendCalls
	d.mu.Unlock()

	if calls >= d.failAfter {
		return driver.Lease{}, driver.ErrLeaseExpired
	}
	return d.inner.ExtendLease(ctx, id, token, now, leaseFor)
}

func (d *leaseFailDriver) Ack(ctx context.Context, id string, token driver.LeaseToken, now time.Time) error {
	return d.inner.Ack(ctx, id, token, now)
}

func (d *leaseFailDriver) Retry(ctx context.Context, id string, token driver.LeaseToken, now time.Time, upd driver.RetryUpdate) error {
	return d.inner.Retry(ctx, id, token, now, upd)
}

func (d *leaseFailDriver) Fail(ctx context.Context, id string, token driver.LeaseToken, now time.Time, reason string) error {
	return d.inner.Fail(ctx, id, token, now, reason)
}

func (d *leaseFailDriver) Close() error {
	return d.inner.Close()
}

func TestWorker_HeartbeatCancelsHandlerOnLeaseLoss(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	inner := memory.New()
	d := &leaseFailDriver{
		inner:     inner,
		failAfter: 1, // first heartbeat fails -> cancel handler
	}

	w := NewWorker(
		d,
		WithDefaultQueue("default"),
		WithConcurrency(1),
		WithPollInterval(5*time.Millisecond),
		WithLeaseDuration(50*time.Millisecond),
		WithHeartbeatInterval(5*time.Millisecond),
	)

	canceled := make(chan struct{}, 1)

	if err := w.Register("lease.job", func(ctx context.Context, job Job) error {
		select {
		case <-ctx.Done():
			canceled <- struct{}{}
			return ctx.Err()
		case <-time.After(300 * time.Millisecond):
			return nil
		}
	}); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	rec := driver.JobRecord{
		ID:        "job-lease-1",
		Type:      "lease.job",
		Payload:   []byte(`{}`),
		Queue:     "default",
		CreatedAt: time.Now().UTC(),
	}

	if _, _, err := d.Enqueue(ctx, rec); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	select {
	case <-canceled:
		cancel()
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for handler context cancellation")
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("worker run returned error: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for worker shutdown")
	}

	d.mu.Lock()
	calls := d.extendCalls
	d.mu.Unlock()

	if calls == 0 {
		t.Fatalf("expected ExtendLease to be called at least once")
	}
}
