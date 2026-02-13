package taskharbor

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ARJ2211/taskharbor/taskharbor/driver"
	"github.com/ARJ2211/taskharbor/taskharbor/driver/memory"
)

func TestWorker_CrashRecovery_ReclaimsAfterLeaseExpiry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t0 := time.Date(2026, 1, 20, 10, 0, 0, 0, time.UTC)
	fc := NewFakeClock(t0)

	d := memory.New()

	rec := driver.JobRecord{
		ID:        "job-crash-1",
		Type:      "crash.job",
		Payload:   []byte(`{}`),
		Queue:     "default",
		CreatedAt: t0,
	}

	if _, _, err := d.Enqueue(ctx, rec); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	// Simulate worker A "crash":
	// reserve a job and then disappear without ack/fail/retry/heartbeat.
	_, _, ok, err := d.Reserve(ctx, "default", fc.Now(), 10*time.Second)
	if err != nil {
		t.Fatalf("reserve (crash sim) failed: %v", err)
	}
	if !ok {
		t.Fatalf("expected ok=true, got ok=false")
	}

	// Worker B should not be able to process it before lease expiry.
	w := NewWorker(
		d,
		WithDefaultQueue("default"),
		WithPollInterval(2*time.Millisecond),
		WithConcurrency(1),
		WithClock(fc),
		WithLeaseDuration(10*time.Second),
		WithHeartbeatInterval(2*time.Millisecond),
	)

	called := make(chan struct{}, 10)
	var n int32

	if err := w.Register("crash.job", func(ctx context.Context, job Job) error {
		atomic.AddInt32(&n, 1)
		called <- struct{}{}
		return nil
	}); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	// Give worker B a moment; it should NOT run the handler yet.
	select {
	case <-called:
		t.Fatalf("handler ran before lease expiry")
	case <-time.After(50 * time.Millisecond):
	}

	// Advance past lease expiry: job should be reclaimed and processed.
	fc.Advance(11 * time.Second)

	select {
	case <-called:
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for reclaimed job to be processed")
	}

	if got := atomic.LoadInt32(&n); got != 1 {
		t.Fatalf("expected handler to run once, got %d", got)
	}

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("worker returned error: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for worker shutdown")
	}
}
