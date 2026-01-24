package taskharbor

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ARJ2211/taskharbor/taskharbor/driver/memory"
)

func recvAttempt(t *testing.T, calls <-chan int, want int) {
	t.Helper()
	select {
	case got := <-calls:
		if got != want {
			t.Fatalf("expected attempt %d, got %d", want, got)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for attempt %d", want)
	}
}

func waitForInflightSize(t *testing.T, d *memory.Driver, queue string, want int) {
	t.Helper()
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if d.InflightSize(queue) == want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for inflight size %d (got %d)", want, d.InflightSize(queue))
}

func waitForDLQSize(t *testing.T, d *memory.Driver, queue string, want int) {
	t.Helper()
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if len(d.DLQItems(queue)) == want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("expected DLQ size %d, got %d", want, len(d.DLQItems(queue)))
}

func TestWorker_PanicRetriesThenDLQ(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t0 := time.Date(2026, 1, 20, 10, 0, 0, 0, time.UTC)
	fc := NewFakeClock(t0)

	d := memory.New()

	p := NewExponentialBackoffPolicy(
		5*time.Second,
		1*time.Minute,
		2.0,
		0.0,
		WithMaxAttempts(3),
	)

	w := NewWorker(
		d,
		WithDefaultQueue("default"),
		WithPollInterval(2*time.Millisecond),
		WithConcurrency(1),
		WithClock(fc),
		WithRetryPolicy(p),
	)

	var n int32
	calls := make(chan int, 10)

	if err := w.Register("job.panic.dlq", func(ctx context.Context, job Job) error {
		c := int(atomic.AddInt32(&n, 1))
		calls <- c
		panic("boom")
	}); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	client := NewClient(d)
	if _, err := client.Enqueue(ctx, JobRequest{
		Type:    "job.panic.dlq",
		Queue:   "default",
		Payload: map[string]any{"x": 1},
	}); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	// attempt 1
	recvAttempt(t, calls, 1)

	// wait until worker persists retry (job leaves inflight)
	waitForInflightSize(t, d, "default", 0)

	// attempt 2 becomes due after 5s
	fc.Advance(5 * time.Second)
	recvAttempt(t, calls, 2)

	waitForInflightSize(t, d, "default", 0)

	// attempt 3 becomes due after 10s
	fc.Advance(10 * time.Second)
	recvAttempt(t, calls, 3)

	// after 3rd failure, worker should DLQ (maxAttempts=3)
	waitForInflightSize(t, d, "default", 0)
	waitForDLQSize(t, d, "default", 1)

	// ensure no 4th attempt happens
	select {
	case got := <-calls:
		t.Fatalf("unexpected extra attempt %d", got)
	case <-time.After(25 * time.Millisecond):
	}

	// stop worker
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
