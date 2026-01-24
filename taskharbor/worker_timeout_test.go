package taskharbor

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ARJ2211/taskharbor/taskharbor/driver/memory"
)

func recvAttemptWithTimeout(t *testing.T, calls <-chan int, want int, timeout time.Duration) {
	t.Helper()
	select {
	case got := <-calls:
		if got != want {
			t.Fatalf("expected attempt %d, got %d", want, got)
		}
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for attempt %d", want)
	}
}

func waitForDLQSizeTimeout(t *testing.T, d *memory.Driver, queue string, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if len(d.DLQItems(queue)) == want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("expected DLQ size %d, got %d", want, len(d.DLQItems(queue)))
}

func TestWorker_TimeoutRetriesThenSucceeds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t0 := time.Date(2026, 1, 20, 10, 0, 0, 0, time.UTC)
	fc := NewFakeClock(t0)

	d := memory.New()

	// BaseDelay=0 => immediate retry (no fake clock coordination needed).
	p := NewExponentialBackoffPolicy(
		0,
		0,
		2.0,
		0.0,
		WithMaxAttempts(5),
	)

	w := NewWorker(
		d,
		WithDefaultQueue("default"),
		WithPollInterval(1*time.Millisecond),
		WithConcurrency(1),
		WithClock(fc),
		WithRetryPolicy(p),
	)

	var n int32
	calls := make(chan int, 10)

	if err := w.Register("job.timeout.retry", func(ctx context.Context, job Job) error {
		c := int(atomic.AddInt32(&n, 1))
		calls <- c

		if c == 1 {
			// First attempt: wait for timeout cancellation
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	}); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	client := NewClient(d)
	if _, err := client.Enqueue(ctx, JobRequest{
		Type:    "job.timeout.retry",
		Queue:   "default",
		Timeout: 20 * time.Millisecond,
		Payload: map[string]any{"x": 1},
	}); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	recvAttemptWithTimeout(t, calls, 1, 1*time.Second)
	recvAttemptWithTimeout(t, calls, 2, 1*time.Second)

	// give time for ack to happen
	time.Sleep(10 * time.Millisecond)

	if got := len(d.DLQItems("default")); got != 0 {
		t.Fatalf("expected empty DLQ, got %d", got)
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

func TestWorker_TimeoutFailsToDLQAfterMaxAttempts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t0 := time.Date(2026, 1, 20, 10, 0, 0, 0, time.UTC)
	fc := NewFakeClock(t0)

	d := memory.New()

	// maxAttempts=3 means 3 executions total.
	p := NewExponentialBackoffPolicy(
		0,
		0,
		2.0,
		0.0,
		WithMaxAttempts(3),
	)

	w := NewWorker(
		d,
		WithDefaultQueue("default"),
		WithPollInterval(1*time.Millisecond),
		WithConcurrency(1),
		WithClock(fc),
		WithRetryPolicy(p),
	)

	var n int32
	calls := make(chan int, 10)

	if err := w.Register("job.timeout.dlq", func(ctx context.Context, job Job) error {
		c := int(atomic.AddInt32(&n, 1))
		calls <- c

		<-ctx.Done()
		// If handler ignores ctx, this would hang forever.
		// This test validates cooperative cancellation.
		if ctx.Err() == nil {
			return errors.New("expected ctx.Err() on timeout")
		}
		return ctx.Err()
	}); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	client := NewClient(d)
	if _, err := client.Enqueue(ctx, JobRequest{
		Type:    "job.timeout.dlq",
		Queue:   "default",
		Timeout: 15 * time.Millisecond,
		Payload: map[string]any{"x": 1},
	}); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	recvAttemptWithTimeout(t, calls, 1, 1*time.Second)
	recvAttemptWithTimeout(t, calls, 2, 1*time.Second)
	recvAttemptWithTimeout(t, calls, 3, 1*time.Second)

	waitForDLQSizeTimeout(t, d, "default", 1, 1*time.Second)

	// ensure no extra attempts happen
	select {
	case got := <-calls:
		t.Fatalf("unexpected extra attempt %d", got)
	case <-time.After(25 * time.Millisecond):
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
