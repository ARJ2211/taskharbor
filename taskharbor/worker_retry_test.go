package taskharbor

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ARJ2211/taskharbor/taskharbor/driver/memory"
)

func TestWorker_RetriesThenSucceeds(t *testing.T) {
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
		WithMaxAttempts(5),
	)

	w := NewWorker(
		d,
		WithDefaultQueue("default"),
		WithPollInterval(2*time.Millisecond),
		WithConcurrency(1),
		WithClock(fc),
		WithRetryPolicy(p),
	)

	calls := make(chan int, 10)
	var n int32

	_ = w.Register("job.retry", func(ctx context.Context, job Job) error {
		c := int(atomic.AddInt32(&n, 1))
		calls <- c
		if c == 1 {
			return errors.New("boom")
		}
		return nil
	})

	client := NewClient(d)
	_, err := client.Enqueue(ctx, JobRequest{Type: "job.retry", Payload: map[string]any{"x": 1}})
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	// first attempt
	select {
	case c := <-calls:
		if c != 1 {
			t.Fatalf("expected call 1, got %d", c)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for first attempt")
	}

	// wait until worker has scheduled the retry (job leaves inflight)
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if d.InflightSize("default") == 0 {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}
	if d.InflightSize("default") != 0 {
		t.Fatalf("timed out waiting for job to leave inflight (retry not scheduled yet)")
	}

	// retry becomes runnable
	fc.Advance(5 * time.Second)

	// second attempt
	select {
	case c := <-calls:
		if c != 2 {
			t.Fatalf("expected call 2, got %d", c)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for retry attempt")
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

	if len(d.DLQItems("default")) != 0 {
		t.Fatalf("expected empty DLQ, got %d", len(d.DLQItems("default")))
	}
}

func TestWorker_FailsToDLQAfterMaxAttempts(t *testing.T) {
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
		WithMaxAttempts(3), // total executions allowed = 3
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

	_ = w.Register("job.retry.dlq", func(ctx context.Context, job Job) error {
		c := int(atomic.AddInt32(&n, 1))
		calls <- c
		return errors.New("boom")
	})

	client := NewClient(d)
	_, err := client.Enqueue(ctx, JobRequest{
		Type:    "job.retry.dlq",
		Payload: map[string]any{"x": 1},
	})
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	// attempt #1
	select {
	case c := <-calls:
		if c != 1 {
			t.Fatalf("expected call 1, got %d", c)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for attempt 1")
	}

	// wait until worker has scheduled the retry (job leaves inflight)
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if d.InflightSize("default") == 0 {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}
	if d.InflightSize("default") != 0 {
		t.Fatalf("timed out waiting for job to leave inflight (retry not scheduled yet)")
	}

	// retry for attempt #2 (NextDelay(1) = 5s)
	fc.Advance(5 * time.Second)
	select {
	case c := <-calls:
		if c != 2 {
			t.Fatalf("expected call 2, got %d", c)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for attempt 2")
	}

	// wait until worker has scheduled the retry (job leaves inflight)
	deadline = time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if d.InflightSize("default") == 0 {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}
	if d.InflightSize("default") != 0 {
		t.Fatalf("timed out waiting for job to leave inflight (retry not scheduled yet)")
	}

	// retry for attempt #3 (NextDelay(2) = 10s)
	fc.Advance(10 * time.Second)
	select {
	case c := <-calls:
		if c != 3 {
			t.Fatalf("expected call 3, got %d", c)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for attempt 3")
	}

	// after the 3rd failure, it should DLQ (maxAttempts = 3)
	// give worker a moment to process Fail -> DLQ
	time.Sleep(10 * time.Millisecond)

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("worker returned error: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for worker shutdown")
	}

	items := d.DLQItems("default")
	if len(items) != 1 {
		t.Fatalf("expected DLQ size 1, got %d", len(items))
	}
}
