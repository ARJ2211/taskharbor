package taskharbor

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ARJ2211/taskharbor/taskharbor/driver/memory"
)

/*
FakeClock is a controllable clock for tests.
*/
type FakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func NewFakeClock(t time.Time) *FakeClock {
	return &FakeClock{now: t}
}

func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func TestWorker_ScheduledJobRunsAfterRunAt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t0 := time.Date(2026, 1, 20, 10, 0, 0, 0, time.UTC)
	fc := NewFakeClock(t0)

	d := memory.New()

	w := NewWorker(
		d,
		WithDefaultQueue("default"),
		WithPollInterval(2*time.Millisecond),
		WithConcurrency(1),
		WithClock(fc),
	)

	called := make(chan struct{}, 1)

	err := w.Register("scheduled.job", func(ctx context.Context, job Job) error {
		called <- struct{}{}
		return nil
	})
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}

	client := NewClient(d)

	runAt := t0.Add(10 * time.Second)

	_, err = client.Enqueue(ctx, JobRequest{
		Type:    "scheduled.job",
		Payload: map[string]any{"x": 1},
		Queue:   "default",
		RunAt:   runAt,
	})
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- w.Run(ctx)
	}()

	// Confirm it does NOT run early
	select {
	case <-called:
		t.Fatalf("job ran before RunAt")
	case <-time.After(30 * time.Millisecond):
	}

	// Advance time past RunAt and confirm it DOES run
	fc.Advance(10 * time.Second)

	select {
	case <-called:
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for scheduled job to run")
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
