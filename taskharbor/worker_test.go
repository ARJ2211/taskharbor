package taskharbor

import (
	"context"
	"testing"
	"time"

	"github.com/ARJ2211/taskharbor/taskharbor/driver"
	"github.com/ARJ2211/taskharbor/taskharbor/driver/memory"
)

func TestWorker_ProcessesEnqueuedJob(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d := memory.New()

	w := NewWorker(d, WithPollInterval(5*time.Millisecond), WithConcurrency(2), WithDefaultQueue("default"))

	called := make(chan struct{}, 1)

	err := w.Register("test.job", func(ctx context.Context, job Job) error {
		if job.Type != "test.job" {
			t.Fatalf("expected job type test.job, got %s", job.Type)
		}
		if len(job.Payload) == 0 {
			t.Fatalf("expected payload bytes, got empty")
		}
		called <- struct{}{}
		return nil
	})
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}

	rec := driver.JobRecord{
		ID:        "job-1",
		Type:      "test.job",
		Payload:   []byte(`{"x":1}`),
		Queue:     "default",
		RunAt:     time.Time{},
		CreatedAt: time.Now().UTC(),
	}

	if err := d.Enqueue(ctx, rec); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- w.Run(ctx)
	}()

	select {
	case <-called:
		cancel()
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for handler to run")
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("worker run returned error: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for worker shutdown")
	}
}

func TestWorker_GracefulShutdownWaitsForInflight(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d := memory.New()

	w := NewWorker(d, WithPollInterval(5*time.Millisecond), WithConcurrency(1), WithDefaultQueue("default"))

	started := make(chan struct{}, 1)
	unblock := make(chan struct{})

	err := w.Register("block.job", func(ctx context.Context, job Job) error {
		started <- struct{}{}
		<-unblock
		return nil
	})
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}

	rec := driver.JobRecord{
		ID:        "job-2",
		Type:      "block.job",
		Payload:   []byte(`{}`),
		Queue:     "default",
		RunAt:     time.Time{},
		CreatedAt: time.Now().UTC(),
	}

	if err := d.Enqueue(ctx, rec); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- w.Run(ctx)
	}()

	select {
	case <-started:
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for handler to start")
	}

	cancel()

	select {
	case <-done:
		t.Fatalf("worker exited before inflight handler finished")
	case <-time.After(50 * time.Millisecond):
	}

	close(unblock)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("worker run returned error: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for worker shutdown")
	}
}
