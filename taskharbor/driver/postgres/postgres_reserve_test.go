package postgres

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ARJ2211/taskharbor/taskharbor/driver"
	"github.com/ARJ2211/taskharbor/taskharbor/internal/envutil"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestReserve_NoJobs(t *testing.T) {
	_ = envutil.LoadRepoDotenv(".")
	dsn := os.Getenv("TASKHARBOR_TEST_DSN")
	if dsn == "" {
		t.Skip("TASKHARBOR_TEST_DSN not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	defer pool.Close()

	if err := ApplyMigrations(ctx, pool); err != nil {
		t.Fatalf("ApplyMigrations: %v", err)
	}
	if _, err := pool.Exec(ctx, `TRUNCATE th_jobs`); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	d, err := NewWithPool(pool)
	if err != nil {
		t.Fatalf("NewWithPool: %v", err)
	}

	_, _, ok, err := d.Reserve(ctx, "default", time.Now().UTC(), 5*time.Second)
	if err != nil {
		t.Fatalf("Reserve err: %v", err)
	}
	if ok {
		t.Fatalf("expected ok=false")
	}
}

func TestReserve_NoDoubleReserve_AndReclaim(t *testing.T) {
	_ = envutil.LoadRepoDotenv(".")
	dsn := os.Getenv("TASKHARBOR_TEST_DSN")
	if dsn == "" {
		t.Skip("TASKHARBOR_TEST_DSN not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	defer pool.Close()

	if err := ApplyMigrations(ctx, pool); err != nil {
		t.Fatalf("ApplyMigrations: %v", err)
	}
	if _, err := pool.Exec(ctx, `TRUNCATE th_jobs`); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	d, err := NewWithPool(pool)
	if err != nil {
		t.Fatalf("NewWithPool: %v", err)
	}

	now1 := time.Now().UTC()

	// Due scheduled job (run_at in the past) so first reserve returns a non-zero RunAt.
	rec := driver.JobRecord{
		ID:          "job_sched_1",
		Type:        "t",
		Queue:       "default",
		Payload:     []byte(`{}`),
		RunAt:       now1.Add(-1 * time.Minute),
		Timeout:     1 * time.Second,
		CreatedAt:   now1,
		Attempts:    0,
		MaxAttempts: 3,
	}

	if _, _, err := d.Enqueue(ctx, rec); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	r1, l1, ok, err := d.Reserve(ctx, "default", now1, 1*time.Second)
	if err != nil {
		t.Fatalf("Reserve#1: %v", err)
	}
	if !ok {
		t.Fatalf("expected ok=true")
	}
	if r1.ID != rec.ID {
		t.Fatalf("expected %s got %s", rec.ID, r1.ID)
	}
	if r1.RunAt.IsZero() {
		t.Fatalf("expected first reserve to keep original run_at (scheduled due)")
	}

	// second reserve while lease valid should return ok=false
	_, _, ok2, err := d.Reserve(ctx, "default", now1, 1*time.Second)
	if err != nil {
		t.Fatalf("Reserve#2: %v", err)
	}
	if ok2 {
		t.Fatalf("expected ok=false while lease is valid")
	}

	// reclaim after lease expiry (advance 'now' manually)
	now2 := now1.Add(2 * time.Second)
	r2, l2, ok3, err := d.Reserve(ctx, "default", now2, 1*time.Second)
	if err != nil {
		t.Fatalf("Reserve#3 (reclaim): %v", err)
	}
	if !ok3 {
		t.Fatalf("expected ok=true after expiry")
	}
	if r2.ID != rec.ID {
		t.Fatalf("expected reclaimed job id %s got %s", rec.ID, r2.ID)
	}
	if !r2.RunAt.IsZero() {
		t.Fatalf("expected reclaim to clear run_at to zero (NULL in DB), got %v", r2.RunAt)
	}
	if l1.Token == l2.Token {
		t.Fatalf("expected new lease token after reclaim")
	}
	if !l2.ExpiresAt.After(now2) {
		t.Fatalf("expected lease expiry after now2")
	}
}

func TestReserve_Concurrency_NoDoubleLease(t *testing.T) {
	pool, ctx, cancel := newTestPool(t)
	defer cancel()
	defer pool.Close()

	d, err := NewWithPool(pool)
	if err != nil {
		t.Fatalf("NewWithPool: %v", err)
	}

	if _, err := pool.Exec(ctx, `DELETE FROM th_jobs`); err != nil {
		t.Fatalf("cleanup: %v", err)
	}

	const (
		queue     = "default"
		totalJobs = 2000
		workers   = 32
		leaseFor  = 200 * time.Millisecond
	)

	t0 := time.Now().UTC().Truncate(time.Microsecond)

	for i := 0; i < totalJobs; i++ {
		id := fmt.Sprintf("conc_reserve_%d_%d", time.Now().UnixNano(), i)
		rec := newJobRecord(id, t0)
		rec.Queue = queue
		if _, _, err := d.Enqueue(ctx, rec); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	var inFlight sync.Map // jobID -> true (currently leased)
	var doneCount int64

	start := make(chan struct{})
	errCh := make(chan error, workers)

	var wg sync.WaitGroup
	wg.Add(workers)

	for w := 0; w < workers; w++ {
		go func(workerNum int) {
			defer wg.Done()
			<-start

			for {
				if ctx.Err() != nil {
					return
				}
				if atomic.LoadInt64(&doneCount) >= int64(totalJobs) {
					return
				}

				now := time.Now().UTC().Truncate(time.Microsecond)
				job, lease, ok, err := d.Reserve(ctx, queue, now, leaseFor)
				if err != nil {
					errCh <- fmt.Errorf("Reserve: %w", err)
					return
				}
				if !ok {
					time.Sleep(1 * time.Millisecond)
					continue
				}

				// Detect a double lease while job is in-flight
				if _, loaded := inFlight.LoadOrStore(string(job.ID), true); loaded {
					errCh <- fmt.Errorf("double lease detected for job=%s", job.ID)
					return
				}

				// Keep it in-flight for a moment to widen overlap window
				time.Sleep(2 * time.Millisecond)

				ackNow := time.Now().UTC().Truncate(time.Microsecond)
				if err := d.Ack(ctx, string(job.ID), lease.Token, ackNow); err != nil {
					inFlight.Delete(string(job.ID))
					errCh <- fmt.Errorf("Ack: %w", err)
					return
				}

				inFlight.Delete(string(job.ID))
				atomic.AddInt64(&doneCount, 1)
			}
		}(w)
	}

	close(start)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case err := <-errCh:
		t.Fatal(err)
	case <-time.After(20 * time.Second):
		t.Fatalf("timeout: done=%d/%d", atomic.LoadInt64(&doneCount), totalJobs)
	}

	// sanity: everything should be terminal now
	var doneCnt, dlqCnt int64
	if err := pool.QueryRow(ctx, `
		SELECT
		  COUNT(*) FILTER (WHERE status='done')::bigint,
		  COUNT(*) FILTER (WHERE status='dlq')::bigint
		FROM th_jobs
	`).Scan(&doneCnt, &dlqCnt); err != nil {
		t.Fatalf("scan terminal counts: %v", err)
	}

	if doneCnt+dlqCnt != int64(totalJobs) {
		t.Fatalf("expected terminal=%d, got done=%d dlq=%d", totalJobs, doneCnt, dlqCnt)
	}
}

func TestReserve_ReclaimExpiredLease_SingleWinner(t *testing.T) {
	pool, ctx, cancel := newTestPool(t)
	defer cancel()
	defer pool.Close()

	d, err := NewWithPool(pool)
	if err != nil {
		t.Fatalf("NewWithPool: %v", err)
	}

	if _, err := pool.Exec(ctx, `DELETE FROM th_jobs`); err != nil {
		t.Fatalf("cleanup: %v", err)
	}

	const (
		queue    = "default"
		workers  = 32
		leaseFor = 50 * time.Millisecond
	)

	t0 := time.Now().UTC().Truncate(time.Microsecond)
	id := fmt.Sprintf("reclaim_%d", time.Now().UnixNano())

	rec := newJobRecord(id, t0)
	rec.Queue = queue

	if _, _, err := d.Enqueue(ctx, rec); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// First reserve (we do not ack)
	now := time.Now().UTC().Truncate(time.Microsecond)
	job1, lease1, ok, err := d.Reserve(ctx, queue, now, leaseFor)
	if err != nil || !ok {
		t.Fatalf("Reserve first: ok=%v err=%v", ok, err)
	}
	if string(job1.ID) != id {
		t.Fatalf("expected job id=%s got %s", id, job1.ID)
	}

	// Let lease expire
	time.Sleep(leaseFor + 30*time.Millisecond)

	// Now hammer reserve concurrently: exactly one should reclaim the expired job
	start := make(chan struct{})
	errCh := make(chan error, workers)

	var winnerCount int64
	var wg sync.WaitGroup
	wg.Add(workers)

	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			<-start

			now2 := time.Now().UTC().Truncate(time.Microsecond)
			job2, lease2, ok2, err := d.Reserve(ctx, queue, now2, 200*time.Millisecond)
			if err != nil {
				errCh <- fmt.Errorf("Reserve reclaim: %w", err)
				return
			}
			if !ok2 {
				return
			}

			// Only one goroutine should get the job
			if string(job2.ID) != id {
				errCh <- fmt.Errorf("unexpected reclaimed job id=%s", job2.ID)
				return
			}
			if lease2.Token == lease1.Token {
				errCh <- fmt.Errorf("expected new lease token on reclaim, got same token")
				return
			}

			if atomic.AddInt64(&winnerCount, 1) > 1 {
				errCh <- fmt.Errorf("multiple winners reclaimed the same job")
				return
			}

			ackNow := time.Now().UTC().Truncate(time.Microsecond)
			if err := d.Ack(ctx, id, lease2.Token, ackNow); err != nil {
				errCh <- fmt.Errorf("Ack reclaimed: %w", err)
				return
			}
		}()
	}

	close(start)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case err := <-errCh:
		t.Fatal(err)
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for reclaim workers")
	}

	if atomic.LoadInt64(&winnerCount) != 1 {
		t.Fatalf("expected exactly 1 reclaim winner, got %d", winnerCount)
	}

	// job should be done and not reservable
	now3 := time.Now().UTC().Truncate(time.Microsecond)
	_, _, ok3, err := d.Reserve(ctx, queue, now3, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("Reserve after done: %v", err)
	}
	if ok3 {
		t.Fatalf("expected ok=false after final ack")
	}
}
