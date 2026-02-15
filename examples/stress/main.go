package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ARJ2211/taskharbor/taskharbor"
)

func main() {
	cfg := parseConfigOrExit()

	ctx, cancel := context.WithTimeout(context.Background(), cfg.RunTimeout)
	defer cancel()

	b, err := newBackend(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = b.Close() }()

	if cfg.Reset {
		if err := b.Reset(ctx); err != nil {
			log.Fatal(err)
			return
		}
	}

	client := taskharbor.NewClient(b.Driver)
	queues := buildQueues(cfg.NumQueues)

	// Optional: custom retry policy.
	rp := taskharbor.NewExponentialBackoffPolicy(
		2*time.Second,  // baseDelay
		10*time.Second, // maxDelay
		2.0,            // multiplier
		0.0,            // jitterFrac
		taskharbor.WithMaxAttempts(5),
	)

	var workerWG sync.WaitGroup
	errCh := make(chan error, cfg.NumQueues*cfg.WorkersPerQueue)

	handler := makeStressHandler(client)

	workers := make([]*taskharbor.Worker, 0, cfg.NumQueues*cfg.WorkersPerQueue)
	labels := make([]string, 0, cfg.NumQueues*cfg.WorkersPerQueue)

	for _, q := range queues {
		for i := 0; i < cfg.WorkersPerQueue; i++ {
			var w *taskharbor.Worker
			if cfg.UseRetryPol {
				w = taskharbor.NewWorker(
					b.Driver,
					taskharbor.WithDefaultQueue(q),
					taskharbor.WithConcurrency(cfg.Concurrency),
					taskharbor.WithPollInterval(cfg.PollInterval),
					taskharbor.WithHeartbeatInterval(cfg.HeartbeatInterval),
					taskharbor.WithRetryPolicy(rp),
				)
			} else {
				w = taskharbor.NewWorker(
					b.Driver,
					taskharbor.WithDefaultQueue(q),
					taskharbor.WithConcurrency(cfg.Concurrency),
					taskharbor.WithPollInterval(cfg.PollInterval),
					taskharbor.WithHeartbeatInterval(cfg.HeartbeatInterval),
				)
			}

			if err := w.Register("stress:job", handler); err != nil {
				log.Fatal(err)
			}

			workers = append(workers, w)
			labels = append(labels, fmt.Sprintf("%s/%d", q, i))

			workerWG.Add(1)
			go func(wk *taskharbor.Worker) {
				defer workerWG.Done()
				if err := wk.Run(ctx); err != nil &&
					!errors.Is(err, context.Canceled) &&
					!errors.Is(err, context.DeadlineExceeded) {
					select {
					case errCh <- err:
					default:
					}
				}
			}(w)
		}
	}

	bodyStr := makeBody(cfg.BodyBytes)

	var (
		enqueued        int64
		plannedChildren int64
		targetJobs      int64 = int64(cfg.TotalJobs)
		enqueueFinished int32
	)

	enqueueDone := make(chan struct{})
	enqueueErr := make(chan error, 1)

	go func() {
		rng := newRNG(cfg.Seed)

		pc, err := enqueueInitialJobs(
			ctx,
			cfg,
			client,
			rng,
			queues,
			bodyStr,
			func() { atomic.AddInt64(&enqueued, 1) },
		)
		if err != nil {
			enqueueErr <- err
			close(enqueueDone)
			return
		}

		atomic.StoreInt64(&plannedChildren, pc)
		atomic.StoreInt64(&targetJobs, int64(cfg.TotalJobs)+pc)
		atomic.StoreInt32(&enqueueFinished, 1)
		close(enqueueDone)
	}()

	start := time.Now()

	fmt.Printf(
		"START: driver=%s jobs=%d queues=%d workersPerQueue=%d concurrency=%d maxAttempts=%d spawnEvery=%d\n",
		cfg.DriverType, cfg.TotalJobs, cfg.NumQueues, cfg.WorkersPerQueue, cfg.Concurrency, cfg.MaxAttempts, cfg.SpawnEvery,
	)

	ticker := time.NewTicker(cfg.PrintEvery)
	defer ticker.Stop()

	dashHideCursor()
	defer dashShowCursor()

	for {
		select {
		case <-ctx.Done():
			fmt.Printf("TIMEOUT/CANCEL: ctx=%v\n", ctx.Err())
			goto shutdown

		case err := <-errCh:
			log.Fatalf("worker error: %v", err)

		case err := <-enqueueErr:
			log.Fatalf("enqueue error: %v", err)

		case <-enqueueDone:
			// prevent repeated selects
			enqueueDone = nil

		case <-ticker.C:
			now := time.Now().UTC()
			p, err := b.Progress(ctx, queues, now)
			if err != nil {
				log.Fatal(err)
			}

			tj := atomic.LoadInt64(&targetJobs)
			pc := atomic.LoadInt64(&plannedChildren)
			enq := atomic.LoadInt64(&enqueued)
			enqDone := atomic.LoadInt32(&enqueueFinished) == 1

			terminal := p.Terminal()
			elapsed := time.Since(start).Seconds()

			rate := 0.0
			if elapsed > 0 {
				rate = float64(terminal) / elapsed
			}

			RenderDashboard(cfg, start, cfg.DriverType, tj, p, rate, workers, labels)

			_ = pc
			_ = enq
			_ = enqDone

			if enqDone &&
				terminal >= tj &&
				p.Ready == 0 && p.Scheduled == 0 && p.Inflight == 0 {
				goto shutdown
			}
		}
	}

shutdown:
	cancel()
	workerWG.Wait()

	totalDur := time.Since(start)
	p, _ := b.Progress(context.Background(), queues, time.Now().UTC())
	terminal := p.Terminal()

	finalRate := 0.0
	if totalDur.Seconds() > 0 {
		finalRate = float64(terminal) / totalDur.Seconds()
	}

	fmt.Println()
	fmt.Println("SUMMARY")

	totalWorkers := cfg.NumQueues * cfg.WorkersPerQueue
	theoreticalMaxInFlight := totalWorkers * cfg.Concurrency

	success := p.Done
	dlq := p.DLQ
	successPct := 0.0
	dlqPct := 0.0
	if terminal > 0 {
		successPct = (float64(success) / float64(terminal)) * 100
		dlqPct = (float64(dlq) / float64(terminal)) * 100
	}

	finalTargetJobs := atomic.LoadInt64(&targetJobs)
	finalPlannedChildren := atomic.LoadInt64(&plannedChildren)

	fmt.Printf(`
+----------------------+-------------------------------------------+
| Field                | Value                                     |
+----------------------+-------------------------------------------+
| Driver               | %-41s |
| Duration             | %-41s |
| Total Jobs (initial) | %-41d |
| Planned Children     | %-41d |
| Target Jobs          | %-41d |
| Terminal Jobs        | %-41d |
|   - Success          | %-41d |
|   - DLQ              | %-41d |
| Success %%            |  %-40.2f |
| DLQ %%                |  %-40.2f |
| Rate (terminal/s)    | %-41.0f |
+----------------------+-------------------------------------------+
| Queues               | %-41d |
| Workers/Queue        | %-41d |
| Total Workers        | %-41d |
| Concurrency/Worker   | %-41d |
| Max Inflight (theory)| %-41d |
| Poll Interval        | %-41s |
| Heartbeat Interval   | %-41s |
| Max Attempts         | %-41d |
| Fail %% / Flaky %%     | %-41s |
| Spawn Every          | %-41d |
| Schedule Every       | %-41d |
| Body Bytes           | %-41d |
| Seed                 | %-41d |
+----------------------+-------------------------------------------+
`,
		cfg.DriverType,
		totalDur.Round(time.Millisecond).String(),
		cfg.TotalJobs,
		finalPlannedChildren,
		finalTargetJobs,
		terminal,
		success,
		dlq,
		successPct,
		dlqPct,
		finalRate,
		cfg.NumQueues,
		cfg.WorkersPerQueue,
		totalWorkers,
		cfg.Concurrency,
		theoreticalMaxInFlight,
		cfg.PollInterval.String(),
		cfg.HeartbeatInterval.String(),
		cfg.MaxAttempts,
		fmt.Sprintf("%d%% / %d%%", cfg.FailPct, cfg.FlakyPct),
		cfg.SpawnEvery,
		cfg.ScheduleRate,
		cfg.BodyBytes,
		cfg.Seed,
	)
}
