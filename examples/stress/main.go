package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
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

	var workerWG sync.WaitGroup
	errCh := make(chan error, cfg.NumQueues*cfg.WorkersPerQueue)

	handler := makeStressHandler(client)

	/*
		Optional: custom retry policy.

		You do NOT need this for retries anymore if the worker has a default retry policy.
		Only set a custom policy when you want:
		- different backoff timing (base/max/multiplier/jitter)
		- a worker-wide cap that overrides large job MaxAttempts (global safety cap)

	*/

	var rp taskharbor.RetryPolicy
	rp = taskharbor.NewExponentialBackoffPolicy(
		2*time.Second,  // baseDelay
		10*time.Second, // maxDelay
		2.0,            // multiplier
		0.0,            // jitterFrac
		taskharbor.WithMaxAttempts(5),
	)

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

			workerWG.Add(1)
			go func(wk *taskharbor.Worker) {
				defer workerWG.Done()
				if err := wk.Run(ctx); err != nil &&
					!errors.Is(err, context.Canceled) &&
					!errors.Is(err, context.DeadlineExceeded) {
					errCh <- err
				}
			}(w)
		}
	}

	rng := newRNG(cfg.Seed)
	bodyStr := makeBody(cfg.BodyBytes)

	fmt.Printf(
		"ENQUEUE: driver=%s jobs=%d queues=%d workersPerQueue=%d concurrency=%d maxAttempts=%d spawnEvery=%d\n",
		cfg.DriverType, cfg.TotalJobs, cfg.NumQueues, cfg.WorkersPerQueue, cfg.Concurrency, cfg.MaxAttempts, cfg.SpawnEvery,
	)

	start := time.Now()

	plannedChildren := int64(0)
	targetJobs := int64(0)

	plannedChildren, err = enqueueInitialJobs(ctx, cfg, client, rng, queues, bodyStr)
	if err != nil {
		log.Fatal(err)
	}
	targetJobs = int64(cfg.TotalJobs) + plannedChildren

	fmt.Printf("ENQUEUE: done (planned children=%d targetJobs=%d)\n", plannedChildren, targetJobs)

	ticker := time.NewTicker(cfg.PrintEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			fmt.Printf("TIMEOUT/CANCEL: ctx=%v\n", ctx.Err())
			goto shutdown

		case err := <-errCh:
			log.Fatalf("worker error: %v", err)

		case <-ticker.C:
			p, err := b.Progress(ctx, queues, time.Now().UTC())
			if err != nil {
				log.Fatal(err)
			}

			terminal := p.Terminal()
			elapsed := time.Since(start).Seconds()

			rate := 0.0
			if elapsed > 0 {
				rate = float64(terminal) / elapsed
			}

			fmt.Println(p.Format(targetJobs, rate))

			if terminal >= targetJobs && p.Ready == 0 && p.Scheduled == 0 && p.Inflight == 0 {
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
	fmt.Printf(
		"duration=%s initialJobs=%d plannedChildren=%d targetJobs=%d terminal=%d finalRate=%.0f jobs/s\n",
		totalDur.Round(time.Millisecond),
		cfg.TotalJobs,
		plannedChildren,
		targetJobs,
		terminal,
		finalRate,
	)
}
