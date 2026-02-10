package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ARJ2211/taskharbor/taskharbor"
	"github.com/ARJ2211/taskharbor/taskharbor/driver/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

type StressPayload struct {
	JobNum int
	Queue  string
	Mode   string // "ok", "flaky", "fail"
	WorkMS int
	Body   string
}

var (
	db          *pgxpool.Pool
	flakySeen   sync.Map // key: jobID -> bool, used to fail only once
	okCount     int64
	failCount   int64
	flakyFailed int64
)

func main() {
	_ = godotenv.Load()

	var (
		totalJobs      = flag.Int("jobs", 120000, "total jobs to enqueue")
		numQueues      = flag.Int("queues", 16, "number of queues (q0..qN)")
		workersPerQ    = flag.Int("workers-per-queue", 4, "number of worker instances per queue")
		concurrency    = flag.Int("concurrency", 50, "handler concurrency per worker")
		pollMS         = flag.Int("poll-ms", 10, "worker poll interval (ms)")
		hbMS           = flag.Int("heartbeat-ms", 50, "worker heartbeat interval (ms)")
		maxAttempts    = flag.Int("max-attempts", 5, "job-level max attempts")
		flakyPct       = flag.Int("flaky-pct", 30, "percent of jobs that fail once then succeed")
		failPct        = flag.Int("fail-pct", 20, "percent of jobs that always fail (should DLQ)")
		workMinMS      = flag.Int("work-min-ms", 1, "min simulated work per job (ms)")
		workMaxMS      = flag.Int("work-max-ms", 15, "max simulated work per job (ms)")
		bodyBytes      = flag.Int("body-bytes", 256, "payload body size in bytes")
		maxConns       = flag.Int("max-conns", 50, "pgxpool max connections")
		reset          = flag.Bool("reset", true, "TRUNCATE th_jobs before starting")
		printEveryMS   = flag.Int("print-ms", 1000, "print DB progress every N ms")
		runTimeoutSecs = flag.Int("timeout-secs", 6000, "overall run timeout (seconds)")
		seed           = flag.Int64("seed", 42, "rng seed (repeatable runs)")
	)
	flag.Parse()

	dsn := os.Getenv("TASKHARBOR_DSN")
	if dsn == "" {
		log.Fatal("TASKHARBOR_DSN not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*runTimeoutSecs)*time.Second)
	defer cancel()

	pcfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		log.Fatal(err)
	}
	pcfg.MaxConns = int32(*maxConns)

	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	if err := postgres.ApplyMigrations(ctx, pool); err != nil {
		log.Fatal(err)
	}

	if *reset {
		if _, err := pool.Exec(ctx, `TRUNCATE th_jobs`); err != nil {
			log.Fatal(err)
		}
	}

	d, err := postgres.NewWithPool(pool)
	if err != nil {
		log.Fatal(err)
	}

	db = pool

	client := taskharbor.NewClient(d)

	/*
		// Optional: explicit retry policy.
		rp := taskharbor.NewExponentialBackoffPolicy(
			2*time.Second,
			10*time.Second,
			2.0,
			0.0,
			taskharbor.WithMaxAttempts(*maxAttempts),
		)
	*/

	queues := make([]string, 0, *numQueues)
	for i := 0; i < *numQueues; i++ {
		queues = append(queues, fmt.Sprintf("q%d", i))
	}

	handler := func(ctx context.Context, job taskharbor.Job) error {
		var p StressPayload
		err := taskharbor.JsonCodec{}.Unmarshal(job.Payload, &p)
		if err != nil {
			return err
		}

		time.Sleep(time.Duration(p.WorkMS) * time.Millisecond)

		switch p.Mode {
		case "fail":
			atomic.AddInt64(&failCount, 1)
			return errors.New("always-fail (should DLQ after max attempts)")
		case "flaky":
			// fail only once per job ID, then succeed on retry
			if _, loaded := flakySeen.LoadOrStore(string(job.ID), true); !loaded {
				atomic.AddInt64(&flakyFailed, 1)
				return errors.New("flaky-fail-once (should retry then succeed)")
			}
			atomic.AddInt64(&okCount, 1)
			return nil
		default:
			atomic.AddInt64(&okCount, 1)
			return nil
		}
	}

	var workerWG sync.WaitGroup
	errCh := make(chan error, *numQueues**workersPerQ)

	for _, q := range queues {
		for i := 0; i < *workersPerQ; i++ {
			w := taskharbor.NewWorker(
				d,
				taskharbor.WithDefaultQueue(q),
				taskharbor.WithConcurrency(*concurrency),
				taskharbor.WithPollInterval(time.Duration(*pollMS)*time.Millisecond),
				taskharbor.WithHeartbeatInterval(time.Duration(*hbMS)*time.Millisecond),
				// taskharbor.WithRetryPolicy(rp),
			)

			if err := w.Register("stress:job", handler); err != nil {
				log.Fatal(err)
			}

			workerWG.Add(1)
			go func(wk *taskharbor.Worker) {
				defer workerWG.Done()
				if err := wk.Run(ctx); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					errCh <- err
				}
			}(w)
		}
	}

	rng := rand.New(rand.NewSource(*seed))

	body := make([]byte, *bodyBytes)
	for i := range body {
		body[i] = byte('a' + (i % 26))
	}
	bodyStr := string(body)

	fmt.Printf("ENQUEUE: jobs=%d queues=%d workersPerQueue=%d concurrency=%d maxAttempts=%d\n",
		*totalJobs, *numQueues, *workersPerQ, *concurrency, *maxAttempts,
	)

	start := time.Now()

	for i := 0; i < *totalJobs; i++ {
		q := queues[i%len(queues)]

		modeRoll := rng.Intn(100)
		mode := "ok"
		if modeRoll < *failPct {
			mode = "fail"
		} else if modeRoll < *failPct+*flakyPct {
			mode = "flaky"
		}

		work := *workMinMS
		if *workMaxMS > *workMinMS {
			work = *workMinMS + rng.Intn(*workMaxMS-*workMinMS+1)
		}

		// Add some scheduled jobs so you also hit run_at gating.
		var runAt time.Time
		if i%10 == 0 {
			runAt = time.Now().UTC().Add(time.Duration(rng.Intn(1500)) * time.Millisecond)
		}

		// IdempotencyKey: deterministic per logical job (scoped by queue on the DB side).
		// If you rerun with the same seed and reset=false, duplicates will dedupe and return the original job id.
		idemKey := fmt.Sprintf("stress:%d:%s:%d", *seed, q, i)

		req := taskharbor.JobRequest{
			Type: "stress:job",
			Payload: StressPayload{
				JobNum: i,
				Queue:  q,
				Mode:   mode,
				WorkMS: work,
				Body:   bodyStr,
			},
			Queue:          q,
			MaxAttempts:    *maxAttempts,
			IdempotencyKey: idemKey,
		}
		if !runAt.IsZero() {
			req.RunAt = runAt
		}

		if _, err := client.Enqueue(ctx, req); err != nil {
			log.Fatalf("enqueue failed at i=%d: %v", i, err)
		}
	}

	fmt.Println("ENQUEUE: done")

	// Progress printer + completion wait: done + dlq should reach totalJobs.
	ticker := time.NewTicker(time.Duration(*printEveryMS) * time.Millisecond)
	defer ticker.Stop()

	var (
		doneCnt     int64
		dlqCnt      int64
		readyCnt    int64
		inflightCnt int64
	)

	for {
		select {
		case <-ctx.Done():
			fmt.Printf("TIMEOUT/CANCEL: ctx=%v\n", ctx.Err())
			goto shutdown
		case err := <-errCh:
			log.Fatalf("worker error: %v", err)
		case <-ticker.C:
			err := pool.QueryRow(ctx, `
				SELECT
				  COUNT(*) FILTER (WHERE status='done')::bigint,
				  COUNT(*) FILTER (WHERE status='dlq')::bigint,
				  COUNT(*) FILTER (WHERE status='ready')::bigint,
				  COUNT(*) FILTER (WHERE status='inflight')::bigint
				FROM th_jobs
			`).Scan(&doneCnt, &dlqCnt, &readyCnt, &inflightCnt)
			if err != nil {
				log.Fatalf("progress query failed: %v", err)
			}

			elapsed := time.Since(start).Seconds()
			terminal := doneCnt + dlqCnt
			rate := float64(terminal) / elapsed

			fmt.Printf("progress: done=%d dlq=%d ready=%d inflight=%d terminal=%d/%d rate=%.0f jobs/s handlerOK=%d handlerErr=%d flakyFirstFail=%d\n",
				doneCnt, dlqCnt, readyCnt, inflightCnt, terminal, *totalJobs, rate,
				atomic.LoadInt64(&okCount),
				atomic.LoadInt64(&failCount),
				atomic.LoadInt64(&flakyFailed),
			)

			if terminal >= int64(*totalJobs) {
				goto shutdown
			}
		}
	}

shutdown:
	// Stop workers and wait for clean exit.
	cancel()
	workerWG.Wait()

	totalDur := time.Since(start)
	terminal := doneCnt + dlqCnt
	var finalRate float64
	if totalDur.Seconds() > 0 {
		finalRate = float64(terminal) / totalDur.Seconds()
	}

	fmt.Println()
	fmt.Println("SUMMARY")
	fmt.Printf("duration=%s totalJobs=%d done=%d dlq=%d terminal=%d finalRate=%.0f jobs/s\n",
		totalDur.Round(time.Millisecond),
		*totalJobs,
		doneCnt,
		dlqCnt,
		terminal,
		finalRate,
	)
}
