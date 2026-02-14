// basic example but using Redis instead of memory.
// needs Redis running (e.g. docker-compose up -d redis).
//
// Env:
//   REDIS_ADDR (default: localhost:6379)

package main

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/ARJ2211/taskharbor/taskharbor"
	"github.com/ARJ2211/taskharbor/taskharbor/driver/redis"
)

type EmailPayload struct {
	To      string
	Subject string
	Body    string
}

// Demo-only: fail exactly once for "RetryMe" so you can see retry.
// atomic makes it safe with concurrency > 1.
var failOnce int32 = 1

func main() {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d, err := redis.New(ctx, addr)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = d.Close()
	}()

	client := taskharbor.NewClient(d)

	/*
		Optional: custom retry policy.

		You do NOT need this for retries anymore if the worker has a default retry policy.
		Only set a custom policy when you want:
		- different backoff timing (base/max/multiplier/jitter)
		- a worker-wide cap that overrides large job MaxAttempts (global safety cap)

		Uncomment to make retries very obvious (2s base delay):

		rp := taskharbor.NewExponentialBackoffPolicy(
			2*time.Second,  // baseDelay
			10*time.Second, // maxDelay
			2.0,            // multiplier
			0.0,            // jitterFrac
			taskharbor.WithMaxAttempts(5),
		)
	*/

	worker := taskharbor.NewWorker(
		d,
		taskharbor.WithDefaultQueue("default"),
		taskharbor.WithConcurrency(2),
		taskharbor.WithPollInterval(50*time.Millisecond),
		// taskharbor.WithRetryPolicy(rp),
	)

	errReg := worker.Register("email:send:redis", func(ctx context.Context, job taskharbor.Job) error {
		time.Sleep(1 * time.Second)

		var p EmailPayload
		err := taskharbor.JsonCodec{}.Unmarshal(job.Payload, &p)
		if err != nil {
			return err
		}

		// Slow down the "RetryMe" job so you can clearly watch:
		// ready -> inflight -> ready(with run_at in future) -> inflight -> done
		if p.Subject == "RetryMe" {
			time.Sleep(2 * time.Second)
		}

		// Fail exactly once to trigger retry.
		if p.Subject == "RetryMe" && atomic.CompareAndSwapInt32(&failOnce, 1, 0) {
			fmt.Printf("HANDLER: intentional failure to trigger retry (id=%s)\n", job.ID)
			return fmt.Errorf("intentional failure to trigger retry")
		}

		// Always fail so it eventually goes to DLQ.
		if p.Subject == "WillDLQ" {
			fmt.Printf("HANDLER: always failing -> DLQ (id=%s)\n", job.ID)
			return fmt.Errorf("always fail for DLQ demo")
		}

		fmt.Printf(
			"HANDLER: sending email to=%s subject=%s body=%s (id=%s)\n",
			p.To,
			p.Subject,
			p.Body,
			job.ID,
		)

		return nil
	})
	if errReg != nil {
		panic(errReg)
	}

	base := time.Now().UTC()

	fmt.Println("ENQUEUE: immediate job")
	_, err = client.Enqueue(ctx, taskharbor.JobRequest{
		Type: "email:send:redis",
		Payload: EmailPayload{
			To:      "user@example.com",
			Subject: "Hello",
			Body:    "Immediate job",
		},
		Queue:       "default",
		MaxAttempts: 5,
		// Optional: idempotency key. (Milestone 6 behavior varies by driver.)
		// IdempotencyKey: "k1",
	})
	if err != nil {
		panic(err)
	}

	fmt.Println("ENQUEUE: retry demo job (fails once, then succeeds)")
	_, err = client.Enqueue(ctx, taskharbor.JobRequest{
		Type: "email:send:redis",
		Payload: EmailPayload{
			To:      "user@example.com",
			Subject: "RetryMe",
			Body:    "Fails once to show retry/backoff",
		},
		Queue:       "default",
		MaxAttempts: 5,
		RunAt:       base.Add(3 * time.Second),
	})
	if err != nil {
		panic(err)
	}

	fmt.Println("ENQUEUE: scheduled job (2s later)")
	_, err = client.Enqueue(ctx, taskharbor.JobRequest{
		Type: "email:send:redis",
		Payload: EmailPayload{
			To:      "user@example.com",
			Subject: "Scheduled",
			Body:    "Scheduled job (2s)",
		},
		Queue:       "default",
		RunAt:       base.Add(2 * time.Second),
		MaxAttempts: 5,
	})
	if err != nil {
		panic(err)
	}

	fmt.Println("ENQUEUE: scheduled job (5s later)")
	_, err = client.Enqueue(ctx, taskharbor.JobRequest{
		Type: "email:send:redis",
		Payload: EmailPayload{
			To:      "user@example.com",
			Subject: "Scheduled",
			Body:    "Scheduled job (5s)",
		},
		Queue:       "default",
		RunAt:       base.Add(5 * time.Second),
		MaxAttempts: 5,
	})
	if err != nil {
		panic(err)
	}

	fmt.Println("ENQUEUE: scheduled job (8s later)")
	_, err = client.Enqueue(ctx, taskharbor.JobRequest{
		Type: "email:send:redis",
		Payload: EmailPayload{
			To:      "user@example.com",
			Subject: "Scheduled",
			Body:    "Scheduled job (8s)",
		},
		Queue:       "default",
		RunAt:       base.Add(8 * time.Second),
		MaxAttempts: 5,
	})
	if err != nil {
		panic(err)
	}

	fmt.Println("ENQUEUE: job that will go to DLQ (always fails)")
	_, err = client.Enqueue(ctx, taskharbor.JobRequest{
		Type: "email:send:redis",
		Payload: EmailPayload{
			To:      "user@example.com",
			Subject: "WillDLQ",
			Body:    "Always fails -> DLQ",
		},
		Queue:       "default",
		MaxAttempts: 2,
	})
	if err != nil {
		panic(err)
	}

	done := make(chan error, 1)
	go func() {
		done <- worker.Run(ctx)
	}()

	// Let worker process jobs.
	time.Sleep(15 * time.Second)

	fmt.Println("SHUTDOWN: cancel worker")
	cancel()

	err = <-done
	if err != nil {
		fmt.Printf("worker stopped with error: %v\n", err)
		return
	}

	fmt.Println("DONE")

	fmt.Println("TIP: watch Redis keys while this runs (prefix depends on driver options):")
	fmt.Println(`redis-cli -h 127.0.0.1 -p 6379 --scan | head`)
}
