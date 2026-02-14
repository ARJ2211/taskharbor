// basic example but using Redis instead of memory. need Redis running (e.g. docker-compose up -d redis).
// REDIS_ADDR env or default localhost:6379

package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
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

// Demo: fail exactly once for "RetryMe" so retry/backoff is visible.
var failOnce int32 = 1

func randomIdempotencyKey() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

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
		if err := d.Close(); err != nil {
			fmt.Printf("driver close error: %v\n", err)
		}
	}()

	client := taskharbor.NewClient(d)

	rp := taskharbor.NewExponentialBackoffPolicy(
		2*time.Second,  // baseDelay
		10*time.Second, // maxDelay
		2.0,            // multiplier
		0.2,            // jitterFrac
		taskharbor.WithMaxAttempts(5),
	)
	worker := taskharbor.NewWorker(
		d,
		taskharbor.WithDefaultQueue("default"),
		taskharbor.WithConcurrency(2),
		taskharbor.WithPollInterval(50*time.Millisecond),
		taskharbor.WithRetryPolicy(rp),
	)

	err = worker.Register("email.send", func(ctx context.Context, job taskharbor.Job) error {
		var p EmailPayload
		err := taskharbor.JsonCodec{}.Unmarshal(job.Payload, &p)
		if err != nil {
			return err
		}
		// RetryMe: fail once to showcase retry + exponential backoff.
		if p.Subject == "RetryMe" && atomic.CompareAndSwapInt32(&failOnce, 1, 0) {
			fmt.Printf("HANDLER: intentional failure to trigger retry (id=%s)\n", job.ID)
			return fmt.Errorf("intentional failure to trigger retry")
		}
		// WillDLQ: always fail so job eventually goes to DLQ.
		if p.Subject == "WillDLQ" {
			fmt.Printf("HANDLER: always failing -> DLQ (id=%s)\n", job.ID)
			return fmt.Errorf("always fail for DLQ demo")
		}
		fmt.Printf("HANDLER: sending email to=%s subject=%s body=%s\n", p.To, p.Subject, p.Body)
		return nil
	})
	if err != nil {
		panic(err)
	}

	fmt.Println("ENQUEUE: immediate job")
	_, err = client.Enqueue(ctx, taskharbor.JobRequest{
		Type: "email.send",
		Payload: EmailPayload{
			To:      "user@example.com",
			Subject: "Hello",
			Body:    "Immediate job",
		},
		Queue:          "default",
		MaxAttempts:    5,
		IdempotencyKey: randomIdempotencyKey(),
	})
	if err != nil {
		panic(err)
	}

	fmt.Println("ENQUEUE: scheduled job (2s later)")
	_, err = client.Enqueue(ctx, taskharbor.JobRequest{
		Type: "email.send",
		Payload: EmailPayload{
			To:      "user@example.com",
			Subject: "Scheduled",
			Body:    "Scheduled job (2s)",
		},
		Queue: "default",
		RunAt: time.Now().UTC().Add(2 * time.Second),
	})
	if err != nil {
		panic(err)
	}

	base := time.Now().UTC()
	fmt.Println("ENQUEUE: retry demo job (fails once, then succeeds)")
	_, err = client.Enqueue(ctx, taskharbor.JobRequest{
		Type: "email.send",
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

	fmt.Println("ENQUEUE: job that will go to DLQ (always fails)")
	_, err = client.Enqueue(ctx, taskharbor.JobRequest{
		Type: "email.send",
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

	time.Sleep(15 * time.Second)
	fmt.Println("SHUTDOWN: cancel worker")
	cancel()

	err = <-done
	if err != nil {
		fmt.Printf("worker stopped with error: %v\n", err)
		return
	}
	fmt.Println("DONE")
}
