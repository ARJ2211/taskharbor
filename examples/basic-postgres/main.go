package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"time"

	"github.com/ARJ2211/taskharbor/taskharbor"
	"github.com/ARJ2211/taskharbor/taskharbor/driver/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

type EmailPayload struct {
	To      string
	Subject string
	Body    string
}

// Demo-only: shared pool so the handler can read attempts from Postgres.
// taskharbor.Job does not expose Attempts yet.
var db *pgxpool.Pool

// Demo-only: fail exactly once for "RetryMe" so you can see retry.
// atomic makes it safe with concurrency > 1.
var failOnce int32 = 1

func main() {
	_ = godotenv.Load()

	dsn := os.Getenv("TASKHARBOR_DSN")
	if dsn == "" {
		log.Fatal("TASKHARBOR_DSN not set")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	if err := postgres.ApplyMigrations(ctx, pool); err != nil {
		log.Fatal(err)
	}

	d, err := postgres.NewWithPool(pool)
	if err != nil {
		log.Fatal(err)
	}

	db = pool

	client := taskharbor.NewClient(d)

	/*
		Optional: custom retry policy.

		You do NOT need this for retries anymore if the worker has a default retry policy.
		Only set a custom policy when you want:
		- different backoff timing (base/max/multiplier/jitter)
		- a worker-wide cap that overrides large job MaxAttempts (global safety cap)

		Uncomment to make retries very obvious in the DB (2s base delay):

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
		taskharbor.WithHeartbeatInterval(50*time.Millisecond),
		// taskharbor.WithRetryPolicy(rp),
	)

	errReg := worker.Register("email:send:postgres", func(ctx context.Context, job taskharbor.Job) error {
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

		// Attempts aren't on taskharbor.Job yet, so read from Postgres for display.
		var attempts int
		_ = db.QueryRow(context.Background(), `SELECT attempts FROM th_jobs WHERE id=$1`, job.ID).Scan(&attempts)

		fmt.Printf(
			"HANDLER: sending email to=%s subject=%s body=%s (id=%s attempts=%d)\n",
			p.To,
			p.Subject,
			p.Body,
			job.ID,
			attempts,
		)

		return nil
	})
	if errReg != nil {
		panic(errReg)
	}

	base := time.Now().UTC()

	fmt.Println("ENQUEUE: immediate job")
	_, err = client.Enqueue(ctx, taskharbor.JobRequest{
		Type: "email:send:postgres",
		Payload: EmailPayload{
			To:      "aayush@example.com",
			Subject: "Hello",
			Body:    "Immediate job",
		},
		Queue:       "default",
		MaxAttempts: 5,
	})
	if err != nil {
		panic(err)
	}

	fmt.Println("ENQUEUE: retry demo job (fails once, then succeeds)")
	_, err = client.Enqueue(ctx, taskharbor.JobRequest{
		Type: "email:send:postgres",
		Payload: EmailPayload{
			To:      "aayush@example.com",
			Subject: "RetryMe",
			Body:    "Fails once to show attempts/run_at backoff",
		},
		Queue:          "default",
		MaxAttempts:    5,
		RunAt:          base.Add(3 * time.Second),
		IdempotencyKey: "k1",
	})
	if err != nil {
		panic(err)
	}

	fmt.Println("ENQUEUE: scheduled job (2s later)")
	_, err = client.Enqueue(ctx, taskharbor.JobRequest{
		Type: "email:send:postgres",
		Payload: EmailPayload{
			To:      "aayush@example.com",
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
		Type: "email:send:postgres",
		Payload: EmailPayload{
			To:      "aayush@example.com",
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
		Type: "email:send:postgres",
		Payload: EmailPayload{
			To:      "aayush@example.com",
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

	fmt.Println("TIP: watch DB state while this runs:")
	fmt.Println(`watch -n 0.5 'psql --username=taskharbor --dbname=taskharbor -c "select id, type, status, attempts, run_at, last_error, dlq_reason from th_jobs order by created_at desc;"'`)
}
