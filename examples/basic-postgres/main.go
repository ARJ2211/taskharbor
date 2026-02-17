package main

import (
	"context"
	"fmt"
	"log"
	"os"
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

func runJob(ctx context.Context, job taskharbor.Job) error {
	var p EmailPayload
	err := taskharbor.JsonCodec{}.Unmarshal(job.Payload, &p)
	if err != nil {
		return err
	}

	fmt.Printf(
		"HANDLER: sending email to=%s subject=%s and body=%s\n",
		p.To,
		p.Subject,
		p.Body,
	)

	return nil
}

func main() {
	_ = godotenv.Load()

	dsn := os.Getenv("TASKHARBOR_DSN")
	if dsn == "" {
		log.Fatal("TASKHARBOR_DSN not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
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

	client := taskharbor.NewClient(d)
	worker := taskharbor.NewWorker(
		d,
		taskharbor.WithDefaultQueue("default"),
		taskharbor.WithConcurrency(2),
		taskharbor.WithPollInterval(50*time.Millisecond),
		taskharbor.WithHeartbeatInterval(20*time.Millisecond),
	)

	errReg := worker.Register("email:send:postgres", runJob)
	if errReg != nil {
		panic(errReg)
	}

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

	fmt.Println("ENQUEUE: scheduled job (5s later)")
	_, err = client.Enqueue(ctx, taskharbor.JobRequest{
		Type: "email:send:postgres",
		Payload: EmailPayload{
			To:      "aayush@example.com",
			Subject: "Scheduled",
			Body:    "Scheduled job (5s)",
		},
		Queue: "default",
		RunAt: time.Now().UTC().Add(5 * time.Second),
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
		Queue: "default",
		RunAt: time.Now().UTC().Add(2 * time.Second),
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
		Queue: "default",
		RunAt: time.Now().UTC().Add(8 * time.Second),
	})
	if err != nil {
		panic(err)
	}

	done := make(chan error, 1)
	go func() {
		done <- worker.Run(ctx)
	}()

	// Let worker process jobs
	time.Sleep(10 * time.Second)

	fmt.Println("SHUTDOWN: cancel worker")
	cancel()

	err = <-done
	if err != nil {
		fmt.Printf("worker stopped with error: %v\n", err)
		return
	}

	fmt.Println("DONE")
}
