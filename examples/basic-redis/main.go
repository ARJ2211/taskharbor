// basic example but using Redis instead of memory. need Redis running (e.g. docker-compose up -d redis).
// REDIS_ADDR env or default localhost:6379

package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/ARJ2211/taskharbor/taskharbor"
	"github.com/ARJ2211/taskharbor/taskharbor/driver/redis"
)

type EmailPayload struct {
	To      string
	Subject string
	Body    string
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
	defer d.Close()

	client := taskharbor.NewClient(d)
	worker := taskharbor.NewWorker(
		d,
		taskharbor.WithDefaultQueue("default"),
		taskharbor.WithConcurrency(2),
		taskharbor.WithPollInterval(50*time.Millisecond),
	)

	err = worker.Register("email.send", func(ctx context.Context, job taskharbor.Job) error {
		var p EmailPayload
		err := taskharbor.JsonCodec{}.Unmarshal(job.Payload, &p)
		if err != nil {
			return err
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
		Queue:       "default",
		MaxAttempts: 5,
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

	done := make(chan error, 1)
	go func() {
		done <- worker.Run(ctx)
	}()

	time.Sleep(5 * time.Second)
	fmt.Println("SHUTDOWN: cancel worker")
	cancel()

	err = <-done
	if err != nil {
		fmt.Printf("worker stopped with error: %v\n", err)
		return
	}
	fmt.Println("DONE")
}
