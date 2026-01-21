package main

import (
	"context"
	"fmt"
	"time"

	"github.com/ARJ2211/taskharbor/taskharbor"
	"github.com/ARJ2211/taskharbor/taskharbor/driver/memory"
)

type EmailPayload struct {
	To      string
	Subject string
	Body    string
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d := memory.New()

	client := taskharbor.NewClient(d)
	worker := taskharbor.NewWorker(
		d,
		taskharbor.WithDefaultQueue("default"),
		taskharbor.WithConcurrency(2),
		taskharbor.WithPollInterval(50*time.Millisecond),
	)

	err := worker.Register("email.send", func(ctx context.Context, job taskharbor.Job) error {
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
	})
	if err != nil {
		panic(err)
	}

	fmt.Println("ENQUEUE: immediate job")
	_, err = client.Enqueue(ctx, taskharbor.JobRequest{
		Type: "email.send",
		Payload: EmailPayload{
			To:      "aayush@example.com",
			Subject: "Hello",
			Body:    "Immediate job",
		},
		Queue: "default",
	})
	if err != nil {
		panic(err)
	}

	fmt.Println("ENQUEUE: scheduled job (5s later)")
	_, err = client.Enqueue(ctx, taskharbor.JobRequest{
		Type: "email.send",
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
		Type: "email.send",
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
