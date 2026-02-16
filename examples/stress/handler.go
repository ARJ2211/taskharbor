// examples/stress/handler.go
package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ARJ2211/taskharbor/taskharbor"
)

type StressPayload struct {
	JobNum int
	Queue  string
	Mode   string // "ok", "flaky", "fail"
	WorkMS int
	Body   string

	Depth int  // 0 for original, 1 for child
	Spawn bool // if true, enqueue a child on successful completion
}

var (
	flakySeen     sync.Map
	spawnedChild  sync.Map
	okCount       int64
	failCount     int64
	flakyFailed   int64
	childEnqueued int64
)

func newRNG(seed int64) *rand.Rand {
	return rand.New(rand.NewSource(seed))
}

func makeBody(n int) string {
	if n <= 0 {
		return ""
	}
	b := make([]byte, n)
	for i := range b {
		b[i] = byte('a' + (i % 26))
	}
	return string(b)
}

func buildQueues(n int) []string {
	if n <= 0 {
		n = 1
	}
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, fmt.Sprintf("q%d", i))
	}
	return out
}

func makeStressHandler(client *taskharbor.Client) func(ctx context.Context, job taskharbor.Job) error {
	return func(ctx context.Context, job taskharbor.Job) error {
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
			if _, loaded := flakySeen.LoadOrStore(string(job.ID), true); !loaded {
				atomic.AddInt64(&flakyFailed, 1)
				return errors.New("flaky-fail-once (should retry then succeed)")
			}
		}

		if p.Spawn && p.Depth == 0 {
			if _, loaded := spawnedChild.LoadOrStore(string(job.ID), true); !loaded {
				child := StressPayload{
					JobNum: p.JobNum,
					Queue:  p.Queue,
					Mode:   "ok",
					WorkMS: 1,
					Body:   p.Body,
					Depth:  1,
					Spawn:  false,
				}

				childReq := taskharbor.JobRequest{
					Type:           "stress:job",
					Queue:          p.Queue,
					Payload:        child,
					MaxAttempts:    3,
					IdempotencyKey: fmt.Sprintf("child:%s:1", job.ID),
				}

				if _, err := client.Enqueue(ctx, childReq); err != nil {
					spawnedChild.Delete(string(job.ID))
					return err
				}
				atomic.AddInt64(&childEnqueued, 1)
			}
		}

		atomic.AddInt64(&okCount, 1)
		return nil
	}
}

func enqueueInitialJobs(
	ctx context.Context,
	cfg Config,
	client *taskharbor.Client,
	rng *rand.Rand,
	queues []string,
	bodyStr string,
	onEnqued func(),
) (int64, error) {
	var plannedChildren int64

	for i := 0; i < cfg.TotalJobs; i++ {
		q := queues[i%len(queues)]

		modeRoll := rng.Intn(100)
		mode := "ok"
		if modeRoll < cfg.FailPct {
			mode = "fail"
		} else if modeRoll < cfg.FailPct+cfg.FlakyPct {
			mode = "flaky"
		}

		workMS := int(cfg.WorkMin / time.Millisecond)
		if cfg.WorkMax > cfg.WorkMin {
			min := int(cfg.WorkMin / time.Millisecond)
			max := int(cfg.WorkMax / time.Millisecond)
			workMS = min + rng.Intn(max-min+1)
		}

		var runAt time.Time
		if cfg.ScheduleRate > 0 && (i%cfg.ScheduleRate == 0) {
			runAt = time.Now().UTC().Add(time.Duration(rng.Intn(1500)) * time.Millisecond)
		}

		spawn := false
		if cfg.SpawnEvery > 0 && (i%cfg.SpawnEvery == 0) && mode != "fail" {
			spawn = true
			plannedChildren++
		}

		req := taskharbor.JobRequest{
			Type: "stress:job",
			Payload: StressPayload{
				JobNum: i,
				Queue:  q,
				Mode:   mode,
				WorkMS: workMS,
				Body:   bodyStr,
				Depth:  0,
				Spawn:  spawn,
			},
			Queue:          q,
			MaxAttempts:    cfg.MaxAttempts,
			IdempotencyKey: fmt.Sprintf("stress:%d:%s:%d", cfg.Seed, q, i),
		}
		if !runAt.IsZero() {
			req.RunAt = runAt
		}

		if _, err := client.Enqueue(ctx, req); err != nil {
			return plannedChildren, err
		}

		if onEnqued != nil {
			onEnqued()
		}
	}

	return plannedChildren, nil
}
