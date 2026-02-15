// examples/stress/progress.go
package main

import (
	"fmt"
	"sync/atomic"
	"time"
)

type Progress struct {
	Now time.Time

	Done       int64
	DoneApprox bool

	DLQ       int64
	Ready     int64
	Scheduled int64
	Inflight  int64
}

func (p Progress) Terminal() int64 {
	return p.Done + p.DLQ
}

func (p Progress) Format(targetJobs int64, rate float64) string {
	doneLabel := "done"
	termLabel := "terminal"
	if p.DoneApprox {
		doneLabel = "done≈"
		termLabel = "terminal≈"
	}

	return fmt.Sprintf(
		"progress: %s=%d dlq=%d ready=%d scheduled=%d inflight=%d %s=%d/%d rate≈%.0f jobs/s ok=%d handlerErr=%d flakyFirstFail=%d childrenEnq=%d",
		doneLabel, p.Done,
		p.DLQ,
		p.Ready,
		p.Scheduled,
		p.Inflight,
		termLabel, p.Terminal(), targetJobs,
		rate,
		atomic.LoadInt64(&okCount),
		atomic.LoadInt64(&failCount),
		atomic.LoadInt64(&flakyFailed),
		atomic.LoadInt64(&childEnqueued),
	)
}

func atomicLoadOKCount() int64 {
	return atomic.LoadInt64(&okCount)
}
