package main

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/ARJ2211/taskharbor/taskharbor/driver/memory"
)

func newMemoryBackend(ctx context.Context, cfg Config) (backend, error) {
	_ = cfg
	_ = ctx

	d := memory.New()

	return backend{
		Driver:   d,
		DriverID: "memory",

		CloseFn: func() error {
			return d.Close()
		},

		ResetFn: func(ctx context.Context) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			return d.Reset()
		},

		ProgFn: func(ctx context.Context, qs []string, now time.Time) (Progress, error) {
			if err := ctx.Err(); err != nil {
				return Progress{}, err
			}

			var ready, scheduled, inflight, dlq int64
			for _, q := range qs {
				ready += int64(d.RunnableSize(q))
				scheduled += int64(d.ScheduledSize(q))
				inflight += int64(d.InflightSize(q))
				dlq += int64(d.DLQSize(q))
			}

			doneApprox := atomic.LoadInt64(&okCount)

			return Progress{
				Now:        now,
				Done:       doneApprox,
				DoneApprox: true,
				DLQ:        dlq,
				Ready:      ready,
				Scheduled:  scheduled,
				Inflight:   inflight,
			}, nil
		},
	}, nil
}
