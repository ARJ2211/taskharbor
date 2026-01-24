/*
.						***********                  ***********
.					*****************            *****************
.					*********************        *********************
.					***********************      ***********************
.					************************    ************************
.					*************************  *************************
.					**************************************************
.					************************************************
.						********************************************
.						****************************************
.							**********************************
.							******************************
.								************************
.									********************
.									**************
.										**********
.										******
.											**

*/

package taskharbor

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/ARJ2211/taskharbor/taskharbor/driver"
)

/*
The worker will poll the driver, dispatch jobs to the handlers,
and calls the Ack/Fail on completion or termination.
*/
type Worker struct {
	driver   driver.Driver
	cfg      Config
	mu       sync.RWMutex
	handlers map[string]Handler
	queue    string
}

/*
This function will create a new worker with the provided
driver and options to be able to start handling jobs.
*/
func NewWorker(d driver.Driver, opts ...Option) *Worker {
	cfg := applyOptions(opts...)
	worker := Worker{
		driver:   d,
		cfg:      cfg,
		handlers: make(map[string]Handler),
		queue:    cfg.DefaultQueue,
	}
	return &worker
}

/*
This function is used to wrap our handlers around
the different middlewares.
*/
func (w *Worker) wrapHandler(h Handler) Handler {
	// Add the default middlewares
	// 1. RecoverMiddleware to handle panics.
	// 2. TimeoutMiddleware to handle timeouts.
	middlewares := []Middleware{RecoverMiddleware(), TimeoutMiddleware()}

	// Add any user middlewares
	middlewares = append(middlewares, w.cfg.Middlewares...)

	for i := len(middlewares) - 1; i >= 0; i-- {
		h = middlewares[i](h)
	}
	return h
}

/*
This function will register a handler for a particular
worker as a key-value pair so the worker knows what
handler to call when a job is sent to it on its queue.
*/
func (w *Worker) Register(jobType string, h Handler) error {
	if strings.TrimSpace(jobType) == "" {
		return ErrJobTypeRequired
	}

	if h == nil {
		return ErrHandlerRequired
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	w.handlers[jobType] = w.wrapHandler(h)
	return nil
}

/*
This function will start the WORKER LOOP until the context is
cancelled. IMPLEMENT A GRACEFUL SHUTDOWN TO AWAIT ANY INFLIGHT
HANDLERS CURRENTLY IN PROGRESS TO FINISH!
*/
func (w *Worker) Run(ctx context.Context) error {
	sem := make(chan struct{}, w.cfg.Concurrency)
	var wg sync.WaitGroup

	for {
		if err := ctx.Err(); err != nil {
			break
		}

		now := w.cfg.Clock.Now()

		rec, lease, ok, err := w.driver.Reserve(ctx, w.queue, now, w.cfg.LeaseDuration)
		if err != nil {
			// If context was cancelled gracefully shut down
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				break
			}
			return err
		}
		if !ok {
			// No runnable job. Sleep, but wake early if ctx is canceled.
			select {
			case <-ctx.Done():
				// stop loop, go wait on inflight
				goto shutdown
			case <-time.After(w.cfg.PollInterval):
			}
			continue
		}

		// Concurrency limit
		sem <- struct{}{}
		wg.Add(1)

		go func(r driver.JobRecord, lease driver.Lease) {
			defer wg.Done()
			defer func() { <-sem }()

			drvCtx := context.WithoutCancel(ctx)

			h := w.getHandler(r.Type)
			if h == nil {
				_ = w.driver.Fail(
					drvCtx,
					r.ID,
					lease.Token,
					w.cfg.Clock.Now(),
					"no handler registered for job type: "+r.Type,
				)
				return
			}

			job := Job{
				ID:        JobId(r.ID),
				Type:      r.Type,
				Payload:   r.Payload,
				Queue:     r.Queue,
				RunAt:     r.RunAt,
				Timeout:   r.Timeout,
				CreatedAt: r.CreatedAt,
			}

			// Belt-and-suspenders panic safety: middleware should catch panics,
			// but this ensures a panic never kills the worker goroutine.
			err := func() (err error) {
				defer func() {
					if v := recover(); v != nil {
						err = PanicError{Value: v, Stack: debug.Stack()}
					}
				}()
				return h(ctx, job)
			}()

			if err == nil {
				_ = w.driver.Ack(drvCtx, r.ID, lease.Token, w.cfg.Clock.Now())
				return
			}

			// ERROR PATH BELOW:
			now := w.cfg.Clock.Now().UTC()
			reason := fmt.Sprintf("handler error: %v", err)

			// Unrecoverable means DLQ immediately.
			if IsUnrecoverable(err) {
				_ = w.driver.Fail(drvCtx, r.ID, lease.Token, w.cfg.Clock.Now(), reason)
				return
			}

			maxAttempts := w.maxAttemptsFor(r)

			// If no policy configured, straight to DLQ
			if w.cfg.RetryPolicy == nil || maxAttempts <= 0 {
				_ = w.driver.Fail(drvCtx, r.ID, lease.Token, w.cfg.Clock.Now(), reason)
				return
			}

			// Check if we have any available attempts
			nextAttempts := r.Attempts + 1
			if nextAttempts >= maxAttempts {
				_ = w.driver.Fail(drvCtx, r.ID, lease.Token, w.cfg.Clock.Now(), reason)
				return
			}

			delay := w.cfg.RetryPolicy.NextDelay(nextAttempts)
			nextRunAt := now.Add(delay)

			upd := driver.RetryUpdate{
				RunAt:     nextRunAt,
				Attempts:  nextAttempts,
				LastError: reason,
				FailedAt:  now,
			}
			_ = w.driver.Retry(drvCtx, r.ID, lease.Token, w.cfg.Clock.Now(), upd)
		}(rec, lease)
	}

shutdown:
	wg.Wait()
	return nil
}

/*
This function fetches a handler for a job type.
*/
func (w *Worker) getHandler(jobType string) Handler {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.handlers[jobType]
}

/*
This function returns the max attempts for a job.
*/
func (w *Worker) maxAttemptsFor(rec driver.JobRecord) int {
	if rec.MaxAttempts > 0 {
		return rec.MaxAttempts
	}

	if w.cfg.RetryPolicy == nil {
		return 0
	}

	return w.cfg.RetryPolicy.MaxAttempts()
}

// Errors
var (
	ErrHandlerRequired = errors.New("handler is required")
)
