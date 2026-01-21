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
This function will register a handler for a perticular
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

	w.handlers[jobType] = h
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

		now := time.Now().UTC()

		rec, ok, err := w.driver.Reserve(ctx, w.queue, now)
		if err != nil {
			// If context was cancelled gracefully shut down
			if errors.Is(
				err, context.Canceled) || errors.Is(
				err, context.DeadlineExceeded) {
				break
			}
			return err
		}
		if !ok {
			// No runnable job. Sleep, but wake early if ctx is canceled.
			select {
			case <-ctx.Done():
				// stop loop, go wait on inflight
				break
			case <-time.After(w.cfg.PollInterval):
			}
			continue
		}
		// Concurrency limit
		sem <- struct{}{}
		wg.Add(1)

		go func(r driver.JobRecord) {
			defer wg.Done()
			defer func() { <-sem }()

			h := w.getHandler(r.Type)
			if h == nil {
				_ = w.driver.Fail(ctx, r.ID, "no handler registered for job type")
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

			err := h(ctx, job)
			if err == nil {
				_ = w.driver.Ack(ctx, r.ID)
				return
			}

			_ = w.driver.Fail(ctx, r.ID, fmt.Sprintf("handler error: %v", err))
		}(rec)
	}

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

// Errors
var (
	ErrHandlerRequired = errors.New("handler is required")
)
