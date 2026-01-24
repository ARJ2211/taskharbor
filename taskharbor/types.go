package taskharbor

import (
	"errors"
	"strings"
	"time"
)

type JobId string

const DefaultQueue = "default"

/*
This is the user facing input for enqueueing work.
The payload is empty so that callers or producers can
pass structs directly. This will be then encoded into
bytes using the codec.
*/
type JobRequest struct {
	Type           string        // Defines which handler takes the job
	Payload        any           // Defines the payload that the user sends for processing
	Queue          string        // Defines the name of the queue to run the task in
	RunAt          time.Time     // Defines the scheduled time to run the Job at (Default 0)
	Timeout        time.Duration // Defines the timeout (TODO: MILESTONE 3)
	IdempotencyKey string        // Facilitate idempotency (TODO: MILESTONE 6)
	MaxAttempts    int           // Total number of retries allowed (0 default)
}

/*
This is the stable representation of the Job Request that
the handlers receive. Payload will be stored in bytes
*/
type Job struct {
	ID        JobId
	Type      string
	Payload   []byte
	Queue     string
	RunAt     time.Time
	Timeout   time.Duration
	CreatedAt time.Time
}

/*
This function validates the Job Request coming
in that will be passed on to the handler.
*/
func (j JobRequest) Validate() error {
	if strings.TrimSpace(j.Type) == "" {
		return ErrJobTypeRequired
	}

	if j.Timeout < 0 {
		return ErrNegativeTimeout
	}

	if j.MaxAttempts < 0 {
		return ErrNegativeMaxAttempts
	}
	return nil
}

/*
This function returns a copy of the request with
some defaults applied
*/
func (j JobRequest) WithDefaults() JobRequest {
	var out JobRequest = j
	if strings.TrimSpace(out.Queue) == "" {
		out.Queue = DefaultQueue
	}
	return out
}

// Errors
var (
	ErrJobTypeRequired     = errors.New("job type is required")
	ErrNegativeTimeout     = errors.New("timeout cannot be negative")
	ErrNegativeMaxAttempts = errors.New("max attempts cannot be negative")
)
