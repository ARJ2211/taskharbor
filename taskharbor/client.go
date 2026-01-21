package taskharbor

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"

	"github.com/ARJ2211/taskharbor/taskharbor/driver"
)

/*
Client will be responsible for queueing the jobs
in the selected backend. This will validate requests,
applies default, encodes payloads, and calls the
driver.Enqueue method and add the job in the respective
queue [runnable or scheduled].
*/
type Client struct {
	driver driver.Driver
	cfg    Config
}

/*
This function will create a new client with the
appropriate driver and config option.
*/
func NewClient(d driver.Driver, opts ...Option) *Client {
	cfg := applyOptions(opts...)
	client := Client{
		driver: d,
		cfg:    cfg,
	}
	return &client
}

/*
This function will create and store a job. If RunAt is 0,
the job will run immediately else the job is scheduled
in the min-heap.
*/
func (c *Client) Enqueue(ctx context.Context, req JobRequest) (JobId, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}

	if err := req.Validate(); err != nil {
		return "", err
	}

	req = req.WithDefaults()
	if req.Queue == "" {
		req.Queue = c.cfg.DefaultQueue
	}

	id := newJobId()

	codec := c.cfg.Codec
	payloadBytes, err := codec.Marshal(req.Payload)
	if err != nil {
		return "", err
	}

	now := time.Now().UTC()

	rec := driver.JobRecord{
		ID:             string(id),
		Type:           req.Type,
		Payload:        payloadBytes,
		Queue:          req.Queue,
		RunAt:          req.RunAt,
		Timeout:        req.Timeout,
		IdempotencyKey: req.IdempotencyKey,
		CreatedAt:      now,
		Attempts:       0,
	}

	if err := c.driver.Enqueue(ctx, rec); err != nil {
		return "", err
	}

	return id, nil
}

/*
Helper function to create random Job ID.
*/
func newJobId() JobId {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return JobId(hex.EncodeToString(b))
}
