package driver

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

type JobState string

const (
	StateReady     JobState = "ready"     // runnable now (due)
	StateScheduled JobState = "scheduled" // not due yet (run_at > now)
	StateInflight  JobState = "inflight"  // leased
	StateDone      JobState = "done"      // terminal success
	StateDLQ       JobState = "dlq"       // terminal failure
)

var (
	ErrAdminUnsupported = errors.New("admin interface not supported by driver")
	ErrInvalidCursor    = errors.New("invalid cursor")
	ErrJobNotDLQ        = errors.New("job is not in dlq")
)

type LeaseInfo struct {
	Token     LeaseToken
	ExpiresAt time.Time
}

type DLQInfo struct {
	Reason   string
	FailedAt time.Time
}

type JobInfo struct {
	Record JobRecord
	State  JobState

	Lease *LeaseInfo
	DLQ   *DLQInfo
}

type JobSummary struct {
	ID          string
	Type        string
	Queue       string
	State       JobState
	RunAt       time.Time
	CreatedAt   time.Time
	Timeout     time.Duration
	Attempts    int
	MaxAttempts int
	LastError   string
	FailedAt    time.Time

	LeaseExpiresAt time.Time
	DLQReason      string
	DLQFailedAt    time.Time
}

type ListRequest struct {
	Queue  string   // required
	State  JobState // optional; empty means "all"
	Now    time.Time
	Limit  int
	Cursor string
}

type ListPage struct {
	Jobs       []JobSummary
	NextCursor string
}

type RequeueOptions struct {
	Queue         string    // optional guard; if set and mismatch, driver should error
	RunAt         time.Time // zero => immediate
	ResetAttempts bool      // if true: set attempts=0 and clear last_error/failed_at
}

// Admin is optional and used by CLI/dev tooling. Worker + Client must not depend on this.
type Admin interface {
	Inspect(ctx context.Context, id string, now time.Time) (JobInfo, error)
	List(ctx context.Context, req ListRequest) (ListPage, error)
	RequeueDLQ(ctx context.Context, id string, now time.Time, opt RequeueOptions) error
}

// Cursor is an opaque pagination token with a standard encoding.
// A/B/ID meaning depends on State and the driver’s chosen ordering.
type Cursor struct {
	V     int      `json:"v"`
	State JobState `json:"s"`
	A     int64    `json:"a"`
	B     int64    `json:"b,omitempty"`
	ID    string   `json:"id"`
}

func EncodeCursor(c Cursor) string {
	c.V = 1
	b, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(b)
}

func DecodeCursor(s string) (Cursor, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return Cursor{}, nil
	}
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return Cursor{}, fmt.Errorf("%w: %v", ErrInvalidCursor, err)
	}
	var c Cursor
	if err := json.Unmarshal(b, &c); err != nil {
		return Cursor{}, fmt.Errorf("%w: %v", ErrInvalidCursor, err)
	}
	if c.V != 1 {
		return Cursor{}, fmt.Errorf("%w: unsupported cursor version", ErrInvalidCursor)
	}
	if strings.TrimSpace(c.ID) == "" {
		return Cursor{}, fmt.Errorf("%w: missing id", ErrInvalidCursor)
	}
	return c, nil
}
