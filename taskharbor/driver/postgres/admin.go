package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ARJ2211/taskharbor/taskharbor/driver"
	"github.com/jackc/pgx/v5"
)

var _ driver.Admin = (*Driver)(nil)

const qAdminInspect = `
SELECT
  id,
  type,
  queue,
  payload,
  run_at,
  timeout_nanos,
  idempotency_key,
  created_at,
  attempts,
  max_attempts,
  last_error,
  failed_at,
  status,
  lease_token,
  lease_expires_at,
  dlq_reason,
  dlq_failed_at
FROM th_jobs
WHERE id = $1
`

const qAdminGetStatus = `
SELECT queue, status
FROM th_jobs
WHERE id = $1
`

const qAdminRequeueDLQ = `
UPDATE th_jobs
SET
  status = 'ready',
  run_at = $2,
  dlq_reason = NULL,
  dlq_failed_at = NULL,
  lease_token = NULL,
  lease_expires_at = NULL,
  attempts = CASE WHEN $3 THEN 0 ELSE attempts END,
  last_error = CASE WHEN $3 THEN '' ELSE last_error END,
  failed_at = CASE WHEN $3 THEN NULL ELSE failed_at END
WHERE id = $1
  AND status = 'dlq'
  AND ($4::text IS NULL OR queue = $4)
RETURNING id
`

func (d *Driver) Inspect(ctx context.Context, id string, now time.Time) (driver.JobInfo, error) {
	if err := ctx.Err(); err != nil {
		return driver.JobInfo{}, err
	}
	if err := d.ensureOpen(); err != nil {
		return driver.JobInfo{}, err
	}

	id = strings.TrimSpace(id)
	if id == "" {
		return driver.JobInfo{}, driver.ErrJobNotFound
	}
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}

	var (
		dbID           string
		typ            string
		queue          string
		payload        []byte
		runAtPtr       *time.Time
		timeoutNanos   int64
		idemPtr        *string
		createdAt      time.Time
		attempts       int
		maxAttempts    int
		lastError      string
		failedAtPtr    *time.Time
		status         string
		leaseTokPtr    *string
		leaseExpPtr    *time.Time
		dlqReasonPtr   *string
		dlqFailedAtPtr *time.Time
	)

	err := d.pool.QueryRow(ctx, qAdminInspect, id).Scan(
		&dbID,
		&typ,
		&queue,
		&payload,
		&runAtPtr,
		&timeoutNanos,
		&idemPtr,
		&createdAt,
		&attempts,
		&maxAttempts,
		&lastError,
		&failedAtPtr,
		&status,
		&leaseTokPtr,
		&leaseExpPtr,
		&dlqReasonPtr,
		&dlqFailedAtPtr,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return driver.JobInfo{}, driver.ErrJobNotFound
		}
		return driver.JobInfo{}, err
	}

	rec := driver.JobRecord{
		ID:             dbID,
		Type:           typ,
		Queue:          queue,
		Payload:        payload,
		RunAt:          time.Time{},
		Timeout:        time.Duration(timeoutNanos),
		IdempotencyKey: "",
		CreatedAt:      createdAt.UTC(),
		Attempts:       attempts,
		MaxAttempts:    maxAttempts,
		LastError:      lastError,
		FailedAt:       time.Time{},
	}

	if runAtPtr != nil {
		rec.RunAt = runAtPtr.UTC()
	}
	if failedAtPtr != nil {
		rec.FailedAt = failedAtPtr.UTC()
	}
	if idemPtr != nil {
		rec.IdempotencyKey = *idemPtr
	}

	st := stateFromDB(status, runAtPtr, now)

	var lease *driver.LeaseInfo
	if status == "inflight" && leaseTokPtr != nil && leaseExpPtr != nil {
		lease = &driver.LeaseInfo{
			Token:     driver.LeaseToken(*leaseTokPtr),
			ExpiresAt: leaseExpPtr.UTC(),
		}
	}

	var dlq *driver.DLQInfo
	if status == "dlq" && dlqFailedAtPtr != nil {
		reason := ""
		if dlqReasonPtr != nil {
			reason = *dlqReasonPtr
		}
		dlq = &driver.DLQInfo{
			Reason:   reason,
			FailedAt: dlqFailedAtPtr.UTC(),
		}
	}

	return driver.JobInfo{
		Record: rec,
		State:  st,
		Lease:  lease,
		DLQ:    dlq,
	}, nil
}

func (d *Driver) List(ctx context.Context, req driver.ListRequest) (driver.ListPage, error) {
	if err := ctx.Err(); err != nil {
		return driver.ListPage{}, err
	}
	if err := d.ensureOpen(); err != nil {
		return driver.ListPage{}, err
	}

	req.Queue = strings.TrimSpace(req.Queue)
	if req.Queue == "" {
		return driver.ListPage{}, fmt.Errorf("queue is required")
	}
	if req.Now.IsZero() {
		req.Now = time.Now().UTC()
	} else {
		req.Now = req.Now.UTC()
	}
	if req.Limit <= 0 {
		req.Limit = 50
	}

	var cur driver.Cursor
	if strings.TrimSpace(req.Cursor) != "" {
		c, err := driver.DecodeCursor(req.Cursor)
		if err != nil {
			return driver.ListPage{}, err
		}
		cur = c
	}

	sql, args := buildAdminListQuery(req, cur)

	rows, err := d.pool.Query(ctx, sql, args...)
	if err != nil {
		return driver.ListPage{}, err
	}
	defer rows.Close()

	out := make([]driver.JobSummary, 0, req.Limit+1)
	for rows.Next() {
		var (
			id           string
			typ          string
			queue        string
			runAtPtr     *time.Time
			timeoutNanos int64
			createdAt    time.Time
			attempts     int
			maxAttempts  int
			lastError    string
			failedAtPtr  *time.Time
			status       string
			leaseExpPtr  *time.Time
			dlqReasonPtr *string
			dlqFailedPtr *time.Time
		)

		if err := rows.Scan(
			&id,
			&typ,
			&queue,
			&runAtPtr,
			&timeoutNanos,
			&createdAt,
			&attempts,
			&maxAttempts,
			&lastError,
			&failedAtPtr,
			&status,
			&leaseExpPtr,
			&dlqReasonPtr,
			&dlqFailedPtr,
		); err != nil {
			return driver.ListPage{}, err
		}

		s := driver.JobSummary{
			ID:          id,
			Type:        typ,
			Queue:       queue,
			RunAt:       time.Time{},
			CreatedAt:   createdAt.UTC(),
			Timeout:     time.Duration(timeoutNanos),
			Attempts:    attempts,
			MaxAttempts: maxAttempts,
			LastError:   lastError,
			FailedAt:    time.Time{},
			State:       stateFromDB(status, runAtPtr, req.Now),
		}

		if runAtPtr != nil {
			s.RunAt = runAtPtr.UTC()
		}
		if failedAtPtr != nil {
			s.FailedAt = failedAtPtr.UTC()
		}
		if leaseExpPtr != nil {
			s.LeaseExpiresAt = leaseExpPtr.UTC()
		}
		if dlqReasonPtr != nil {
			s.DLQReason = *dlqReasonPtr
		}
		if dlqFailedPtr != nil {
			s.DLQFailedAt = dlqFailedPtr.UTC()
		}

		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		return driver.ListPage{}, err
	}

	if len(out) == 0 {
		return driver.ListPage{Jobs: nil, NextCursor: ""}, nil
	}

	if len(out) <= req.Limit {
		return driver.ListPage{Jobs: out, NextCursor: ""}, nil
	}

	page := out[:req.Limit]
	last := page[len(page)-1]
	next := driver.EncodeCursor(driver.Cursor{
		State: req.State,
		A:     last.CreatedAt.UnixNano(),
		ID:    last.ID,
	})

	return driver.ListPage{Jobs: page, NextCursor: next}, nil
}

func (d *Driver) RequeueDLQ(ctx context.Context, id string, now time.Time, opt driver.RequeueOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := d.ensureOpen(); err != nil {
		return err
	}

	id = strings.TrimSpace(id)
	if id == "" {
		return driver.ErrJobNotFound
	}
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}

	var runAt any
	if opt.RunAt.IsZero() || !opt.RunAt.After(now) {
		runAt = nil
	} else {
		runAt = opt.RunAt.UTC()
	}

	var queueGuard any
	if strings.TrimSpace(opt.Queue) == "" {
		queueGuard = nil
	} else {
		queueGuard = opt.Queue
	}

	var ignored string
	err := d.pool.QueryRow(ctx, qAdminRequeueDLQ, id, runAt, opt.ResetAttempts, queueGuard).Scan(&ignored)
	if err == nil {
		return nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}

	// classify
	var q string
	var status string
	err2 := d.pool.QueryRow(ctx, qAdminGetStatus, id).Scan(&q, &status)
	if err2 != nil {
		if errors.Is(err2, pgx.ErrNoRows) {
			return driver.ErrJobNotFound
		}
		return err2
	}

	if opt.Queue != "" && q != opt.Queue {
		return fmt.Errorf("queue mismatch: job is in %q, not %q", q, opt.Queue)
	}

	if status != "dlq" {
		return driver.ErrJobNotDLQ
	}

	return driver.ErrJobNotDLQ
}

func stateFromDB(status string, runAt *time.Time, now time.Time) driver.JobState {
	switch status {
	case "ready":
		if runAt == nil || !runAt.UTC().After(now) {
			return driver.StateReady
		}
		return driver.StateScheduled
	case "inflight":
		return driver.StateInflight
	case "dlq":
		return driver.StateDLQ
	case "done":
		return driver.StateDone
	default:
		// safest fallback
		if runAt == nil || !runAt.UTC().After(now) {
			return driver.StateReady
		}
		return driver.StateScheduled
	}
}

func buildAdminListQuery(req driver.ListRequest, cur driver.Cursor) (string, []any) {
	// We use stable ordering: created_at ASC, id ASC
	// Cursor is (created_at, id) > (cursorTime, cursorID)
	args := make([]any, 0, 6)

	where := "WHERE queue = $1"
	args = append(args, req.Queue)
	p := 2

	needNow := false

	switch req.State {
	case "":
		// no filter
	case driver.StateReady:
		where += fmt.Sprintf(" AND status = 'ready' AND (run_at IS NULL OR run_at <= $%d)", p)
		args = append(args, req.Now)
		p++
		needNow = true
	case driver.StateScheduled:
		where += fmt.Sprintf(" AND status = 'ready' AND run_at > $%d", p)
		args = append(args, req.Now)
		p++
		needNow = true
	case driver.StateInflight:
		where += " AND status = 'inflight'"
	case driver.StateDLQ:
		where += " AND status = 'dlq'"
	case driver.StateDone:
		where += " AND status = 'done'"
	default:
		// unknown state filter: return empty page rather than surprise
		where += " AND 1=0"
	}

	if strings.TrimSpace(req.Cursor) != "" {
		cursorTime := time.Unix(0, cur.A).UTC()
		where += fmt.Sprintf(" AND (created_at > $%d OR (created_at = $%d AND id > $%d))", p, p, p+1)
		args = append(args, cursorTime, cur.ID)
		p += 2
	}

	limit := req.Limit + 1
	args = append(args, limit)

	// select fields needed for JobSummary
	sql := `
SELECT
  id,
  type,
  queue,
  run_at,
  timeout_nanos,
  created_at,
  attempts,
  max_attempts,
  last_error,
  failed_at,
  status,
  lease_expires_at,
  dlq_reason,
  dlq_failed_at
FROM th_jobs
` + where + `
ORDER BY created_at ASC, id ASC
LIMIT $` + fmt.Sprintf("%d", p)

	_ = needNow // just to make intent obvious while editing

	return sql, args
}
