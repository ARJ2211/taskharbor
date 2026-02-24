package redis

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ARJ2211/taskharbor/taskharbor/driver"
	"github.com/redis/go-redis/v9"
)

type hgetAllCmd interface {
	Result() (map[string]string, error)
}

var _ driver.Admin = (*Driver)(nil)

func (d *Driver) keyReady(queue string) string { return d.opts.prefix + ":queue:" + queue + ":ready" }
func (d *Driver) keyScheduled(queue string) string {
	return d.opts.prefix + ":queue:" + queue + ":scheduled"
}
func (d *Driver) keyInflight(queue string) string {
	return d.opts.prefix + ":queue:" + queue + ":inflight"
}
func (d *Driver) keyDLQ(queue string) string  { return d.opts.prefix + ":queue:" + queue + ":dlq" }
func (d *Driver) keyDone(queue string) string { return d.opts.prefix + ":queue:" + queue + ":done" }

type jobMeta struct {
	rec       driver.JobRecord
	status    string
	leaseTok  string
	leaseExp  int64
	dlqReason string
	dlqFail   int64
}

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

	m, err := d.loadJobMeta(ctx, id)
	if err != nil {
		return driver.JobInfo{}, err
	}

	state := stateFromRedis(now, m.status, m.rec.RunAt)

	var lease *driver.LeaseInfo
	if state == driver.StateInflight && m.leaseTok != "" && m.leaseExp != 0 {
		lease = &driver.LeaseInfo{
			Token:     driver.LeaseToken(m.leaseTok),
			ExpiresAt: time.Unix(0, m.leaseExp).UTC(),
		}
	}

	// Important: always return DLQ info when state is DLQ (even if dlq_failed_at_nano isn't stored).
	var dlq *driver.DLQInfo
	if state == driver.StateDLQ {
		failedAt := time.Time{}
		if m.dlqFail != 0 {
			failedAt = time.Unix(0, m.dlqFail).UTC()
		} else if !m.rec.FailedAt.IsZero() {
			failedAt = m.rec.FailedAt.UTC()
		}
		dlq = &driver.DLQInfo{
			Reason:   m.dlqReason,
			FailedAt: failedAt,
		}
	}

	return driver.JobInfo{
		Record: m.rec,
		State:  state,
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

	cap := req.Limit * 10
	if cap < 200 {
		cap = 200
	}
	if cap > 2000 {
		cap = 2000
	}

	ids, err := d.collectIDsForList(ctx, req, cap)
	if err != nil {
		return driver.ListPage{}, err
	}
	if len(ids) == 0 {
		return driver.ListPage{Jobs: nil, NextCursor: ""}, nil
	}

	metas, err := d.loadManyJobMeta(ctx, ids)
	if err != nil {
		return driver.ListPage{}, err
	}

	jobs := make([]driver.JobSummary, 0, len(ids))
	for _, id := range ids {
		m, ok := metas[id]
		if !ok {
			continue
		}
		st := stateFromRedis(req.Now, m.status, m.rec.RunAt)
		if req.State != "" && st != req.State {
			continue
		}
		jobs = append(jobs, summaryFromMeta(req.Now, st, m))
	}

	sort.Slice(jobs, func(i, j int) bool {
		ai := jobs[i].CreatedAt.UnixNano()
		aj := jobs[j].CreatedAt.UnixNano()
		if ai != aj {
			return ai < aj
		}
		return jobs[i].ID < jobs[j].ID
	})

	if strings.TrimSpace(req.Cursor) != "" {
		out := jobs[:0]
		for _, s := range jobs {
			a := s.CreatedAt.UnixNano()
			if a > cur.A || (a == cur.A && s.ID > cur.ID) {
				out = append(out, s)
			}
		}
		jobs = out
	}

	if len(jobs) == 0 {
		return driver.ListPage{Jobs: nil, NextCursor: ""}, nil
	}
	if len(jobs) <= req.Limit {
		return driver.ListPage{Jobs: jobs, NextCursor: ""}, nil
	}

	page := jobs[:req.Limit]
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

	runAtNano := int64(0)
	runAtSec := int64(0)
	runAtMember := ""

	if !opt.RunAt.IsZero() && opt.RunAt.UTC().After(now) {
		ru := opt.RunAt.UTC()
		runAtNano = ru.UnixNano()
		runAtSec = ru.Unix()
		runAtMember = schedMember(int64(ru.Nanosecond()), id)
	}

	queueGuard := strings.TrimSpace(opt.Queue)

	code, err := d.runRequeueDLQScript(
		ctx,
		id,
		now.UnixNano(),
		queueGuard,
		runAtNano,
		runAtSec,
		runAtMember,
		opt.ResetAttempts,
	)
	if err != nil {
		return err
	}

	switch code {
	case 1:
		return nil
	case 0:
		return driver.ErrJobNotFound
	case -1:
		return driver.ErrJobNotDLQ
	case -2:
		return fmt.Errorf("queue mismatch for job %s", id)
	default:
		return driver.ErrJobNotDLQ
	}
}

func stateFromRedis(now time.Time, status string, runAt time.Time) driver.JobState {
	switch status {
	case "inflight":
		return driver.StateInflight
	case "dlq":
		return driver.StateDLQ
	case "done":
		return driver.StateDone
	default:
		// ready/scheduled stored status is not authoritative; run_at decides.
		if runAt.IsZero() || !runAt.After(now) {
			return driver.StateReady
		}
		return driver.StateScheduled
	}
}

func summaryFromMeta(now time.Time, st driver.JobState, m jobMeta) driver.JobSummary {
	s := driver.JobSummary{
		ID:          m.rec.ID,
		Type:        m.rec.Type,
		Queue:       m.rec.Queue,
		State:       st,
		RunAt:       m.rec.RunAt,
		CreatedAt:   m.rec.CreatedAt,
		Timeout:     m.rec.Timeout,
		Attempts:    m.rec.Attempts,
		MaxAttempts: m.rec.MaxAttempts,
		LastError:   m.rec.LastError,
		FailedAt:    m.rec.FailedAt,
	}

	if m.leaseExp != 0 {
		s.LeaseExpiresAt = time.Unix(0, m.leaseExp).UTC()
	}
	if m.dlqReason != "" {
		s.DLQReason = m.dlqReason
	}
	if m.dlqFail != 0 {
		s.DLQFailedAt = time.Unix(0, m.dlqFail).UTC()
	}

	return s
}

func (d *Driver) loadJobMeta(ctx context.Context, id string) (jobMeta, error) {
	jobKey := d.keyJob(id)
	m, err := d.client.HGetAll(ctx, jobKey).Result()
	if err != nil {
		return jobMeta{}, err
	}
	if len(m) == 0 {
		return jobMeta{}, driver.ErrJobNotFound
	}

	rec := driver.JobRecord{ID: id}
	rec.Type = m["type"]
	rec.Queue = m["queue"]
	if v, ok := m["payload"]; ok {
		rec.Payload = []byte(v)
	}
	rec.IdempotencyKey = m["idempotency_key"]
	rec.LastError = m["last_error"]

	if v := m["run_at_nano"]; v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n != 0 {
			rec.RunAt = time.Unix(0, n).UTC()
		}
	}
	if v := m["timeout_nano"]; v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			rec.Timeout = time.Duration(n)
		}
	}
	if v := m["created_at_nano"]; v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			rec.CreatedAt = time.Unix(0, n).UTC()
		}
	}
	if v := m["failed_at_nano"]; v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n != 0 {
			rec.FailedAt = time.Unix(0, n).UTC()
		}
	}
	if v := m["attempts"]; v != "" {
		rec.Attempts, _ = strconv.Atoi(v)
	}
	if v := m["max_attempts"]; v != "" {
		rec.MaxAttempts, _ = strconv.Atoi(v)
	}

	meta := jobMeta{
		rec:       rec,
		status:    m["status"],
		leaseTok:  m["lease_token"],
		dlqReason: m["dlq_reason"],
	}

	if v := m["lease_expires_at_nano"]; v != "" {
		meta.leaseExp, _ = strconv.ParseInt(v, 10, 64)
	}
	if v := m["dlq_failed_at_nano"]; v != "" {
		meta.dlqFail, _ = strconv.ParseInt(v, 10, 64)
	}

	// Fallback for older/legacy Redis schema: DLQ time/reason might live in failed_at_nano/last_error.
	if meta.status == "dlq" {
		if meta.dlqReason == "" {
			meta.dlqReason = rec.LastError
		}
		if meta.dlqFail == 0 && !rec.FailedAt.IsZero() {
			meta.dlqFail = rec.FailedAt.UnixNano()
		}
	}

	return meta, nil
}

func (d *Driver) loadManyJobMeta(ctx context.Context, ids []string) (map[string]jobMeta, error) {
	pipe := d.client.Pipeline()
	cmds := make(map[string]hgetAllCmd, len(ids))

	for _, id := range ids {
		cmds[id] = pipe.HGetAll(ctx, d.keyJob(id))
	}

	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return nil, err
	}

	out := make(map[string]jobMeta, len(ids))
	for id, cmd := range cmds {
		m, err := cmd.Result()
		if err != nil || len(m) == 0 {
			continue
		}

		rec := driver.JobRecord{ID: id}
		rec.Type = m["type"]
		rec.Queue = m["queue"]
		if v, ok := m["payload"]; ok {
			rec.Payload = []byte(v)
		}
		rec.IdempotencyKey = m["idempotency_key"]
		rec.LastError = m["last_error"]

		if v := m["run_at_nano"]; v != "" {
			if n, err := strconv.ParseInt(v, 10, 64); err == nil && n != 0 {
				rec.RunAt = time.Unix(0, n).UTC()
			}
		}
		if v := m["timeout_nano"]; v != "" {
			if n, err := strconv.ParseInt(v, 10, 64); err == nil {
				rec.Timeout = time.Duration(n)
			}
		}
		if v := m["created_at_nano"]; v != "" {
			if n, err := strconv.ParseInt(v, 10, 64); err == nil {
				rec.CreatedAt = time.Unix(0, n).UTC()
			}
		}
		if v := m["failed_at_nano"]; v != "" {
			if n, err := strconv.ParseInt(v, 10, 64); err == nil && n != 0 {
				rec.FailedAt = time.Unix(0, n).UTC()
			}
		}
		if v := m["attempts"]; v != "" {
			rec.Attempts, _ = strconv.Atoi(v)
		}
		if v := m["max_attempts"]; v != "" {
			rec.MaxAttempts, _ = strconv.Atoi(v)
		}

		meta := jobMeta{
			rec:       rec,
			status:    m["status"],
			leaseTok:  m["lease_token"],
			dlqReason: m["dlq_reason"],
		}
		if v := m["lease_expires_at_nano"]; v != "" {
			meta.leaseExp, _ = strconv.ParseInt(v, 10, 64)
		}
		if v := m["dlq_failed_at_nano"]; v != "" {
			meta.dlqFail, _ = strconv.ParseInt(v, 10, 64)
		}

		// same fallback in bulk path
		if meta.status == "dlq" {
			if meta.dlqReason == "" {
				meta.dlqReason = rec.LastError
			}
			if meta.dlqFail == 0 && !rec.FailedAt.IsZero() {
				meta.dlqFail = rec.FailedAt.UnixNano()
			}
		}

		out[id] = meta
	}

	return out, nil
}

func (d *Driver) collectIDsForList(ctx context.Context, req driver.ListRequest, cap int) ([]string, error) {
	queue := req.Queue

	add := func(dst []string, ids ...string) []string {
		for _, id := range ids {
			if id != "" {
				dst = append(dst, id)
			}
		}
		return dst
	}

	uniq := make(map[string]struct{})
	out := make([]string, 0, cap)

	pushUnique := func(id string) {
		if id == "" {
			return
		}
		if _, ok := uniq[id]; ok {
			return
		}
		uniq[id] = struct{}{}
		out = append(out, id)
	}

	pushMany := func(ids []string) {
		for _, id := range ids {
			if len(out) >= cap {
				return
			}
			pushUnique(id)
		}
	}

	switch req.State {
	case driver.StateReady:
		ids, err := d.client.LRange(ctx, d.keyReady(queue), 0, int64(cap-1)).Result()
		if err != nil && err != redis.Nil {
			return nil, err
		}
		pushMany(ids)

		due, err := d.dueScheduledIDs(ctx, queue, req.Now, cap-len(out))
		if err != nil {
			return nil, err
		}
		pushMany(due)

	case driver.StateScheduled:
		fut, err := d.futureScheduledIDs(ctx, queue, req.Now, cap)
		if err != nil {
			return nil, err
		}
		pushMany(fut)

	case driver.StateInflight:
		ids, err := d.client.ZRange(ctx, d.keyInflight(queue), 0, int64(cap-1)).Result()
		if err != nil && err != redis.Nil {
			return nil, err
		}
		pushMany(ids)

	case driver.StateDLQ:
		ids, err := d.client.LRange(ctx, d.keyDLQ(queue), 0, int64(cap-1)).Result()
		if err != nil && err != redis.Nil {
			return nil, err
		}
		pushMany(ids)

	case driver.StateDone:
		ids, err := d.client.LRange(ctx, d.keyDone(queue), 0, int64(cap-1)).Result()
		if err != nil && err != redis.Nil {
			return nil, err
		}
		pushMany(ids)

	case "":
		ids, err := d.client.LRange(ctx, d.keyReady(queue), 0, int64(cap-1)).Result()
		if err != nil && err != redis.Nil {
			return nil, err
		}
		pushMany(ids)

		sched, err := d.client.ZRange(ctx, d.keyScheduled(queue), 0, int64(cap-1)).Result()
		if err != nil && err != redis.Nil {
			return nil, err
		}
		for _, m := range sched {
			if len(out) >= cap {
				break
			}
			if _, id, ok := strings.Cut(m, ":"); ok {
				pushUnique(id)
			}
		}

		inflight, err := d.client.ZRange(ctx, d.keyInflight(queue), 0, int64(cap-1)).Result()
		if err != nil && err != redis.Nil {
			return nil, err
		}
		pushMany(inflight)

		dlq, err := d.client.LRange(ctx, d.keyDLQ(queue), 0, int64(cap-1)).Result()
		if err != nil && err != redis.Nil {
			return nil, err
		}
		pushMany(dlq)

		done, err := d.client.LRange(ctx, d.keyDone(queue), 0, int64(cap-1)).Result()
		if err != nil && err != redis.Nil {
			return nil, err
		}
		pushMany(done)

	default:
		out = add(out)
	}

	return out, nil
}

func (d *Driver) dueScheduledIDs(ctx context.Context, queue string, now time.Time, cap int) ([]string, error) {
	if cap <= 0 {
		return nil, nil
	}
	nowSec := now.Unix()
	nowSub := int64(now.Nanosecond())
	skey := d.keyScheduled(queue)

	out := make([]string, 0, cap)

	// < nowSec
	if nowSec-1 >= 0 {
		members, err := d.client.ZRangeByScore(ctx, skey, &redis.ZRangeBy{
			Min:   "0",
			Max:   strconv.FormatInt(nowSec-1, 10),
			Count: int64(cap),
		}).Result()
		if err != nil && err != redis.Nil {
			return nil, err
		}
		for _, m := range members {
			if len(out) >= cap {
				break
			}
			if _, id, ok := strings.Cut(m, ":"); ok {
				out = append(out, id)
			}
		}
	}

	// == nowSec (filter by sub)
	if len(out) < cap {
		members, err := d.client.ZRangeByScore(ctx, skey, &redis.ZRangeBy{
			Min: strconv.FormatInt(nowSec, 10),
			Max: strconv.FormatInt(nowSec, 10),
		}).Result()
		if err != nil && err != redis.Nil {
			return nil, err
		}
		for _, m := range members {
			if len(out) >= cap {
				break
			}
			if len(m) < 10 {
				continue
			}
			sub, _ := strconv.ParseInt(m[:9], 10, 64)
			if sub <= nowSub {
				if _, id, ok := strings.Cut(m, ":"); ok {
					out = append(out, id)
				}
			}
		}
	}

	return out, nil
}

func (d *Driver) futureScheduledIDs(ctx context.Context, queue string, now time.Time, cap int) ([]string, error) {
	if cap <= 0 {
		return nil, nil
	}
	nowSec := now.Unix()
	nowSub := int64(now.Nanosecond())
	skey := d.keyScheduled(queue)

	out := make([]string, 0, cap)

	// > nowSec
	members, err := d.client.ZRangeByScore(ctx, skey, &redis.ZRangeBy{
		Min:   strconv.FormatInt(nowSec+1, 10),
		Max:   "+inf",
		Count: int64(cap),
	}).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}
	for _, m := range members {
		if len(out) >= cap {
			break
		}
		if _, id, ok := strings.Cut(m, ":"); ok {
			out = append(out, id)
		}
	}

	// == nowSec (filter by sub > nowSub)
	if len(out) < cap {
		members, err := d.client.ZRangeByScore(ctx, skey, &redis.ZRangeBy{
			Min: strconv.FormatInt(nowSec, 10),
			Max: strconv.FormatInt(nowSec, 10),
		}).Result()
		if err != nil && err != redis.Nil {
			return nil, err
		}
		for _, m := range members {
			if len(out) >= cap {
				break
			}
			if len(m) < 10 {
				continue
			}
			sub, _ := strconv.ParseInt(m[:9], 10, 64)
			if sub > nowSub {
				if _, id, ok := strings.Cut(m, ":"); ok {
					out = append(out, id)
				}
			}
		}
	}

	return out, nil
}
