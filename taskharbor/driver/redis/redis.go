package redis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ARJ2211/taskharbor/taskharbor/driver"
	"github.com/redis/go-redis/v9"
)

// Redis driver for TaskHarbor. Jobs in hashes, queues use lists + sorted sets. Lua for atomic reserve/reclaim.
var _ driver.Driver = (*Driver)(nil)

type Driver struct {
	mu        sync.Mutex
	client    redis.UniversalClient
	opts      options
	closed    bool
	ownClient bool // we created it in New() so we're allowed to close it
}

// New connects to Redis at addr (e.g. "localhost:6379"). We own the client and close it on Close().
func New(ctx context.Context, addr string, opts ...Option) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	o := applyOptions(opts...)
	roc := &redis.Options{Addr: addr, DB: o.db}
	for _, fn := range o.clientOpts {
		fn(roc)
	}
	client := redis.NewClient(roc)
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, err
	}
	return &Driver{
		client:    client,
		opts:      o,
		ownClient: true,
	}, nil
}

// NewWithClient wraps an existing client. Caller keeps ownership — we don't close it.
func NewWithClient(client redis.UniversalClient, opts ...Option) (*Driver, error) {
	if client == nil {
		return nil, ErrNilClient
	}
	o := applyOptions(opts...)
	return &Driver{
		client:    client,
		opts:      o,
		ownClient: false,
	}, nil
}

func (d *Driver) Close() error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	d.closed = true
	client := d.client
	ownClient := d.ownClient
	d.client = nil
	d.mu.Unlock()
	if ownClient && client != nil {
		return client.Close()
	}
	return nil
}

func (d *Driver) ensureOpen() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return ErrDriverClosed
	}
	if d.client == nil {
		return ErrDriverClosed
	}
	return nil
}

// Per-queue we keep four structures: ready (list), scheduled (zset), inflight (zset), dlq (list).
// key helpers — everything prefixed so you can share one Redis instance
func (d *Driver) keyJob(id string) string      { return d.opts.prefix + ":job:" + id }
func (d *Driver) keyReady(queue string) string { return d.opts.prefix + ":queue:" + queue + ":ready" }
func (d *Driver) keyScheduled(queue string) string {
	return d.opts.prefix + ":queue:" + queue + ":scheduled"
}
func (d *Driver) keyInflight(queue string) string {
	return d.opts.prefix + ":queue:" + queue + ":inflight"
}
func (d *Driver) keyDlq(queue string) string {
	return d.opts.prefix + ":queue:" + queue + ":dlq"
}

func newLeaseToken() (driver.LeaseToken, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return driver.LeaseToken(hex.EncodeToString(b)), nil
}

// Enqueue: atomically write job hash and add to ready list or scheduled zset (single Lua script).
// Returns (rec.ID, false, nil) on success. Idempotency dedupe (existed=true) not implemented yet.
func (d *Driver) Enqueue(ctx context.Context, rec driver.JobRecord) (string, bool, error) {
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	if err := rec.Validate(); err != nil {
		return "", false, err
	}
	if err := d.ensureOpen(); err != nil {
		return "", false, err
	}

	runAtNano := int64(0)
	if !rec.RunAt.IsZero() {
		runAtNano = rec.RunAt.UTC().UnixNano()
	}
	createdAtNano := rec.CreatedAt.UTC().UnixNano()
	failedAtNano := int64(0)
	if !rec.FailedAt.IsZero() {
		failedAtNano = rec.FailedAt.UTC().UnixNano()
	}

	err := d.runEnqueueScript(ctx, &rec, runAtNano, createdAtNano, failedAtNano)
	if err != nil {
		return "", false, err
	}
	return rec.ID, false, nil
}

// Reserve: reclaim expired, promote due scheduled, pop one from ready, lease it. ok=false = nothing to do.
func (d *Driver) Reserve(
	ctx context.Context,
	queue string,
	now time.Time,
	leaseFor time.Duration,
) (driver.JobRecord, driver.Lease, bool, error) {
	if err := ctx.Err(); err != nil {
		return driver.JobRecord{}, driver.Lease{}, false, err
	}
	if leaseFor <= 0 {
		return driver.JobRecord{}, driver.Lease{}, false, driver.ErrInvalidLeaseDuration
	}
	if strings.TrimSpace(queue) == "" {
		return driver.JobRecord{}, driver.Lease{}, false, driver.ErrQueueRequired
	}
	if err := d.ensureOpen(); err != nil {
		return driver.JobRecord{}, driver.Lease{}, false, err
	}

	tok, err := newLeaseToken()
	if err != nil {
		return driver.JobRecord{}, driver.Lease{}, false, err
	}
	nowNano := now.UTC().UnixNano()
	expiresAt := now.Add(leaseFor).UTC()
	expiresAtNano := expiresAt.UnixNano()

	id, err := d.runReserveScript(ctx, queue, nowNano, string(tok), expiresAtNano)
	if err != nil {
		return driver.JobRecord{}, driver.Lease{}, false, err
	}
	if id == "" {
		return driver.JobRecord{}, driver.Lease{}, false, nil
	}

	rec, err := d.loadJob(ctx, id)
	if err != nil {
		return driver.JobRecord{}, driver.Lease{}, false, err
	}
	// worker sees it as "run now"
	rec.RunAt = time.Time{}
	lease := driver.Lease{Token: tok, ExpiresAt: expiresAt}
	return rec, lease, true, nil
}

// loadJob loads one job by exact key (prefix:job:id). HGETALL returns all fields of that single hash
// in one round-trip — we are not scanning keys; the key is known (e.g. from reserve). O(fields) for one hash.
func (d *Driver) loadJob(ctx context.Context, id string) (driver.JobRecord, error) {
	jobKey := d.keyJob(id)
	m, err := d.client.HGetAll(ctx, jobKey).Result()
	if err != nil {
		return driver.JobRecord{}, err
	}
	if len(m) == 0 {
		return driver.JobRecord{}, driver.ErrJobNotFound
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
	return rec, nil
}

// ExtendLease: bump expiry. Job must be inflight, token must match, lease not expired at now.
func (d *Driver) ExtendLease(
	ctx context.Context,
	id string,
	token driver.LeaseToken,
	now time.Time,
	leaseFor time.Duration,
) (driver.Lease, error) {
	if err := ctx.Err(); err != nil {
		return driver.Lease{}, err
	}
	if leaseFor <= 0 {
		return driver.Lease{}, driver.ErrInvalidLeaseDuration
	}
	if err := d.ensureOpen(); err != nil {
		return driver.Lease{}, err
	}
	nowNano := now.UTC().UnixNano()
	newExpiresAt := now.Add(leaseFor).UTC()
	newExpiresAtNano := newExpiresAt.UnixNano()

	updated, err := d.runExtendLeaseScript(ctx, id, string(token), nowNano, newExpiresAtNano)
	if err != nil {
		return driver.Lease{}, err
	}
	if updated {
		return driver.Lease{Token: token, ExpiresAt: newExpiresAt}, nil
	}
	return driver.Lease{}, d.classifyLeaseError(ctx, id, token, now)
}

// Ack: mark job done (status=done, clear lease), remove from inflight. Job hash is kept for inspect/done (parity with postgres).
func (d *Driver) Ack(ctx context.Context, id string, token driver.LeaseToken, now time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := d.ensureOpen(); err != nil {
		return err
	}
	nowNano := now.UTC().UnixNano()
	code, err := d.runAckScript(ctx, id, string(token), nowNano)
	if err != nil {
		return err
	}
	switch code {
	case 1:
		return nil
	case 2:
		return driver.ErrJobNotInflight
	case 3:
		return driver.ErrLeaseMismatch
	case 4:
		return driver.ErrLeaseExpired
	}
	return d.classifyLeaseError(ctx, id, token, now)
}

// Retry: put job back on ready/scheduled with new attempt info. Core decides when; we just persist.
func (d *Driver) Retry(
	ctx context.Context,
	id string,
	token driver.LeaseToken,
	now time.Time,
	upd driver.RetryUpdate,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := d.ensureOpen(); err != nil {
		return err
	}
	nowNano := now.UTC().UnixNano()
	runAtNano := int64(0)
	if !upd.RunAt.IsZero() {
		runAtNano = upd.RunAt.UTC().UnixNano()
	}
	failedAtNano := int64(0)
	if !upd.FailedAt.IsZero() {
		failedAtNano = upd.FailedAt.UTC().UnixNano()
	}

	ok, err := d.runRetryScript(ctx, id, string(token), nowNano, runAtNano, upd.Attempts, upd.LastError, failedAtNano)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}
	return d.classifyLeaseError(ctx, id, token, now)
}

// Fail: move to DLQ + store reason. Core calls this when max attempts or unrecoverable.
func (d *Driver) Fail(
	ctx context.Context,
	id string,
	token driver.LeaseToken,
	now time.Time,
	reason string,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := d.ensureOpen(); err != nil {
		return err
	}
	if reason == "" {
		reason = "failed"
	}
	nowNano := now.UTC().UnixNano()
	ok, err := d.runFailScript(ctx, id, string(token), nowNano, reason, nowNano)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}
	return d.classifyLeaseError(ctx, id, token, now)
}

// classifyLeaseError: retry/fail/extend Lua scripts only return 0 on failure; they don't say why.
// We read the job hash once and return the appropriate error (not inflight → expired → wrong token),
// in the same order as the postgres and memory drivers for consistent behavior.
func (d *Driver) classifyLeaseError(ctx context.Context, id string, token driver.LeaseToken, now time.Time) error {
	jobKey := d.keyJob(id)
	status, _ := d.client.HGet(ctx, jobKey, "status").Result()
	dbTok, _ := d.client.HGet(ctx, jobKey, "lease_token").Result()
	dbExpNano, _ := d.client.HGet(ctx, jobKey, "lease_expires_at_nano").Result()

	if status != "inflight" {
		return driver.ErrJobNotInflight
	}
	if dbTok == "" || dbExpNano == "" {
		return driver.ErrJobNotInflight
	}
	expNano, _ := strconv.ParseInt(dbExpNano, 10, 64)
	expAt := time.Unix(0, expNano).UTC()
	if !expAt.After(now) {
		return driver.ErrLeaseExpired
	}
	if driver.LeaseToken(dbTok) != token {
		return driver.ErrLeaseMismatch
	}
	return driver.ErrJobNotInflight
}

var (
	ErrDriverClosed = errors.New("driver is closed")
	ErrNilClient    = errors.New("redis client is nil")
)
