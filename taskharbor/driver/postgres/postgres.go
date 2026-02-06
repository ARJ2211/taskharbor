package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/ARJ2211/taskharbor/taskharbor/driver"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Driver struct {
	mu     sync.Mutex
	pool   *pgxpool.Pool
	closed bool
}

// Compile time check that we implement contract.
var _ driver.Driver = (*Driver)(nil)

/*
This function creates a postgres driver from a DSN and optional
pool config. This driver will own the pool and will Close() when
Driver.Close() is called.
*/
func New(ctx context.Context, dsn string, opts ...Option) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}

	for _, opt := range opts {
		if opt != nil {
			opt(cfg)
		}
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}

	// Ping test
	if err := pool.Ping(ctx); err != nil {
		return nil, err
	}

	var postgresDriver Driver = Driver{
		pool: pool,
	}
	return &postgresDriver, nil
}

/*
This function will wrap an existing pool. It will
extend Driver and still Close() the pool when
Driver.Close() will be called
*/
func NewWithPool(pool *pgxpool.Pool) (*Driver, error) {
	if pool != nil {
		var postgresDriver Driver = Driver{
			pool: pool,
		}
		return &postgresDriver, nil
	}
	return nil, ErrNilPool
}

/*
This function will close the PSQL connection pool.
*/
func (d *Driver) Close() error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}

	d.closed = true
	pool := d.pool
	d.pool = nil
	d.mu.Unlock()

	if pool != nil {
		pool.Close()
	}

	return nil
}

/*
This function will help
issue a new lease token.
*/
func newLeaseToken() (driver.LeaseToken, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return driver.LeaseToken(hex.EncodeToString(b)), nil
}

/*
This functino will validate if the pgSQL driver
is open. This will be used in all the interface
methods other than Close().
*/
func (d *Driver) ensureOpen() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return ErrDriverClosed
	}
	if d.pool == nil {
		return ErrNilPool
	}

	return nil
}

/*
This function will enqueue a new job record. This will
add a new job record into the jobs table and make the
job runnable or schedule it for later depending on
rec.RunAt
*/
func (d *Driver) Enqueue(ctx context.Context, rec driver.JobRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if err := rec.Validate(); err != nil {
		return err
	}

	if err := d.ensureOpen(); err != nil {
		return err
	}

	d.mu.Lock()
	closed := d.closed
	pool := d.pool
	d.mu.Unlock()

	if closed {
		return ErrDriverClosed
	}
	if pool == nil {
		return ErrNilPool
	}

	var runAt any
	if rec.RunAt.IsZero() {
		runAt = nil
	} else {
		runAt = rec.RunAt.UTC()
	}

	var failedAt any
	if rec.FailedAt.IsZero() {
		failedAt = nil
	} else {
		failedAt = rec.FailedAt.UTC()
	}

	var idemKey any
	if strings.TrimSpace(rec.IdempotencyKey) == "" {
		idemKey = nil
	} else {
		idemKey = rec.IdempotencyKey
	}

	_, err := pool.Exec(ctx, QEnqueue,
		rec.ID,
		rec.Type,
		rec.Queue,
		rec.Payload,
		runAt,
		rec.Timeout.Nanoseconds(),
		rec.CreatedAt.UTC(),
		rec.Attempts,
		rec.MaxAttempts,
		rec.LastError,
		failedAt,
		"ready",
		idemKey,
	)
	if err != nil {
		return err
	}

	return nil
}

/*
This function will select the next runnable job for the
given queue and atomically lease it. If no job is there
to be ran, return false
*/
func (d *Driver) Reserve(
	ctx context.Context,
	queue string,
	now time.Time,
	leaseFor time.Duration) (
	driver.JobRecord, driver.Lease, bool, error,
) {
	if err := ctx.Err(); err != nil {
		return driver.JobRecord{}, driver.Lease{}, false, err
	}

	if leaseFor <= 0 {
		return driver.JobRecord{}, driver.Lease{}, false,
			driver.ErrInvalidLeaseDuration
	}

	if err := d.ensureOpen(); err != nil {
		return driver.JobRecord{}, driver.Lease{}, false, err
	}

	if strings.TrimSpace(queue) == "" {
		return driver.JobRecord{}, driver.Lease{}, false, driver.ErrQueueRequired
	}

	tok, err := newLeaseToken()
	if err != nil {
		return driver.JobRecord{}, driver.Lease{}, false, err
	}

	now = now.UTC()
	expiresAt := now.Add(leaseFor).UTC()

	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return driver.JobRecord{}, driver.Lease{}, false, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	var (
		id             string
		typ            string
		q              string
		payload        []byte
		runAtPtr       *time.Time
		timeoutNanos   int64
		idemPtr        *string
		createdAt      time.Time
		attempts       int
		maxAttempts    int
		lastError      string
		failedAtPtr    *time.Time
		leaseTokStr    string
		leaseExpiresAt time.Time
	)

	err = tx.QueryRow(ctx, QReserve, queue, now, string(tok), expiresAt).Scan(
		&id,
		&typ,
		&q,
		&payload,
		&runAtPtr,
		&timeoutNanos,
		&idemPtr,
		&createdAt,
		&attempts,
		&maxAttempts,
		&lastError,
		&failedAtPtr,
		&leaseTokStr,
		&leaseExpiresAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return driver.JobRecord{}, driver.Lease{}, false, nil
		}
		return driver.JobRecord{}, driver.Lease{}, false, err
	}

	rec := driver.JobRecord{
		ID:             id,
		Type:           typ,
		Queue:          q,
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

	lease := driver.Lease{
		Token:     driver.LeaseToken(leaseTokStr),
		ExpiresAt: leaseExpiresAt.UTC(),
	}

	if err := tx.Commit(ctx); err != nil {
		return driver.JobRecord{}, driver.Lease{}, false, err
	}

	return rec, lease, true, nil
}

/*
This function is required to extend the lease of an inflight
job. On success set the extended duration of the lease.

This should validate:
  - the job is currently inflgiht
  - the lease token matches.
  - the lease as not expired at "now"
*/
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

	now = now.UTC()
	newExpiry := now.Add(leaseFor).UTC()

	var updated time.Time
	err := d.pool.QueryRow(ctx, QExtendLeaseAtomic, newExpiry, id, string(token), now).Scan(&updated)
	if err == nil {
		updatedLease := driver.Lease{
			Token:     token,
			ExpiresAt: updated.UTC(),
		}
		return updatedLease, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return driver.Lease{}, err
	}

	// No rows updated: classify error to match precedence:
	// not inflight -> expired -> mismatch
	var status string
	var dbTok *string
	var dbExp *time.Time

	err2 := d.pool.QueryRow(ctx, QLeaseState, id).Scan(&status, &dbTok, &dbExp)
	if err2 != nil {
		if errors.Is(err2, pgx.ErrNoRows) {
			return driver.Lease{}, driver.ErrJobNotInflight
		}
		return driver.Lease{}, err2
	}

	if status != "inflight" || dbTok == nil || dbExp == nil {
		return driver.Lease{}, driver.ErrJobNotInflight
	}
	if !dbExp.UTC().After(now) {
		return driver.Lease{}, driver.ErrLeaseExpired
	}
	if driver.LeaseToken(*dbTok) != token {
		return driver.Lease{}, driver.ErrLeaseMismatch
	}

	// If we get here, it was a race (someone changed it between attempts).
	return driver.Lease{}, driver.ErrJobNotInflight
}

/*
This function will Ack successful completion of a leased
job. This job will be moved to a terminal state.
*/
func (d *Driver) Ack(
	ctx context.Context,
	id string,
	token driver.LeaseToken,
	now time.Time,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if err := d.ensureOpen(); err != nil {
		return err
	}

	now = now.UTC()

	var ignored string
	err := d.pool.QueryRow(
		ctx,
		QAckAtomic,
		id,
		string(token),
		now,
	).Scan(&ignored)

	if err == nil {
		return nil
	}

	if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}

	var status string
	var dbTok *string
	var dbExp *time.Time

	err2 := d.pool.QueryRow(ctx, QAckState, id).Scan(&status, &dbTok, &dbExp)
	if err2 != nil {
		if errors.Is(err2, pgx.ErrNoRows) {
			return driver.ErrJobNotInflight
		}
		return err2
	}

	if status != "inflight" || dbTok == nil || dbExp == nil {
		return driver.ErrJobNotInflight
	}
	if !dbExp.UTC().After(now) {
		return driver.ErrLeaseExpired
	}
	if driver.LeaseToken(*dbTok) != token {
		return driver.ErrLeaseMismatch
	}

	// Race fallback
	return driver.ErrJobNotInflight
}

/*
This function will move a failed leased job into a runnable
or a scheduled queue for later attempt
*/
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

	now = now.UTC()

	var runAt any
	if upd.RunAt.IsZero() {
		runAt = nil
	} else {
		runAt = upd.RunAt.UTC()
	}

	var failedAt any
	if upd.FailedAt.IsZero() {
		failedAt = nil
	} else {
		failedAt = upd.FailedAt.UTC()
	}

	// Atomic success path
	var ignored string
	err := d.pool.QueryRow(ctx, QRetryAtomic,
		id,
		string(token),
		now,
		runAt,
		upd.Attempts,
		upd.LastError,
		failedAt,
	).Scan(&ignored)

	if err == nil {
		return nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}

	// 0 rows updated: classify error (not inflight -> expired -> mismatch)
	var status string
	var dbTok *string
	var dbExp *time.Time

	err2 := d.pool.QueryRow(ctx, QRetryState, id).Scan(&status, &dbTok, &dbExp)
	if err2 != nil {
		if errors.Is(err2, pgx.ErrNoRows) {
			return driver.ErrJobNotInflight
		}
		return err2
	}

	if status != "inflight" || dbTok == nil || dbExp == nil {
		return driver.ErrJobNotInflight
	}
	if !dbExp.UTC().After(now) {
		return driver.ErrLeaseExpired
	}
	if driver.LeaseToken(*dbTok) != token {
		return driver.ErrLeaseMismatch
	}

	return driver.ErrJobNotInflight
}

/*
This function will move a failed job (after retrying) into
a dead letter queue. This should also validate the lease ownership
so that only one worker may add it to a DLQ
*/
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

	now = now.UTC()
	if reason == "" {
		reason = "failed"
	}

	var ignored string
	err := d.pool.QueryRow(
		ctx,
		QFailAtomic,
		id,
		string(token),
		now,
		reason,
		now,
	).Scan(&ignored)
	if err == nil {
		return nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}

	var status string
	var dbTok *string
	var dbExp *time.Time

	err2 := d.pool.QueryRow(ctx, QFailState, id).Scan(&status, &dbTok, &dbExp)
	if err2 != nil {
		if errors.Is(err2, pgx.ErrNoRows) {
			return driver.ErrJobNotInflight
		}
		return err2
	}

	if status != "inflight" || dbTok == nil || dbExp == nil {
		return driver.ErrJobNotInflight
	}
	if !dbExp.UTC().After(now) {
		return driver.ErrLeaseExpired
	}
	if driver.LeaseToken(*dbTok) != token {
		return driver.ErrLeaseMismatch
	}

	return driver.ErrJobNotInflight
}

var (
	// Keep message consistent with memory driver.
	ErrDriverClosed   = errors.New("driver is closed")
	ErrNilPool        = errors.New("nil pgx pool")
	ErrNotImplemented = errors.New("postgres driver: not implemented")
)
