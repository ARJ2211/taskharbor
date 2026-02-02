package postgres

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/ARJ2211/taskharbor/taskharbor/driver"
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

	if err := d.ensureOpen(); err != nil {
		return err
	}

	return ErrNotImplemented
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

	if err := d.ensureOpen(); err != nil {
		return driver.JobRecord{}, driver.Lease{}, false, err
	}

	return driver.JobRecord{}, driver.Lease{}, false, ErrNotImplemented
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
	if err := d.ensureOpen(); err != nil {
		return driver.Lease{}, err
	}

	return driver.Lease{}, ErrNotImplemented

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

	return ErrNotImplemented
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

	return ErrNotImplemented
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

	return ErrNotImplemented
}

var (
	// Keep message consistent with memory driver.
	ErrDriverClosed   = errors.New("driver is closed")
	ErrNilPool        = errors.New("nil pgx pool")
	ErrNotImplemented = errors.New("postgres driver: not implemented")
)
