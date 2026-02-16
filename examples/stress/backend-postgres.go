// examples/stress/backend_postgres.go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/ARJ2211/taskharbor/taskharbor/driver/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
)

func newPostgresBackend(ctx context.Context, cfg Config) (backend, error) {
	pcfg, err := pgxpool.ParseConfig(cfg.PostgresDSN)
	if err != nil {
		return backend{}, err
	}
	pcfg.MaxConns = int32(cfg.PostgresMaxConns)

	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return backend{}, err
	}

	if err := postgres.ApplyMigrations(ctx, pool); err != nil {
		pool.Close()
		return backend{}, err
	}

	d, err := postgres.NewWithPool(pool)
	if err != nil {
		pool.Close()
		return backend{}, err
	}

	resetFn := func(ctx context.Context) error {
		_, err := pool.Exec(ctx, `TRUNCATE th_jobs`)
		return err
	}

	progFn := func(ctx context.Context, _ []string, now time.Time) (Progress, error) {
		var p Progress
		p.Now = now.UTC()
		err := pool.QueryRow(ctx, `
			SELECT
			  COUNT(*) FILTER (WHERE status='done')::bigint,
			  COUNT(*) FILTER (WHERE status='dlq')::bigint,
			  COUNT(*) FILTER (WHERE status='ready' AND (run_at IS NULL OR run_at <= $1))::bigint,
			  COUNT(*) FILTER (WHERE status='ready' AND run_at > $1)::bigint,
			  COUNT(*) FILTER (WHERE status='inflight')::bigint
			FROM th_jobs
		`, now.UTC()).Scan(&p.Done, &p.DLQ, &p.Ready, &p.Scheduled, &p.Inflight)
		return p, err
	}

	return backend{
		Driver:   d,
		DriverID: "postgres",
		CloseFn: func() error {
			_ = d.Close()
			pool.Close()
			return nil
		},
		ResetFn: resetFn,
		ProgFn:  progFn,
	}, nil
}

func (b backend) String() string {
	return fmt.Sprintf("backend(%s)", b.DriverID)
}
