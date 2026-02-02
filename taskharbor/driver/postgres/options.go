package postgres

import (
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Option func(*pgxpool.Config)

/*
This option sets the max pool size.
*/
func WithMaxConns(n int32) Option {
	return func(cfg *pgxpool.Config) {
		cfg.MaxConns = n
	}
}

/*
This option sets the min pool size.
*/
func WithMinConns(n int32) Option {
	return func(cfg *pgxpool.Config) {
		cfg.MinConns = n
	}
}

/*
This option sets the maximum amount of time
a connection can be reused.
*/
func WithMaxConnLifetime(d time.Duration) Option {
	return func(cfg *pgxpool.Config) {
		cfg.MaxConnLifetime = d
	}
}

/*
This option sets how long an idle connection
may be kept before closing it.
*/
func WithMaxConnIdleTime(d time.Duration) Option {
	return func(cfg *pgxpool.Config) {
		cfg.MaxConnIdleTime = d
	}
}

/*
This option sets the duration or interval of
health checks of idle connections.
*/
func WithHealthCheckPeriod(d time.Duration) Option {
	return func(cfg *pgxpool.Config) {
		cfg.HealthCheckPeriod = d
	}
}
