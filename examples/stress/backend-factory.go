// examples/stress/backend_factory.go
package main

import (
	"context"
	"errors"
	"time"

	"github.com/ARJ2211/taskharbor/taskharbor/driver"
)

type backend struct {
	Driver   driver.Driver
	CloseFn  func() error
	ResetFn  func(context.Context) error
	ProgFn   func(context.Context, []string, time.Time) (Progress, error)
	DriverID string
}

func (b backend) Close() error                    { return b.CloseFn() }
func (b backend) Reset(ctx context.Context) error { return b.ResetFn(ctx) }
func (b backend) Progress(ctx context.Context, qs []string, now time.Time) (Progress, error) {
	return b.ProgFn(ctx, qs, now)
}

func newBackend(ctx context.Context, cfg Config) (backend, error) {
	switch cfg.DriverType {
	case "postgres":
		return newPostgresBackend(ctx, cfg)
	case "redis":
		return newRedisBackend(ctx, cfg)
	default:
		return backend{}, ErrInvalidDriver
	}
}

var ErrInvalidDriver = errors.New("invalid driver")
