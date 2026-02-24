package backend

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/ARJ2211/taskharbor/taskharbor/driver"
	"github.com/ARJ2211/taskharbor/taskharbor/driver/memory"
	"github.com/ARJ2211/taskharbor/taskharbor/driver/postgres"
	"github.com/ARJ2211/taskharbor/taskharbor/driver/redis"
)

type Config struct {
	Driver string

	PostgresDSN string
	RedisAddr   string
}

type Handle struct {
	Driver  driver.Driver
	closeFn func() error
}

func (h *Handle) Close() error {
	if h == nil {
		return nil
	}
	if h.closeFn != nil {
		return h.closeFn()
	}
	if h.Driver == nil {
		return nil
	}
	return h.Driver.Close()
}

var (
	memMu     sync.Mutex
	sharedMem *memory.Driver
)

func Open(ctx context.Context, cfg Config) (*Handle, error) {
	switch strings.ToLower(strings.TrimSpace(cfg.Driver)) {
	case "", "memory":
		memMu.Lock()
		if sharedMem == nil {
			sharedMem = memory.New()
		}
		d := sharedMem
		memMu.Unlock()

		// memory is in-process only; do not close it between commands
		return &Handle{
			Driver:  d,
			closeFn: func() error { return nil },
		}, nil

	case "postgres":
		if strings.TrimSpace(cfg.PostgresDSN) == "" {
			return nil, fmt.Errorf("postgres requires --dsn (or TH_PG_DSN)")
		}
		d, err := postgres.New(ctx, cfg.PostgresDSN)
		if err != nil {
			return nil, err
		}
		return &Handle{Driver: d}, nil

	case "redis":
		if strings.TrimSpace(cfg.RedisAddr) == "" {
			return nil, fmt.Errorf("redis requires --redis-addr (or TH_REDIS_ADDR)")
		}
		d, err := redis.New(ctx, cfg.RedisAddr)
		if err != nil {
			return nil, err
		}
		return &Handle{Driver: d}, nil

	default:
		return nil, fmt.Errorf("unknown driver: %s (expected memory|postgres|redis)", cfg.Driver)
	}
}
