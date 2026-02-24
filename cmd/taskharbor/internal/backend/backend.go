package backend

import (
	"context"
	"fmt"
	"strings"

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
	Driver driver.Driver
}

func (h *Handle) Close() error {
	if h == nil || h.Driver == nil {
		return nil
	}
	if err := h.Driver.Close(); err != nil {
		return err
	}
	return nil
}

/*
This function opens a new driver based on the configs and
arguments provided by the user. Default driver: memory.
*/
func Open(ctx context.Context, cfg Config) (*Handle, error) {
	ds := strings.ToLower(strings.TrimSpace(cfg.Driver))
	switch ds {
	case "", "memory":
		memDrvHnd := Handle{Driver: memory.New()}
		return &memDrvHnd, nil
	case "postgres":
		if strings.TrimSpace(cfg.PostgresDSN) == "" {
			return nil, fmt.Errorf("postgres requires --dsn (or TH_PG_DSN)")
		}
		d, err := postgres.New(ctx, cfg.PostgresDSN)
		if err != nil {
			return nil, err
		}
		psqlDrvHnd := Handle{Driver: d}
		return &psqlDrvHnd, nil
	case "redis":
		if strings.TrimSpace(cfg.RedisAddr) == "" {
			return nil, fmt.Errorf("redis requires --redis-addr (or TH_REDIS_ADDR)")
		}
		d, err := redis.New(ctx, cfg.RedisAddr)
		if err != nil {
			return nil, err
		}
		redisDrvHnd := Handle{Driver: d}
		return &redisDrvHnd, nil
	default:
		return nil, fmt.Errorf(
			"unknown driver: %s (expected memory|postgres|redis)",
			cfg.Driver,
		)
	}
}
