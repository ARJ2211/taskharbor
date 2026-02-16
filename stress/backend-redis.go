// examples/stress/backend_redis.go
package main

import (
	"context"
	"strings"
	"time"

	"github.com/ARJ2211/taskharbor/taskharbor/driver"
	thredis "github.com/ARJ2211/taskharbor/taskharbor/driver/redis"
	goredis "github.com/redis/go-redis/v9"
)

func keyReady(prefix, queue string) string     { return prefix + ":queue:" + queue + ":ready" }
func keyScheduled(prefix, queue string) string { return prefix + ":queue:" + queue + ":scheduled" }
func keyInflight(prefix, queue string) string  { return prefix + ":queue:" + queue + ":inflight" }
func keyDlq(prefix, queue string) string       { return prefix + ":queue:" + queue + ":dlq" }

func deleteByPrefix(ctx context.Context, rdb *goredis.Client, prefix string) error {
	var cursor uint64
	pat := prefix + ":*"

	for {
		keys, next, err := rdb.Scan(ctx, cursor, pat, 1000).Result()
		if err != nil {
			return err
		}
		if len(keys) > 0 {
			pipe := rdb.Pipeline()
			for _, k := range keys {
				pipe.Del(ctx, k)
			}
			if _, err := pipe.Exec(ctx); err != nil {
				return err
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return nil
}

func newRedisBackend(ctx context.Context, cfg Config) (backend, error) {
	rdb := goredis.NewClient(&goredis.Options{
		Addr:     cfg.RedisAddr,
		DB:       cfg.RedisDB,
		PoolSize: cfg.RedisPoolSize,
	})
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return backend{}, err
	}

	d, err := thredis.NewWithClient(rdb, thredis.KeyPrefix(cfg.RedisPrefix))
	if err != nil {
		_ = rdb.Close()
		return backend{}, err
	}

	resetFn := func(ctx context.Context) error {
		switch strings.ToLower(strings.TrimSpace(cfg.RedisResetMode)) {
		case "flush":
			return rdb.FlushDB(ctx).Err()
		default:
			return deleteByPrefix(ctx, rdb, cfg.RedisPrefix)
		}
	}

	progFn := func(ctx context.Context, queues []string, now time.Time) (Progress, error) {
		var p Progress
		p.Now = now.UTC()
		p.Done = atomicLoadOKCount() // approx
		p.DoneApprox = true

		var readyCnt, schedCnt, inflightCnt, dlqCnt int64
		for _, q := range queues {
			rl, err := rdb.LLen(ctx, keyReady(cfg.RedisPrefix, q)).Result()
			if err != nil {
				return Progress{}, err
			}
			sc, err := rdb.ZCard(ctx, keyScheduled(cfg.RedisPrefix, q)).Result()
			if err != nil {
				return Progress{}, err
			}
			ic, err := rdb.ZCard(ctx, keyInflight(cfg.RedisPrefix, q)).Result()
			if err != nil {
				return Progress{}, err
			}
			dl, err := rdb.LLen(ctx, keyDlq(cfg.RedisPrefix, q)).Result()
			if err != nil {
				return Progress{}, err
			}
			readyCnt += rl
			schedCnt += sc
			inflightCnt += ic
			dlqCnt += dl
		}

		p.Ready = readyCnt
		p.Scheduled = schedCnt
		p.Inflight = inflightCnt
		p.DLQ = dlqCnt

		return p, nil
	}

	return backend{
		Driver:   driver.Driver(d),
		DriverID: "redis",
		CloseFn: func() error {
			clErr := d.Close()
			rdErr := rdb.Close()

			if clErr != nil {
				return clErr
			}
			if rdErr != nil {
				return rdErr
			}

			return nil
		},
		ResetFn: resetFn,
		ProgFn:  progFn,
	}, nil
}
