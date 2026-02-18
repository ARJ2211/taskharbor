package redis

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ARJ2211/taskharbor/conformance"
	"github.com/ARJ2211/taskharbor/taskharbor/driver"
	"github.com/ARJ2211/taskharbor/taskharbor/internal/envutil"
	goredis "github.com/redis/go-redis/v9"
)

func TestConformance(t *testing.T) {
	wd, _ := os.Getwd()
	_ = envutil.LoadRepoDotenv(wd)

	addr := strings.TrimSpace(os.Getenv("REDIS_ADDR"))
	if addr == "" {
		t.Skip("REDIS_ADDR not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := goredis.NewClient(&goredis.Options{Addr: addr})
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("redis ping failed: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	prefixFor := func(t *testing.T) string {
		s := t.Name()
		s = strings.ReplaceAll(s, "/", "_")
		s = strings.ReplaceAll(s, " ", "_")
		s = strings.ReplaceAll(s, ":", "_")
		return "taskharbor_test:" + s
	}

	deleteByPrefix := func(t *testing.T, prefix string) {
		ctx := context.Background()
		var cursor uint64
		pat := prefix + "*"
		for {
			keys, next, err := client.Scan(ctx, cursor, pat, 1000).Result()
			if err != nil {
				t.Fatalf("redis scan failed: %v", err)
			}
			if len(keys) > 0 {
				if err := client.Del(ctx, keys...).Err(); err != nil {
					t.Fatalf("redis del failed: %v", err)
				}
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
	}

	f := conformance.Factory{
		New: func(t *testing.T) driver.Driver {
			prefix := prefixFor(t)
			d, err := NewWithClient(client, KeyPrefix(prefix))
			if err != nil {
				t.Fatalf("new redis driver failed: %v", err)
			}
			return d
		},
		Cleanup: func(t *testing.T) {
			deleteByPrefix(t, prefixFor(t))
		},
	}

	conformance.Run(t, f, conformance.All()...)
}
