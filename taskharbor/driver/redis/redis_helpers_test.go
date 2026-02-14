package redis

import (
	"context"
	"os"
	"strings"
	"testing"
)

// skip if no REDIS_ADDR; else return driver + unique queue so tests don't clash
func testRedis(t *testing.T) (*Driver, string) {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set, skipping Redis integration test")
	}
	ctx := context.Background()
	d, err := New(ctx, addr, DB(14), KeyPrefix("th-test"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	queue := "q-" + strings.ReplaceAll(t.Name(), "/", "_")
	return d, queue
}
