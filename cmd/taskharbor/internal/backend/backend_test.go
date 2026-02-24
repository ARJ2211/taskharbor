package backend

import (
	"context"
	"os"
	"testing"

	"github.com/ARJ2211/taskharbor/cmd/taskharbor/internal/envutil"
)

func TestOpenMemory(t *testing.T) {
	h, err := Open(context.Background(), Config{Driver: "memory"})
	if err != nil {
		t.Fatalf("expected nil err, got %v", err)
	}
	if h == nil || h.Driver == nil {
		t.Fatalf("expected non-nil handle + driver")
	}
	if err := h.Close(); err != nil {
		t.Fatalf("expected nil close err, got %v", err)
	}
}

func TestOpenRedis(t *testing.T) {
	cwd, _ := os.Getwd()
	_ = envutil.LoadRepoDotenv(cwd)

	h, err := Open(context.Background(), Config{
		Driver: "redis", RedisAddr: os.Getenv("REDIS_ADDR"),
	})
	if err != nil {
		t.Fatalf("expected nil err, got %v", err)
	}
	if h == nil || h.Driver == nil {
		t.Fatalf("expected non-nil handle + driver")
	}
	if err := h.Close(); err != nil {
		t.Fatalf("expected nil close err, got %v", err)
	}
}

func TestOpenPostgres(t *testing.T) {
	cwd, _ := os.Getwd()
	_ = envutil.LoadRepoDotenv(cwd)

	h, err := Open(context.Background(), Config{
		Driver: "postgres", PostgresDSN: os.Getenv("TASKHARBOR_TEST_DSN"),
	})
	if err != nil {
		t.Fatalf("expected nil err, got %v", err)
	}
	if h == nil || h.Driver == nil {
		t.Fatalf("expected non-nil handle + driver")
	}
	if err := h.Close(); err != nil {
		t.Fatalf("expected nil close err, got %v", err)
	}
}

func TestOpenUnknownDriver(t *testing.T) {
	_, err := Open(context.Background(), Config{Driver: "nope"})
	if err == nil {
		t.Fatalf("expected error")
	}
}
