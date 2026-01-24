package taskharbor

import (
	"testing"
	"time"
)

type dummyCodec struct{}

func (d dummyCodec) Marshal(v any) ([]byte, error)   { return []byte("x"), nil }
func (d dummyCodec) Unmarshal(b []byte, v any) error { return nil }

func TestOptions_Defaults(t *testing.T) {
	cfg := applyOptions()

	if cfg.Codec == nil {
		t.Fatalf("expected default codec, got nil")
	}

	if cfg.Concurrency != 4 {
		t.Fatalf("expected default concurrency 4, got %d", cfg.Concurrency)
	}

	if cfg.PollInterval != 200*time.Millisecond {
		t.Fatalf("expected default poll interval 200ms, got %v", cfg.PollInterval)
	}

	if cfg.DefaultQueue != DefaultQueue {
		t.Fatalf("expected default queue %s, got %s", DefaultQueue, cfg.DefaultQueue)
	}
}

func TestOptions_Overrides(t *testing.T) {
	cfg := applyOptions(
		WithCodec(dummyCodec{}),
		WithConcurrency(9),
		WithPollInterval(50*time.Millisecond),
		WithDefaultQueue("critical"),
	)

	if _, ok := cfg.Codec.(dummyCodec); !ok {
		t.Fatalf("expected dummy codec override")
	}

	if cfg.Concurrency != 9 {
		t.Fatalf("expected concurrency 9, got %d", cfg.Concurrency)
	}

	if cfg.PollInterval != 50*time.Millisecond {
		t.Fatalf("expected poll interval 50ms, got %v", cfg.PollInterval)
	}

	if cfg.DefaultQueue != "critical" {
		t.Fatalf("expected default queue critical, got %s", cfg.DefaultQueue)
	}
}

func TestOptions_Normalization(t *testing.T) {
	cfg := applyOptions(
		WithCodec(nil),
		WithConcurrency(0),
		WithPollInterval(0),
		WithDefaultQueue(""),
	)

	if cfg.Codec == nil {
		t.Fatalf("expected codec normalized to default, got nil")
	}

	if cfg.Concurrency != 1 {
		t.Fatalf("expected concurrency normalized to 1, got %d", cfg.Concurrency)
	}

	if cfg.PollInterval != 200*time.Millisecond {
		t.Fatalf("expected poll interval normalized to 200ms, got %v", cfg.PollInterval)
	}

	if cfg.DefaultQueue != DefaultQueue {
		t.Fatalf("expected default queue normalized to %s, got %s", DefaultQueue, cfg.DefaultQueue)
	}
}
