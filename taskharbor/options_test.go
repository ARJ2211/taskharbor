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

	if cfg.LeaseDuration != 30*time.Second {
		t.Fatalf("expected default lease duration 30s, got %v", cfg.LeaseDuration)
	}

	if cfg.HeartbeatInterval != 10*time.Second {
		t.Fatalf("expected default heartbeat interval 10s, got %v", cfg.HeartbeatInterval)
	}
}

func TestOptions_Overrides(t *testing.T) {
	cfg := applyOptions(
		WithCodec(dummyCodec{}),
		WithConcurrency(9),
		WithPollInterval(50*time.Millisecond),
		WithDefaultQueue("critical"),
		WithLeaseDuration(12*time.Second),
		WithHeartbeatInterval(2*time.Second),
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

	if cfg.LeaseDuration != 12*time.Second {
		t.Fatalf("expected lease duration 12s, got %v", cfg.LeaseDuration)
	}

	if cfg.HeartbeatInterval != 2*time.Second {
		t.Fatalf("expected heartbeat interval 2s, got %v", cfg.HeartbeatInterval)
	}
}

func TestOptions_Normalization(t *testing.T) {
	cfg := applyOptions(
		WithCodec(nil),
		WithConcurrency(0),
		WithPollInterval(0),
		WithDefaultQueue(""),
		WithLeaseDuration(0),
		WithHeartbeatInterval(0),
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

	if cfg.LeaseDuration != 30*time.Second {
		t.Fatalf("expected lease duration normalized to 30s, got %v", cfg.LeaseDuration)
	}

	if cfg.HeartbeatInterval != 10*time.Second {
		t.Fatalf("expected heartbeat interval normalized to 10s, got %v", cfg.HeartbeatInterval)
	}

}

func TestOptions_HeartbeatClampedBelowLease(t *testing.T) {
	cfg := applyOptions(
		WithLeaseDuration(3*time.Second),
		WithHeartbeatInterval(10*time.Second),
	)

	// clamp => lease/3 => 1s
	if cfg.HeartbeatInterval != time.Second {
		t.Fatalf("expected heartbeat clamped to 1s, got %v", cfg.HeartbeatInterval)
	}
}
