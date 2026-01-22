package taskharbor

import (
	"math"
	"math/rand"
	"testing"
	"time"
)

func TestExponentialBackoff_NoJitter_MonotonicAndCapped(t *testing.T) {
	p := NewExponentialBackoffPolicy(
		100*time.Millisecond,
		1*time.Second,
		2.0,
		0,
	)

	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 100 * time.Millisecond},
		{2, 200 * time.Millisecond},
		{3, 400 * time.Millisecond},
		{4, 800 * time.Millisecond},
		{5, 1 * time.Second}, // capped
		{6, 1 * time.Second}, // capped
	}

	var prev time.Duration
	for i, tc := range cases {
		got := p.NextDelay(tc.attempt)
		if got != tc.want {
			t.Fatalf("case %d: attempt=%d got=%v want=%v", i, tc.attempt, got, tc.want)
		}
		if i > 0 && got < prev {
			t.Fatalf("case %d: expected monotonic non-decreasing, prev=%v got=%v", i, prev, got)
		}
		prev = got
	}
}

func TestExponentialBackoff_JitterWithinBounds(t *testing.T) {
	base := 1 * time.Second
	maxD := 30 * time.Second
	mult := 2.0
	jitter := 0.2

	p := NewExponentialBackoffPolicy(
		base,
		maxD,
		mult,
		jitter,
		WithRandSource(rand.NewSource(123)),
	)

	for attempt := 1; attempt <= 6; attempt++ {
		got := p.NextDelay(attempt)

		nominal := time.Duration(float64(base) * math.Pow(mult, float64(attempt-1)))
		if nominal > maxD {
			nominal = maxD
		}

		min := time.Duration(float64(nominal) * (1 - jitter))
		max := time.Duration(float64(nominal) * (1 + jitter))

		if min < 0 {
			min = 0
		}
		if maxD > 0 && max > maxD {
			max = maxD
		}

		if got < min || got > max {
			t.Fatalf("attempt=%d got=%v not in [%v, %v] (nominal=%v)", attempt, got, min, max, nominal)
		}
	}
}

func TestExponentialBackoff_DeterministicWithSeed(t *testing.T) {
	p1 := NewExponentialBackoffPolicy(
		250*time.Millisecond,
		5*time.Second,
		2.0,
		0.35,
		WithRandSource(rand.NewSource(7)),
	)

	p2 := NewExponentialBackoffPolicy(
		250*time.Millisecond,
		5*time.Second,
		2.0,
		0.35,
		WithRandSource(rand.NewSource(7)),
	)

	for attempt := 1; attempt <= 10; attempt++ {
		d1 := p1.NextDelay(attempt)
		d2 := p2.NextDelay(attempt)
		if d1 != d2 {
			t.Fatalf("attempt=%d expected deterministic equality, got %v vs %v", attempt, d1, d2)
		}
	}
}

func TestExponentialBackoff_ClampJitter(t *testing.T) {
	p := NewExponentialBackoffPolicy(
		1*time.Second,
		10*time.Second,
		2.0,
		5.0, // should clamp to 1.0
		WithRandSource(rand.NewSource(1)),
	)

	got := p.NextDelay(1)
	if got < 0 || got > 10*time.Second {
		t.Fatalf("unexpected delay: %v", got)
	}
}
