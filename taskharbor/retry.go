package taskharbor

import (
	"math"
	"math/rand"
	"sync"
	"time"
)

/*
The retry policy is used by the WORKER to decide when a job
should be retried. Attempts start from 1 and represents the
number of failiures recorded until now.
*/
type RetryPolicy interface {
	NextDelay(attempt int) time.Duration
	MaxAttempts() int
}

/*
Exponential backoff

Delay formula:

	delay = base * multiplier ^ (attempt-1), capped at max

Jitter formula:

	delay' = delay * u
	(u is uniform in [1-JitterFrac, 1+JitterFrac])
*/
type ExponentialBackoffPolicy struct {
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	Multiplier  float64
	JitterFrac  float64
	maxAttempts int
	mu          sync.Mutex
	rng         *rand.Rand
}

// ExponentialBackoffOption configures ExponentialBackoffPolicy.
type ExponentialBackoffOption func(*ExponentialBackoffPolicy)

func WithMaxAttempts(n int) ExponentialBackoffOption {
	return func(p *ExponentialBackoffPolicy) { p.maxAttempts = n }
}

// This sets the RNG source used for the jitter.
func WithRandSource(src rand.Source) ExponentialBackoffOption {
	return func(p *ExponentialBackoffPolicy) {
		if src == nil {
			return
		}
		p.rng = rand.New(src)
	}
}

func NewExponentialBackoffPolicy(
	baseDelay time.Duration,
	maxDelay time.Duration,
	multiplier float64,
	jitterFrac float64,
	opts ...ExponentialBackoffOption,
) *ExponentialBackoffPolicy {
	var p ExponentialBackoffPolicy = ExponentialBackoffPolicy{
		BaseDelay:  baseDelay,
		MaxDelay:   maxDelay,
		Multiplier: multiplier,
		JitterFrac: jitterFrac,
		rng:        rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	for _, opt := range opts {
		if opt != nil {
			opt(&p)
		}
	}

	return &p
}

func (p *ExponentialBackoffPolicy) MaxAttempts() int {
	return p.maxAttempts
}

func (p *ExponentialBackoffPolicy) NextDelay(attempt int) time.Duration {
	if attempt <= 0 {
		return 0
	}

	base := p.BaseDelay
	if base <= 0 {
		return 0
	}

	m := p.Multiplier
	if m < 1 {
		m = 1
	}

	maxD := p.MaxDelay
	if maxD > 0 && maxD < base {
		maxD = base
	}

	// Compute exponential delay with float math, then cap.
	exp := math.Pow(m, float64(attempt-1))
	raw := float64(base) * exp

	delay := time.Duration(raw)
	if maxD > 0 && delay > maxD {
		delay = maxD
	}

	j := clamp01(p.JitterFrac)
	if j == 0 {
		return delay
	}

	// Uniform factor in [1-j, 1+j]
	f := (1 - j) + (2*j)*p.randFloat64()
	jittered := time.Duration(float64(delay) * f)

	if jittered < 0 {
		jittered = 0
	}
	if maxD > 0 && jittered > maxD {
		jittered = maxD
	}

	return jittered
}

func (p *ExponentialBackoffPolicy) randFloat64() float64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.rng.Float64()
}

func clamp01(x float64) float64 {
	if x < 0 {
		return 0
	}
	if x > 1 {
		return 1
	}
	return x
}
