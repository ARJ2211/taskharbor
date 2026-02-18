package taskharbor

import "time"

/*
This config stores the runtime configuration that will
be used by the Client and Worker. We will use this to
keep defaults in one place to avoid hardcoding anywhere.
*/
type Config struct {
	Codec             Codec
	Concurrency       int
	PollInterval      time.Duration
	DefaultQueue      string
	Clock             Clock
	RetryPolicy       RetryPolicy
	Middlewares       []Middleware
	LeaseDuration     time.Duration
	HeartbeatInterval time.Duration
}

/*
Option is the functional options pattern.
Each option mutates the config.
*/
type Option func(*Config)

/*
This function will return the default
configuration for the client and worker.
*/
func defaultConfig() Config {
	var c Config = Config{
		Codec:             JsonCodec{},
		Concurrency:       4,
		PollInterval:      200 * time.Millisecond,
		DefaultQueue:      DefaultQueue,
		Clock:             RealClock{},
		LeaseDuration:     30 * time.Second,
		HeartbeatInterval: 0, // Computed in applyoptions.
		RetryPolicy: NewExponentialBackoffPolicy(
			200*time.Millisecond,
			5*time.Second,
			2.0,
			0.20,
			WithMaxAttempts(0), // 0 = no global cap; job.MaxAttempts controls DLQ cutoff
		),
	}
	return c
}

/*
This function will apply options
and normalize any value.
*/
func applyOptions(opts ...Option) Config {
	cfg := defaultConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if cfg.Codec == nil {
		cfg.Codec = JsonCodec{}
	}

	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}

	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 200 * time.Millisecond
	}

	if cfg.DefaultQueue == "" {
		cfg.DefaultQueue = DefaultQueue
	}

	if cfg.Clock == nil {
		cfg.Clock = RealClock{}
	}
	if cfg.LeaseDuration <= 0 {
		cfg.LeaseDuration = 30 * time.Second
	}

	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = cfg.LeaseDuration / 3
	}

	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = time.Second
	}

	if cfg.HeartbeatInterval >= cfg.LeaseDuration {
		cfg.HeartbeatInterval = cfg.LeaseDuration / 3
		if cfg.HeartbeatInterval <= 0 {
			cfg.HeartbeatInterval = time.Second
		}
	}

	return cfg
}

/*
This will overwrite the default codec (JsonCodec{}).

The Codec is responsible for encoding a JobRequest.Payload (any) into bytes at enqueue-time,
and decoding those bytes back into a Go value inside the handler.

Use this when:
- you want MessagePack / Protobuf / Gob instead of JSON
- you want custom versioning / compression
- you want deterministic encoding for hashing / idempotency

If c is nil, applyOptions will reset it back to JsonCodec{}.
*/
func WithCodec(c Codec) Option {
	return func(cfg *Config) {
		cfg.Codec = c
	}
}

/*
This will overwrite the default concurrency (4).

Concurrency controls how many jobs the Worker can execute in parallel.
Conceptually, it’s the number of worker goroutines that can be “in-flight” at once.

Higher values:
- increase throughput (more jobs at the same time)
- increase CPU/memory usage and downstream load (DB, Redis, APIs)

If n <= 0, applyOptions normalizes it to 1.
*/
func WithConcurrency(n int) Option {
	return func(cfg *Config) {
		cfg.Concurrency = n
	}
}

/*
This will overwrite the default poll interval (200ms).

PollInterval is how often the Worker asks the driver for available jobs when it has capacity.
This matters most for drivers that don’t have a push mechanism and rely on polling.

Smaller values:
- lower latency to pick up jobs
- more frequent driver calls (more Redis/DB load)

Larger values:
- less load on the backend
- more “wait time” before a ready job gets picked up

If d <= 0, applyOptions resets it to 200ms.
*/
func WithPollInterval(d time.Duration) Option {
	return func(cfg *Config) {
		cfg.PollInterval = d
	}
}

/*
This will overwrite the default queue name (DefaultQueue).

DefaultQueue is used when a JobRequest does not specify Queue explicitly.
It keeps “simple enqueue” ergonomics while still allowing multiple queues.

If q == "", applyOptions resets it back to DefaultQueue.
*/
func WithDefaultQueue(q string) Option {
	return func(cfg *Config) {
		cfg.DefaultQueue = q
	}
}

/*
This will overwrite the default clock (RealClock{}).

Clock is the time source used by the Client/Worker when they need “now”.
It exists mainly for testability and determinism.

Use this when:
- you want a fake/manual clock in tests (advance time without sleeping)
- you want deterministic scheduling/backoff tests

If c is nil, applyOptions resets it to RealClock{}.
*/
func WithClock(c Clock) Option {
	return func(cfg *Config) {
		cfg.Clock = c
	}
}

/*
This will overwrite the default retry policy (ExponentialBackoffPolicy ...).

RetryPolicy decides how long to wait before retrying a failed job and
(optionally) can impose a global max-attempts cap.

This policy is used by the Worker/core when a handler returns an error:
- it computes the next delay
- the job is rescheduled accordingly

Note: applyOptions does not currently “fix up” a nil RetryPolicy.
So don’t pass nil here unless the rest of your code explicitly handles it.
*/
func WithRetryPolicy(p RetryPolicy) Option {
	return func(cfg *Config) {
		cfg.RetryPolicy = p
	}
}

/*
This option sets the user middlewares.
*/
func WithMiddleware(mw Middleware) Option {
	return func(cfg *Config) {
		cfg.Middlewares = append(cfg.Middlewares, mw)
	}
}

/*
This will overwrite the default lease duration (30s).

LeaseDuration is the time window a worker “owns” a reserved job.
If the worker crashes or doesn’t ack/extend in time, the lease expires and
the job becomes reclaimable/reservable again.

Bigger values:
- safer for long-running jobs without frequent heartbeats
- slower recovery if a worker dies (job waits longer before being reclaimed)

Smaller values:
- faster recovery from worker death
- requires heartbeats/extends to avoid accidental reclaim

If d <= 0, applyOptions resets it to 30s.
*/
func WithLeaseDuration(d time.Duration) Option {
	return func(cfg *Config) {
		cfg.LeaseDuration = d
	}
}

/*
This will overwrite the default heartbeat interval (computed in applyOptions).

HeartbeatInterval controls how often the worker extends the lease while a job is running.
It should be comfortably smaller than LeaseDuration so lease extensions happen in time.

Defaults:
- if not set (<= 0), applyOptions sets it to LeaseDuration/3
- applyOptions also ensures it stays > 0 and < LeaseDuration

Use this when:
- jobs are very long-running and you want frequent extends
- you want fewer extends to reduce backend chatter (while still staying safe)
*/
func WithHeartbeatInterval(d time.Duration) Option {
	return func(cfg *Config) {
		cfg.HeartbeatInterval = d
	}
}
