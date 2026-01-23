package taskharbor

import "time"

/*
This config stores the runtime configuration that will
be used by the Client and Worker. We will use this to
keep defaults in one place to avoid hardcoding anywhere.
*/
type Config struct {
	Codec        Codec
	Concurrency  int
	PollInterval time.Duration
	DefaultQueue string
	Clock        Clock
	RetryPolicy  RetryPolicy
	Middlewares  []Middleware
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
		Codec:        JsonCodec{},
		Concurrency:  4,
		PollInterval: 200 * time.Millisecond,
		DefaultQueue: DefaultQueue,
		Clock:        RealClock{},
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

	return cfg
}

/*
This will overwrite the default codec
*/
func WithCodec(c Codec) Option {
	return func(cfg *Config) {
		cfg.Codec = c
	}
}

/*
This option sets the concurrency for the worker.
*/
func WithConcurrency(n int) Option {
	return func(cfg *Config) {
		cfg.Concurrency = n
	}
}

/*
This option sets the polling interval for the worker.
*/
func WithPollInterval(d time.Duration) Option {
	return func(cfg *Config) {
		cfg.PollInterval = d
	}
}

/*
This option sets the default queue.
*/
func WithDefaultQueue(q string) Option {
	return func(cfg *Config) {
		cfg.DefaultQueue = q
	}
}

/*
This option sets the clock.
*/
func WithClock(c Clock) Option {
	return func(cfg *Config) {
		cfg.Clock = c
	}
}

/*
This option sets the retry policy.
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
