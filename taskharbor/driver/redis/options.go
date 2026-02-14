package redis

import "github.com/redis/go-redis/v9"

// Option tweaks driver config (prefix, DB, or underlying Redis client when we create it in New).
type Option func(*options)

type options struct {
	prefix     string
	db         int
	clientOpts []func(*redis.Options)
}

// KeyPrefix — all keys get this prefix so one Redis can serve multiple apps. default "taskharbor"
func KeyPrefix(prefix string) Option {
	return func(o *options) {
		o.prefix = prefix
	}
}

// DB — which Redis logical DB (0–15). handy for tests so you don't clobber prod
func DB(db int) Option {
	return func(o *options) {
		o.db = db
	}
}

// RedisClientOption mutates the redis.Options used when creating the client in New.
// Ignored when using NewWithClient. Use for timeouts, pool size, TLS, etc.
func RedisClientOption(fn func(*redis.Options)) Option {
	return func(o *options) {
		if fn != nil {
			o.clientOpts = append(o.clientOpts, fn)
		}
	}
}

func applyOptions(opts ...Option) options {
	o := options{
		prefix: "taskharbor",
		db:     0,
	}
	for _, f := range opts {
		if f != nil {
			f(&o)
		}
	}
	return o
}
