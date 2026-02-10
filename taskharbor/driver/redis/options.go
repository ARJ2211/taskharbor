package redis

// Option tweaks driver config (prefix, DB, etc.)
type Option func(*options)

type options struct {
	prefix string
	db     int
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
