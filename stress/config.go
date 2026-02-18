// examples/stress/config.go
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	DriverType string

	TotalJobs       int
	NumQueues       int
	WorkersPerQueue int
	Concurrency     int

	PollInterval      time.Duration
	HeartbeatInterval time.Duration

	MaxAttempts int
	FlakyPct    int
	FailPct     int

	WorkMin   time.Duration
	WorkMax   time.Duration
	BodyBytes int

	UseRetryPol  bool
	Reset        bool
	PrintEvery   time.Duration
	RunTimeout   time.Duration
	Seed         int64
	SpawnEvery   int
	ScheduleRate int // every Nth job is scheduled

	PostgresDSN      string
	PostgresMaxConns int

	RedisAddr      string
	RedisDB        int
	RedisPrefix    string
	RedisPoolSize  int
	RedisResetMode string
}

func parseConfigOrExit() Config {
	_ = godotenv.Load()

	var cfg Config

	flag.StringVar(&cfg.DriverType, "driver", "", "required: postgres | redis")

	flag.IntVar(&cfg.TotalJobs, "jobs", 120000, "total jobs to enqueue")
	flag.IntVar(&cfg.NumQueues, "queues", 16, "number of queues (q0..qN)")
	flag.IntVar(&cfg.WorkersPerQueue, "workers-per-queue", 4, "worker instances per queue")
	flag.IntVar(&cfg.Concurrency, "concurrency", 5, "handler concurrency per worker")

	pollMS := flag.Int("poll-ms", 10, "worker poll interval (ms)")
	hbMS := flag.Int("heartbeat-ms", 50, "worker heartbeat interval (ms)")

	flag.IntVar(&cfg.MaxAttempts, "max-attempts", 5, "job-level max attempts")
	flag.IntVar(&cfg.FlakyPct, "flaky-pct", 20, "percent of jobs that fail once then succeed")
	flag.IntVar(&cfg.FailPct, "fail-pct", 10, "percent of jobs that always fail (should DLQ)")

	workMinMS := flag.Int("work-min-ms", 1, "min simulated work per job (ms)")
	workMaxMS := flag.Int("work-max-ms", 15, "max simulated work per job (ms)")
	flag.IntVar(&cfg.BodyBytes, "body-bytes", 256, "payload body size in bytes")

	flag.BoolVar(&cfg.UseRetryPol, "retry-pol", true, "worker level retry policy")
	flag.BoolVar(&cfg.Reset, "reset", true, "reset backend state before starting")

	printMS := flag.Int("print-ms", 10, "print progress every N ms")
	timeoutSecs := flag.Int("timeout-secs", 60000, "overall run timeout (seconds)")

	flag.Int64Var(&cfg.Seed, "seed", 42, "rng seed (repeatable runs)")
	flag.IntVar(&cfg.SpawnEvery, "spawn-every", 50, "every Nth successful parent job enqueues 1 child")
	flag.IntVar(&cfg.ScheduleRate, "schedule-every", 30, "every Nth job gets a run_at in near future")

	flag.IntVar(&cfg.PostgresMaxConns, "pg-max-conns", 64, "pgxpool max connections (postgres)")
	flag.IntVar(&cfg.RedisDB, "redis-db", 0, "redis logical DB (0-15)")
	flag.StringVar(&cfg.RedisPrefix, "redis-prefix", "taskharbor", "redis key prefix")
	flag.IntVar(&cfg.RedisPoolSize, "redis-pool-size", 128, "redis pool size")
	flag.StringVar(&cfg.RedisResetMode, "redis-reset-mode", "scan", `redis reset: "scan" (delete prefix keys) or "flush" (FLUSHDB)`)

	flag.Parse()

	cfg.PollInterval = time.Duration(*pollMS) * time.Millisecond
	cfg.HeartbeatInterval = time.Duration(*hbMS) * time.Millisecond
	cfg.WorkMin = time.Duration(*workMinMS) * time.Millisecond
	cfg.WorkMax = time.Duration(*workMaxMS) * time.Millisecond
	cfg.PrintEvery = time.Duration(*printMS) * time.Millisecond
	cfg.RunTimeout = time.Duration(*timeoutSecs) * time.Second

	cfg.DriverType = strings.ToLower(strings.TrimSpace(cfg.DriverType))
	if cfg.DriverType == "" {
		fmt.Println("missing required -driver")
		flag.Usage()
		os.Exit(2)
	}

	switch cfg.DriverType {
	case "postgres":
		cfg.PostgresDSN = os.Getenv("TASKHARBOR_DSN")
		if cfg.PostgresDSN == "" {
			log.Fatal("TASKHARBOR_DSN not set (required for -driver postgres)")
		}
	case "redis":
		cfg.RedisAddr = os.Getenv("REDIS_ADDR")
		if cfg.RedisAddr == "" {
			cfg.RedisAddr = "localhost:6379"
		}
		if cfg.RedisDB < 0 || cfg.RedisDB > 15 {
			log.Fatalf("invalid -db=%d (must be 0..15)", cfg.RedisDB)
		}
		if strings.TrimSpace(cfg.RedisPrefix) == "" {
			log.Fatal("invalid -prefix (empty)")
		}
	default:
		log.Fatalf("invalid -driver=%s (must be postgres|redis)", cfg.DriverType)
	}

	return cfg
}
