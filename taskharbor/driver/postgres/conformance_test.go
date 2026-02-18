package postgres

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/ARJ2211/taskharbor/conformance"
	thdriver "github.com/ARJ2211/taskharbor/taskharbor/driver"
	"github.com/ARJ2211/taskharbor/taskharbor/internal/envutil"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Close() must NOT close the pool.
type sharedPoolDriver struct{ *Driver }

func (d *sharedPoolDriver) Close() error { return nil }

func TestConformance(t *testing.T) {
	cwd, _ := os.Getwd()
	_ = envutil.LoadRepoDotenv(cwd)

	dsn := os.Getenv("TASKHARBOR_TEST_DSN")
	if dsn == "" {
		t.Skip("TASKHARBOR_TEST_DSN not set; skipping postgres conformance")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)

	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("pool.Ping: %v", err)
	}

	if err := ApplyMigrations(ctx, pool); err != nil {
		t.Fatalf("ApplyMigrations: %v", err)
	}

	truncate := func(t *testing.T) {
		t.Helper()
		c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := pool.Exec(c, "TRUNCATE TABLE th_jobs"); err != nil {
			t.Fatalf("truncate th_jobs: %v", err)
		}
	}

	truncate(t)

	f := conformance.Factory{
		Name: "postgres",
		New: func(t *testing.T) thdriver.Driver {
			t.Helper()

			truncate(t)

			d, err := NewWithPool(pool)
			if err != nil {
				t.Fatalf("NewWithPool: %v", err)
			}

			return &sharedPoolDriver{Driver: d}
		},
		Cleanup: truncate,
	}

	conformance.Run(t, f, conformance.All()...)
}
