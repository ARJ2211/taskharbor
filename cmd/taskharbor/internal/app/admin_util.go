package app

import (
	"context"
	"fmt"

	"github.com/ARJ2211/taskharbor/cmd/taskharbor/internal/backend"
	drv "github.com/ARJ2211/taskharbor/taskharbor/driver"
)

type adminHandle struct {
	Admin drv.Admin
	Close func() error
}

func openAdmin(ctx context.Context, g GlobalFlags) (*adminHandle, error) {
	h, err := backend.Open(ctx, backend.Config{
		Driver:      g.Driver,
		PostgresDSN: g.PostgresDSN,
		RedisAddr:   g.RedisAddr,
	})
	if err != nil {
		return nil, err
	}

	a, ok := h.Driver.(drv.Admin)
	if !ok {
		_ = h.Close()
		return nil, fmt.Errorf("driver %q does not support admin operations", g.Driver)
	}

	return &adminHandle{
		Admin: a,
		Close: h.Close,
	}, nil
}
