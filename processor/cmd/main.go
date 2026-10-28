package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/barkin94/notification-system/processor/internal/app"
	"github.com/barkin94/notification-system/processor/internal/config"
	sharedlogger "github.com/barkin94/notification-system/shared/logger"
	sharedotel "github.com/barkin94/notification-system/shared/otel"
)

func main() {
	cfg := config.Load()
	sharedlogger.Init(cfg.LogLevel)

	if cfg.OTelEnabled {
		otelShutdown, err := sharedotel.Init(context.Background(), cfg.OTelServiceName, cfg.OTelEndpoint, cfg.LogLevel)
		defer otelShutdown(context.Background()) //nolint:errcheck

		if err != nil {
			slog.Error("otel init", "error", err)
			os.Exit(1)
		}
	}

	ctx, stopSignal := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSignal()

	a, cleanup, err := app.New(ctx, cfg)
	if err != nil {
		slog.Error("init app", "error", err)
		os.Exit(1)
	}
	defer cleanup()

	stop := a.Start(ctx)

	<-ctx.Done()
	slog.Info("shutting down, waiting for workers")
	stop()
	slog.Info("all workers stopped")
}
