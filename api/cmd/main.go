// @title           Notification System API
// @version         1.0
// @description     Notification delivery service — create, list, and cancel notifications.
// @host            localhost:8080
// @BasePath        /api/v1
// @schemes         http

package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/barkin94/notification-system/api/internal/app"
	"github.com/barkin94/notification-system/api/internal/config"
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

	a, cleanup := app.New(ctx, cfg)
	defer cleanup()

	shutdown := a.Start(ctx)

	<-ctx.Done()
	slog.Info("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	shutdown(shutdownCtx)
	slog.Info("all goroutines stopped")
}
