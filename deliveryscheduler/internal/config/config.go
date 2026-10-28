package config

import (
	"log/slog"
	"os"
	"time"

	shared "github.com/barkin94/notification-system/shared/config"
)

type Config struct {
	shared.Base
	DatabaseURL   string
	NATSAddr      string
	PollBatchSize int
	PollInterval  time.Duration
}

func Load() *Config {
	v := shared.NewViper()
	v.SetDefault("POLL_BATCH_SIZE", 100)
	v.SetDefault("POLL_INTERVAL", "5s")
	v.SetDefault("OTEL_SERVICE_NAME", "deliveryscheduler")

	databaseURL := v.GetString("DATABASE_URL")
	if databaseURL == "" {
		slog.Error("missing required env var", "var", "DATABASE_URL")
		os.Exit(1)
	}
	natsAddr := v.GetString("NATS_ADDR")
	if natsAddr == "" {
		slog.Error("missing required env var", "var", "NATS_ADDR")
		os.Exit(1)
	}

	return &Config{
		Base:          shared.LoadBase(v),
		DatabaseURL:   databaseURL,
		NATSAddr:      natsAddr,
		PollBatchSize: v.GetInt("POLL_BATCH_SIZE"),
		PollInterval:  v.GetDuration("POLL_INTERVAL"),
	}
}
