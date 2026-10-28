package config

import (
	"log/slog"
	"os"
	"time"

	"github.com/barkin94/insider-notification/processor/internal/service"
	shared "github.com/barkin94/insider-notification/shared/config"
)

type Config struct {
	shared.Base
	DatabaseURL               string
	NATSAddr                  string
	RedisAddr                 string
	WorkerConcurrency         int
	MaxAttempts               int
	NtfnDeliveryClientURL     string
	NtfnDeliveryClientTimeout time.Duration
	HighWeight                int // HIGH_WEIGHT env var
	NormalWeight              int // NORMAL_WEIGHT env var
	LowWeight                 int // LOW_WEIGHT env var
	SMSRatePerSecond          int // SMS_RATE_PER_SECOND env var
	SMSBurst                  int // SMS_BURST env var
	EmailRatePerSecond        int // EMAIL_RATE_PER_SECOND env var
	EmailBurst                int // EMAIL_BURST env var
	PushRatePerSecond         int // PUSH_RATE_PER_SECOND env var
	PushBurst                 int // PUSH_BURST env var
}

func Load() *Config {
	v := shared.NewViper()
	v.SetDefault("WORKER_CONCURRENCY", 10)
	v.SetDefault("MAX_ATTEMPTS", service.MaxAttempts)
	v.SetDefault("NTFN_DELIVERY_CLIENT_URL", "http://localhost:8080")
	v.SetDefault("NTFN_DELIVERY_CLIENT_TIMEOUT", "10s")
	v.SetDefault("HIGH_WEIGHT", 3)
	v.SetDefault("NORMAL_WEIGHT", 2)
	v.SetDefault("LOW_WEIGHT", 1)
	v.SetDefault("SMS_RATE_PER_SECOND", 10)
	v.SetDefault("SMS_BURST", 15)
	v.SetDefault("EMAIL_RATE_PER_SECOND", 100)
	v.SetDefault("EMAIL_BURST", 120)
	v.SetDefault("PUSH_RATE_PER_SECOND", 500)
	v.SetDefault("PUSH_BURST", 600)

	dbURL := v.GetString("DATABASE_URL")
	if dbURL == "" {
		slog.Error("missing required env var", "var", "DATABASE_URL")
		os.Exit(1)
	}
	natsAddr := v.GetString("NATS_ADDR")
	if natsAddr == "" {
		slog.Error("missing required env var", "var", "NATS_ADDR")
		os.Exit(1)
	}
	redisAddr := v.GetString("REDIS_ADDR")
	if redisAddr == "" {
		slog.Error("missing required env var", "var", "REDIS_ADDR")
		os.Exit(1)
	}

	return &Config{
		Base:                      shared.LoadBase(v),
		DatabaseURL:               dbURL,
		NATSAddr:                  natsAddr,
		RedisAddr:                 redisAddr,
		WorkerConcurrency:         v.GetInt("WORKER_CONCURRENCY"),
		MaxAttempts:               v.GetInt("MAX_ATTEMPTS"),
		NtfnDeliveryClientURL:     v.GetString("NTFN_DELIVERY_CLIENT_URL"),
		NtfnDeliveryClientTimeout: v.GetDuration("NTFN_DELIVERY_CLIENT_TIMEOUT"),
		HighWeight:                v.GetInt("HIGH_WEIGHT"),
		NormalWeight:              v.GetInt("NORMAL_WEIGHT"),
		LowWeight:                 v.GetInt("LOW_WEIGHT"),
		SMSRatePerSecond:          v.GetInt("SMS_RATE_PER_SECOND"),
		SMSBurst:                  v.GetInt("SMS_BURST"),
		EmailRatePerSecond:        v.GetInt("EMAIL_RATE_PER_SECOND"),
		EmailBurst:                v.GetInt("EMAIL_BURST"),
		PushRatePerSecond:         v.GetInt("PUSH_RATE_PER_SECOND"),
		PushBurst:                 v.GetInt("PUSH_BURST"),
	}
}
