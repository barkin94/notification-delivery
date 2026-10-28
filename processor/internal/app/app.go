package app

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/go-redis/redis_rate/v10"

	apipub "github.com/barkin94/insider-notification/api/public"
	"github.com/barkin94/insider-notification/processor/internal/config"
	"github.com/barkin94/insider-notification/processor/internal/db/postgres"
	"github.com/barkin94/insider-notification/processor/internal/delivery"
	"github.com/barkin94/insider-notification/processor/internal/service"
	"github.com/barkin94/insider-notification/processor/internal/transport/messaging"
	"github.com/barkin94/insider-notification/shared/bun"
	sharedmessaging "github.com/barkin94/insider-notification/shared/messaging"
	"github.com/barkin94/insider-notification/shared/lock"
	natsmsg "github.com/barkin94/insider-notification/shared/messaging/nats"
	sharedredis "github.com/barkin94/insider-notification/shared/redis"
)

const notificationStream = "NOTIFICATIONS"

// App wires and runs the processor service.
type App struct {
	consumer    *messaging.NotificationReadyConsumer
	outboxRelay *sharedmessaging.OutboxRelay
	wg          sync.WaitGroup
}

// New constructs all dependencies and returns a ready-to-run App.
// The returned cleanup func closes infrastructure and must be deferred by the caller.
func New(ctx context.Context, cfg *config.Config) (*App, func(), error) {
	rdb := sharedredis.NewClient(ctx, cfg.RedisAddr)

	bundb := bun.Connect(cfg.DatabaseURL)

	natsHandle := natsmsg.NewHandle(cfg.NATSAddr)
	if err := natsmsg.EnsureStream(natsHandle, notificationStream, []string{"notify.>"}); err != nil {
		_ = bundb.Close()
		_ = rdb.Close()
		return nil, nil, fmt.Errorf("ensure nats stream: %w", err)
	}
	natsPub := natsmsg.NewPublisher(natsHandle)

	m, err := service.NewMetrics(rdb)
	if err != nil {
		natsHandle.Close()
		_ = bundb.Close()
		_ = rdb.Close()
		return nil, nil, fmt.Errorf("init metrics: %w", err)
	}

	outboxRepo := postgres.NewOutboxRepository(bundb)

	pipeline := delivery.NewNotificationDeliveryPipeline(
		outboxRepo,
		service.NewNtfnDeliveryClient(cfg.NtfnDeliveryClientURL, cfg.NtfnDeliveryClientTimeout),
		service.NewLimiter(rdb, map[string]redis_rate.Limit{
			string(apipub.ChannelSMS):   {Rate: cfg.SMSRatePerSecond, Burst: cfg.SMSBurst, Period: time.Second},
			string(apipub.ChannelEmail): {Rate: cfg.EmailRatePerSecond, Burst: cfg.EmailBurst, Period: time.Second},
			string(apipub.ChannelPush):  {Rate: cfg.PushRatePerSecond, Burst: cfg.PushBurst, Period: time.Second},
		}),
		lock.NewRedisLocker(rdb),
		m,
	)

	// Create outbox relay
	outboxRelay := sharedmessaging.NewOutboxRelay(
		outboxRepo,
		natsPub,
		500*time.Millisecond,
		100,
		10,
	)

	cleanup := func() {
		natsHandle.Close()
		_ = bundb.Close()
		_ = rdb.Close()
	}

	return &App{
		consumer:    messaging.NewNotificationReadyConsumer(
			ctx,
			natsHandle,
			cfg.OTelServiceName,
			cfg.HighWeight,
			cfg.NormalWeight,
			cfg.LowWeight,
			pipeline,
			cfg.WorkerConcurrency,
			cfg.MaxAttempts+1,
		),
		outboxRelay: outboxRelay,
	}, cleanup, nil
}

// Start launches the worker pool in the background and returns a stop function
// the caller must invoke to wait for all workers to finish their current message.
func (a *App) Start(ctx context.Context) func() {
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		a.consumer.StartMessageProcessing(ctx)
	}()

	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		a.outboxRelay.Run(ctx)
	}()

	slog.Info("processor started")
	return a.wg.Wait
}
