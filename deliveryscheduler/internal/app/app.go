package app

import (
	"context"
	"log/slog"
	"sync"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"

	apipub "github.com/barkin94/notification-system/api/public"
	"github.com/barkin94/notification-system/deliveryscheduler/internal/config"
	dbpostgres "github.com/barkin94/notification-system/deliveryscheduler/internal/db/postgres"
	dispatcher "github.com/barkin94/notification-system/deliveryscheduler/internal/scheduled_notification_dispatcher"
	dsmessaging "github.com/barkin94/notification-system/deliveryscheduler/internal/transport/messaging"
	sharedbun "github.com/barkin94/notification-system/shared/bun"
	sharedmessaging "github.com/barkin94/notification-system/shared/messaging"
	natsmsg "github.com/barkin94/notification-system/shared/messaging/nats"
	"github.com/barkin94/notification-system/shared/txmanager/buntx"
)

const notificationStream = "NOTIFICATIONS"

// App wires and runs the delivery scheduler service.
type App struct {
	consumer       *dsmessaging.Consumer
	cancelConsumer *dsmessaging.CancelConsumer
	dispatcher     *dispatcher.ScheduledNotificationDispatcher
	outboxRelay    *sharedmessaging.OutboxRelay
	pollInterval   time.Duration
	wg             sync.WaitGroup
}

// New constructs all dependencies and returns a ready-to-run App.
// The returned cleanup func closes infrastructure and must be deferred by the caller.
func New(ctx context.Context, cfg *config.Config) (*App, func(), error) {
	bunDB := sharedbun.Connect(cfg.DatabaseURL)
	repo := dbpostgres.NewScheduledNotificationRepository(bunDB)
	outboxRepo := dbpostgres.NewOutboxRepository(bunDB)
	txManager := buntx.NewManager(bunDB)

	natsHandle := natsmsg.NewHandle(cfg.NATSAddr)
	if err := natsmsg.EnsureStream(natsHandle, notificationStream, []string{"notify.>"}); err != nil {
		natsHandle.Close()
		_ = bunDB.Close()
		return nil, nil, err
	}
	pub := natsmsg.NewPublisher(natsHandle)

	// Subscribe to CloudEvents instead of typed events
	msgs := natsmsg.Subscribe[cloudevents.Event](
		ctx, natsHandle, string(apipub.TopicNotificationScheduled), "ds-scheduled", cfg.OTelServiceName, 0,
	)
	cancelMsgs := natsmsg.Subscribe[cloudevents.Event](
		ctx, natsHandle, string(apipub.TopicNotificationScheduleCancelled), "ds-cancel", cfg.OTelServiceName, 0,
	)

	cleanup := func() {
		natsHandle.Close()
		_ = bunDB.Close()
	}

	// Create outbox relay
	outboxRelay := sharedmessaging.NewOutboxRelay(
		outboxRepo,
		pub,
		500*time.Millisecond,
		100,
		10,
	)

	return &App{
		consumer:       dsmessaging.NewConsumer(repo, msgs),
		cancelConsumer: dsmessaging.NewCancelConsumer(repo, cancelMsgs),
		dispatcher:     dispatcher.NewScheduledNotificationDispatcher(repo, outboxRepo, txManager, cfg.PollBatchSize),
		outboxRelay:    outboxRelay,
		pollInterval:   cfg.PollInterval,
	}, cleanup, nil
}

// Start launches the consumer and dispatcher in the background and returns
// a stop function the caller must invoke to wait for all goroutines to finish.
func (a *App) Start(ctx context.Context) func() {
	a.wg.Go(func() { a.consumer.Run(ctx) })
	a.wg.Go(func() { a.cancelConsumer.Run(ctx) })
	a.wg.Go(func() { a.outboxRelay.Run(ctx) })
	a.wg.Go(func() {
		ticker := time.NewTicker(a.pollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				a.dispatcher.Tick(ctx)
			}
		}
	})

	slog.Info("deliveryscheduler started")
	return a.wg.Wait
}
