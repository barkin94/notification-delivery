package app

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/barkin94/insider-notification/api/internal/config"
	"github.com/barkin94/insider-notification/api/internal/db/postgres"
	"github.com/barkin94/insider-notification/api/internal/service"
	handler "github.com/barkin94/insider-notification/api/internal/transport/http"
	apimessaging "github.com/barkin94/insider-notification/api/internal/transport/messaging"
	dspub "github.com/barkin94/insider-notification/deliveryscheduler/public"
	processorpub "github.com/barkin94/insider-notification/processor/public"
	sharedbun "github.com/barkin94/insider-notification/shared/bun"
	sharedmessaging "github.com/barkin94/insider-notification/shared/messaging"
	"github.com/barkin94/insider-notification/shared/txmanager/buntx"
	natsmsg "github.com/barkin94/insider-notification/shared/messaging/nats"
	cloudevents "github.com/cloudevents/sdk-go/v2"
)

const notificationStream = "NOTIFICATIONS"

// App wires and runs the API service.
type App struct {
	server                 *http.Server
	deliveryResultConsumer *apimessaging.DeliveryResultConsumer
	scheduledDueConsumer   *apimessaging.ScheduledDueConsumer
	outboxRelay            *sharedmessaging.OutboxRelay
	wg                     sync.WaitGroup
}

// New constructs all dependencies and returns a ready-to-run App.
// Panics if any infrastructure dependency is unreachable.
func New(ctx context.Context, cfg *config.Config) (*App, func()) {
	bundb := sharedbun.Connect(cfg.DatabaseURL)

	natsHandle := natsmsg.NewHandle(cfg.NATSAddr)
	if err := natsmsg.EnsureStream(natsHandle, notificationStream, []string{"notify.>"}); err != nil {
		panic("ensure nats stream: " + err.Error())
	}
	pub := natsmsg.NewPublisher(natsHandle)

	// Subscribe to CloudEvents instead of typed events
	statusMsgs := natsmsg.Subscribe[cloudevents.Event](
		ctx, natsHandle, processorpub.TopicStatus, "api-status", cfg.OTelServiceName, 0,
	)
	scheduledDueMsgs := natsmsg.SubscribeBatch[cloudevents.Event](
		ctx, natsHandle, dspub.TopicScheduledNotificationDue, "api-scheduled-due", cfg.OTelServiceName, 0,
	)

	notifRepo := postgres.NewNotificationRepository(bundb)
	outboxRepo := postgres.NewOutboxRepository(bundb)
	txManager := buntx.NewManager(bundb)
	svc := service.NewNotificationService(notifRepo, outboxRepo, txManager)
	router := handler.NewRouter(handler.Deps{Service: svc, DB: bundb})

	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Port),
		Handler:           router,
		ReadHeaderTimeout: 30 * time.Second,
	}

	// Create outbox relay
	outboxRelay := sharedmessaging.NewOutboxRelay(
		outboxRepo,
		pub,
		500*time.Millisecond,
		100,
		10,
	)

	cleanup := func() {
		natsHandle.Close()
		_ = bundb.Close()
	}

	return &App{
		server:                 srv,
		deliveryResultConsumer: apimessaging.NewDeliveryResultConsumer(svc, statusMsgs),
		scheduledDueConsumer:   apimessaging.NewScheduledDueConsumer(notifRepo, outboxRepo, scheduledDueMsgs),
		outboxRelay:            outboxRelay,
	}, cleanup
}

// Start launches the HTTP server and consumers in the background.
// It returns a stop function that the caller must invoke to drain all goroutines gracefully.
func (a *App) Start(ctx context.Context) func(context.Context) {
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		a.deliveryResultConsumer.Run(ctx)
	}()

	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		a.scheduledDueConsumer.Run(ctx)
	}()

	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		a.outboxRelay.Run(ctx)
	}()

	go func() {
		slog.Info("api server starting", "addr", a.server.Addr)
		if err := a.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "error", err)
		}
	}()

	return func(ctx context.Context) {
		if err := a.server.Shutdown(ctx); err != nil {
			slog.Error("shutdown error", "error", err)
		}
		a.wg.Wait()
	}
}