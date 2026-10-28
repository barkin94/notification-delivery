package messaging

import (
	"context"
	"encoding/json"
	"log/slog"

	db "github.com/barkin94/notification-system/deliveryscheduler/internal/db"
	"github.com/barkin94/notification-system/shared/bun"
	"github.com/barkin94/notification-system/shared/events"
	sharedotel "github.com/barkin94/notification-system/shared/otel"
	natsmsg "github.com/barkin94/notification-system/shared/messaging/nats"
	cloudevents "github.com/cloudevents/sdk-go/v2"
)

// Consumer consumes NotificationsScheduledEvent CloudEvents and persists scheduled notifications to Postgres
// so the dispatcher picks them up when scheduled_at time has passed.
type Consumer struct {
	repo db.ScheduledNotificationRepository
	msgs <-chan natsmsg.Result[cloudevents.Event]
}

func NewConsumer(
	repo db.ScheduledNotificationRepository,
	msgs <-chan natsmsg.Result[cloudevents.Event],
) *Consumer {
	return &Consumer{repo: repo, msgs: msgs}
}

func (c *Consumer) Run(ctx context.Context) {
	natsmsg.ForEach(ctx, c.msgs, c.handleScheduledEvents)
}

func (c *Consumer) handleScheduledEvents(result natsmsg.Result[cloudevents.Event]) {
	ctx := result.Ctx

	// Extract scheduled notifications data from CloudEvent
	var scheduledData events.NotificationsScheduledData
	if err := json.Unmarshal(result.Event.Data(), &scheduledData); err != nil {
		slog.ErrorContext(ctx, "parse CloudEvent data", "error", err)
		sharedotel.RecordError(ctx, err)
		_ = result.Msg.Nak()
		return
	}

	traceMetadata := sharedotel.ExtractTraceMetadata(ctx)
	notifications := make([]*db.ScheduledNotification, len(scheduledData.Notifications))
	for i, item := range scheduledData.Notifications {
		scheduledAt := item.ScheduledAt
		notifications[i] = &db.ScheduledNotification{
			NotificationID:     item.NotificationID,
			ScheduledAt:        &scheduledAt,
			TraceMetadataModel: bun.TraceMetadataModel{TraceMetadata: traceMetadata},
		}
	}

	if err := c.repo.UpsertAll(ctx, notifications); err != nil {
		ids := make([]string, len(notifications))
		for i, n := range notifications {
			ids[i] = n.NotificationID
		}
		slog.ErrorContext(ctx, "persist scheduled notifications failed", "count", len(notifications), "notification_ids", ids, "error", err)
		_ = result.Msg.Nak()
		sharedotel.RecordError(ctx, err)
		return
	}

	for _, item := range scheduledData.Notifications {
		slog.InfoContext(ctx, "notification scheduled", "id", item.NotificationID, "scheduled_at", item.ScheduledAt)
	}
	_ = result.Msg.Ack()
}