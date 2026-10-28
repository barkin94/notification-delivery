package messaging

import (
	"context"
	"encoding/json"
	"log/slog"

	db "github.com/barkin94/insider-notification/deliveryscheduler/internal/db"
	"github.com/barkin94/insider-notification/shared/events"
	natsmsg "github.com/barkin94/insider-notification/shared/messaging/nats"
	sharedotel "github.com/barkin94/insider-notification/shared/otel"
	cloudevents "github.com/cloudevents/sdk-go/v2"
)

// CancelConsumer consumes NotificationScheduleCancelledEvent CloudEvents and removes the
// matching row from scheduled_notifications so the dispatcher never publishes it.
type CancelConsumer struct {
	repo db.ScheduledNotificationRepository
	msgs <-chan natsmsg.Result[cloudevents.Event]
}

func NewCancelConsumer(
	repo db.ScheduledNotificationRepository,
	msgs <-chan natsmsg.Result[cloudevents.Event],
) *CancelConsumer {
	return &CancelConsumer{repo: repo, msgs: msgs}
}

func (c *CancelConsumer) Run(ctx context.Context) {
	natsmsg.ForEach(ctx, c.msgs, c.handleCancelEvent)
}

func (c *CancelConsumer) handleCancelEvent(result natsmsg.Result[cloudevents.Event]) {
	ctx := result.Ctx

	// Extract cancel data from CloudEvent
	var cancelData events.NotificationScheduleCancelledData
	if err := json.Unmarshal(result.Event.Data(), &cancelData); err != nil {
		slog.ErrorContext(ctx, "parse CloudEvent data", "error", err)
		sharedotel.RecordError(ctx, err)
		_ = result.Msg.Nak()
		return
	}

	if err := c.repo.DeleteByNotificationID(ctx, cancelData.NotificationID); err != nil {
		slog.ErrorContext(ctx, "delete scheduled notification failed", "id", cancelData.NotificationID, "error", err)
		_ = result.Msg.Nak()
		sharedotel.RecordError(ctx, err)
		return
	}

	slog.InfoContext(ctx, "notification schedule cancelled", "id", cancelData.NotificationID)
	_ = result.Msg.Ack()
}