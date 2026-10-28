package messaging

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/google/uuid"

	"github.com/barkin94/insider-notification/api/internal/service"
	"github.com/barkin94/insider-notification/shared/events"
	sharedotel "github.com/barkin94/insider-notification/shared/otel"
	natsmsg "github.com/barkin94/insider-notification/shared/messaging/nats"
	cloudevents "github.com/cloudevents/sdk-go/v2"
)

// DeliveryResultConsumer processes NotificationDeliveryResultEvent CloudEvents from the status stream.
type DeliveryResultConsumer struct {
	svc  service.NotificationService
	msgs <-chan natsmsg.Result[cloudevents.Event]
}

func NewDeliveryResultConsumer(svc service.NotificationService, msgs <-chan natsmsg.Result[cloudevents.Event]) *DeliveryResultConsumer {
	return &DeliveryResultConsumer{svc: svc, msgs: msgs}
}

func (c *DeliveryResultConsumer) Run(ctx context.Context) {
	natsmsg.ForEach(ctx, c.msgs, c.processOne)
}

func (c *DeliveryResultConsumer) processOne(result natsmsg.Result[cloudevents.Event]) {
	ctx := result.Ctx

	// Extract delivery result data from CloudEvent
	var deliveryData events.NotificationDeliveryResultData
	if err := json.Unmarshal(result.Event.Data(), &deliveryData); err != nil {
		slog.ErrorContext(ctx, "delivery result consumer: parse CloudEvent data", "error", err)
		sharedotel.RecordError(ctx, err)
		_ = result.Msg.Nak()
		return
	}

	notifID, err := uuid.Parse(deliveryData.NotificationID)
	if err != nil {
		slog.ErrorContext(ctx, "invalid notification_id", "notification_id", deliveryData.NotificationID, "error", err)
		_ = result.Msg.Nak()
		sharedotel.RecordError(ctx, err)
		return
	}

	if err := c.svc.UpdateStatus(ctx, notifID, deliveryData.Status); err != nil {
		slog.ErrorContext(ctx, "update notification status failed", "notification_id", notifID, "error", err)
		_ = result.Msg.Nak()
		sharedotel.RecordError(ctx, err)
		return
	}

	slog.InfoContext(ctx, "status event processed",
		"notification_id", notifID.String(),
		"status", deliveryData.Status,
		"attempt", deliveryData.AttemptNumber,
	)
	_ = result.Msg.Ack()
}