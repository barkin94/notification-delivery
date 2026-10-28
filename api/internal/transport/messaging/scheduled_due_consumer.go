package messaging

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/google/uuid"

	cloudevents "github.com/cloudevents/sdk-go/v2"

	"github.com/barkin94/notification-system/api/internal/db"
	"github.com/barkin94/notification-system/api/internal/service"
	"github.com/barkin94/notification-system/shared/events"
	sharedmessaging "github.com/barkin94/notification-system/shared/messaging"
	natsmsg "github.com/barkin94/notification-system/shared/messaging/nats"
	sharedotel "github.com/barkin94/notification-system/shared/otel"
)

// NotificationReader is the narrow read port for fetching notifications.
// Transaction is retrieved from context via txmanager.GetTx(ctx).
type NotificationReader interface {
	GetByIDs(ctx context.Context, ids []uuid.UUID) ([]*db.Notification, error)
}

// ScheduledDueConsumer consumes ScheduledNotificationDueEvent CloudEvents batches,
// bulk-fetches the full notification details in a single query, and writes
// NotificationReadyEvent CloudEvents to the outbox for reliable publishing.
type ScheduledDueConsumer struct {
	repo       NotificationReader
	outboxRepo sharedmessaging.OutboxRepository
	msgs       <-chan []natsmsg.Result[cloudevents.Event]
}

func NewScheduledDueConsumer(
	repo NotificationReader,
	outboxRepo sharedmessaging.OutboxRepository,
	msgs <-chan []natsmsg.Result[cloudevents.Event],
) *ScheduledDueConsumer {
	return &ScheduledDueConsumer{repo: repo, outboxRepo: outboxRepo, msgs: msgs}
}

func (c *ScheduledDueConsumer) Run(ctx context.Context) {
	natsmsg.ForEachBatch(ctx, c.msgs, c.handleBatch)
}

func (c *ScheduledDueConsumer) handleBatch(batch []natsmsg.Result[cloudevents.Event]) {
	// Parse IDs and map each uuid back to its Result for per-message ACK/NAK.
	ids := make([]uuid.UUID, 0, len(batch))
	resultByID := make(map[uuid.UUID]natsmsg.Result[cloudevents.Event], len(batch))

	for _, result := range batch {
		// Extract notification ID from CloudEvent data
		var dueData events.ScheduledNotificationDueData
		if err := json.Unmarshal(result.Event.Data(), &dueData); err != nil {
			slog.ErrorContext(result.Ctx, "scheduled due consumer: parse CloudEvent data", "error", err)
			sharedotel.RecordError(result.Ctx, err)
			_ = result.Msg.Nak()
			continue
		}

		id, err := uuid.Parse(dueData.NotificationID)
		if err != nil {
			slog.ErrorContext(result.Ctx, "scheduled due consumer: parse notification id", "id", dueData.NotificationID, "error", err)
			sharedotel.RecordError(result.Ctx, err)
			_ = result.Msg.Nak()
			continue
		}
		ids = append(ids, id)
		resultByID[id] = result
	}

	if len(ids) == 0 {
		return
	}

	// Single bulk DB fetch for the whole batch.
	notifs, err := c.repo.GetByIDs(batch[0].Ctx, ids)
	if err != nil {
		slog.ErrorContext(batch[0].Ctx, "scheduled due consumer: bulk fetch notifications", "count", len(ids), "notification_ids", ids, "error", err)
		for _, result := range resultByID {
			sharedotel.RecordError(result.Ctx, err)
			_ = result.Msg.Nak()
		}
		return
	}

	notifByID := make(map[uuid.UUID]*db.Notification, len(notifs))
	for _, n := range notifs {
		notifByID[n.ID] = n
	}

	for id, result := range resultByID {
		notif, ok := notifByID[id]
		if !ok {
			slog.ErrorContext(result.Ctx, "scheduled due consumer: notification not found", "id", id)
			_ = result.Msg.Nak()
			continue
		}

		dn := notif.ToDomain()
		outboxEvent, err := service.BuildNotificationReadyEvent(result.Ctx, notif.ID.String(), dn, priorityToTopic(notif.Priority))
		if err != nil {
			slog.ErrorContext(result.Ctx, "scheduled due consumer: build outbox event", "id", id, "error", err)
			sharedotel.RecordError(result.Ctx, err)
			_ = result.Msg.Nak()
			continue
		}

		if err := c.outboxRepo.Save(result.Ctx, outboxEvent); err != nil {
			slog.ErrorContext(result.Ctx, "scheduled due consumer: write to outbox", "id", id, "error", err)
			sharedotel.RecordError(result.Ctx, err)
			_ = result.Msg.Nak()
			continue
		}

		slog.InfoContext(result.Ctx, "scheduled notification dispatched to outbox", "id", id)
		_ = result.Msg.Ack()
	}
}

func priorityToTopic(priority string) string {
	switch priority {
	case "high":
		return "notify.high"
	case "low":
		return "notify.low"
	default:
		return "notify.normal"
	}
}
