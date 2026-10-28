package dispatcher

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	db "github.com/barkin94/notification-system/deliveryscheduler/internal/db"
	"github.com/barkin94/notification-system/shared/events"
	"github.com/barkin94/notification-system/shared/messaging"
	"github.com/barkin94/notification-system/shared/txmanager"
)

// ScheduledNotificationDispatcher claims due scheduled notifications and writes them to outbox.
type ScheduledNotificationDispatcher struct {
	repo       db.ScheduledNotificationRepository
	outboxRepo db.OutboxRepository
	txManager  txmanager.TransactionManager
	batch      int
}

func NewScheduledNotificationDispatcher(
	repo db.ScheduledNotificationRepository,
	outboxRepo db.OutboxRepository,
	txManager txmanager.TransactionManager,
	batch int,
) *ScheduledNotificationDispatcher {
	if batch < 1 {
		batch = 100
	}
	return &ScheduledNotificationDispatcher{
		repo:       repo,
		outboxRepo: outboxRepo,
		txManager:  txManager,
		batch:      batch,
	}
}

func (d *ScheduledNotificationDispatcher) Tick(ctx context.Context) {
	for {
		var notifications []*db.ScheduledNotification
		err := d.txManager.RunInTx(ctx, func(ctx context.Context) error {
			var err error
			notifications, err = d.repo.DeleteByScheduledAtBeforeReturning(ctx, time.Now().UTC(), d.batch)
			if err != nil {
				return err
			}
			if len(notifications) == 0 {
				return nil
			}

			// Write each due notification as a CloudEvent to outbox
			outboxEvents := make([]db.OutboxEvent, len(notifications))
			for i, n := range notifications {
				outboxEvent, err := BuildScheduledNotificationDueEvent(ctx, n.NotificationID, "notify.scheduled.due")
				if err != nil {
					slog.ErrorContext(ctx, "failed to build outbox event", "notification_id", n.NotificationID, "error", err)
					return fmt.Errorf("build outbox event: %w", err)
				}

				// Override trace headers from stored metadata
				var traceHeaders map[string]string
				if n.TraceMetadata != nil {
					traceHeaders = make(map[string]string)
					for k, v := range n.TraceMetadata {
						if s, ok := v.(string); ok {
							traceHeaders[k] = s
						}
					}
				}
				outboxEvent.TraceHeaders, _ = json.Marshal(traceHeaders)

				outboxEvents[i] = outboxEvent
			}

			// Use transaction to save outbox events
			if len(outboxEvents) > 0 {
				if err := d.outboxRepo.Save(ctx, outboxEvents...); err != nil {
					return fmt.Errorf("write to outbox: %w", err)
				}
			}

			return nil
		})
		if err != nil {
			slog.ErrorContext(ctx, "delivery scheduler: tick failed", "error", err)
			return
		}
		if len(notifications) == 0 {
			return
		}
	}
}

// BuildScheduledNotificationDueEvent creates an OutboxEvent for ScheduledNotificationDue.
func BuildScheduledNotificationDueEvent(ctx context.Context, notificationID, topic string) (messaging.OutboxEvent, error) {
	data := events.ScheduledNotificationDueData{
		NotificationID: notificationID,
	}
	return messaging.BuildOutboxEvent(ctx, data, events.TypeScheduledNotificationDue, events.SourceScheduler, notificationID, "scheduled_notification", topic)
}
