package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/barkin94/notification-system/api/internal/db"
	"github.com/barkin94/notification-system/api/internal/domain/notification"
	apipub "github.com/barkin94/notification-system/api/public"
	"github.com/barkin94/notification-system/shared/events"
	"github.com/barkin94/notification-system/shared/genericerrors"
	"github.com/barkin94/notification-system/shared/messaging"
	"github.com/barkin94/notification-system/shared/txmanager"
)

// BatchResult holds the outcome of one item in a batch create operation.
type BatchResult struct {
	Index  int
	Status string // "accepted" | "rejected"
	ID     *uuid.UUID
	Error  *string
}

// NotificationService defines the business operations for notifications.
type NotificationService interface {
	Create(ctx context.Context, n notification.Notification) (*db.Notification, error)
	GetByID(ctx context.Context, id uuid.UUID) (*db.Notification, error)
	List(ctx context.Context, filter db.ListFilter) ([]*db.Notification, int, *uuid.UUID, error)
	Cancel(ctx context.Context, id uuid.UUID) (*db.Notification, error)
	UpdateStatus(ctx context.Context, id uuid.UUID, status string) error
	CreateBatch(ctx context.Context, ns []notification.Notification) (uuid.UUID, []BatchResult, error)
}

type notificationService struct {
	repo       db.NotificationRepository
	outboxRepo messaging.OutboxWriter
	txManager  txmanager.TransactionManager
}

func NewNotificationService(
	repo db.NotificationRepository,
	outboxRepo messaging.OutboxWriter,
	txManager txmanager.TransactionManager,
) NotificationService {
	return &notificationService{repo: repo, outboxRepo: outboxRepo, txManager: txManager}
}

func (s *notificationService) Create(ctx context.Context, n notification.Notification) (*db.Notification, error) {
	var entity *db.Notification
	err := s.txManager.RunInTx(ctx, func(ctx context.Context) error {
		var err error
		entity, err = s.persist(ctx, n, nil)
		if err != nil {
			return err
		}
		return s.writeToOutbox(ctx, entity)
	})
	if err != nil {
		return nil, err
	}
	return entity, nil
}

func (s *notificationService) GetByID(ctx context.Context, id uuid.UUID) (*db.Notification, error) {
	return s.repo.GetByID(ctx, id)
}

func (s *notificationService) List(ctx context.Context, filter db.ListFilter) ([]*db.Notification, int, *uuid.UUID, error) {
	return s.repo.List(ctx, filter)
}

func (s *notificationService) Cancel(ctx context.Context, id uuid.UUID) (*db.Notification, error) {
	var updated *db.Notification
	err := s.txManager.RunInTx(ctx, func(ctx context.Context) error {
		entity, err := s.repo.GetByID(ctx, id)
		if err != nil {
			if errors.Is(err, db.ErrNotFound) {
				return &genericerrors.NotFoundError{Message: "notification not found"}
			}
			return err
		}

		dn := entity.ToDomain()
		if err := dn.Transition(apipub.StatusCancelled); err != nil {
			return &genericerrors.ConflictError{Message: err.Error()}
		}

		var err2 error
		updated, err2 = s.repo.UpdateStatus(ctx, id, string(apipub.StatusCancelled))
		if err2 != nil {
			return err2
		}

		// Write cancellation event to outbox
		outboxEvent, err := BuildNotificationCancelledEvent(ctx, id.String(), "notify.schedule.cancelled")
		if err != nil {
			return fmt.Errorf("write cancel event to outbox: %w", err)
		}
		return s.outboxRepo.Save(ctx, outboxEvent)
	})
	if err != nil {
		return nil, err
	}
	return updated, nil
}

func (s *notificationService) UpdateStatus(ctx context.Context, id uuid.UUID, status string) error {
	return s.txManager.RunInTx(ctx, func(ctx context.Context) error {
		entity, err := s.repo.GetByID(ctx, id)
		if err != nil {
			return err
		}

		dn := entity.ToDomain()
		if err := dn.Transition(apipub.Status(status)); err != nil {
			return err
		}

		_, err = s.repo.UpdateStatus(ctx, id, status)
		return err
	})
}

func (s *notificationService) CreateBatch(ctx context.Context, ns []notification.Notification) (uuid.UUID, []BatchResult, error) {
	batchID := uuid.New()
	results := make([]BatchResult, len(ns))

	// Convert all to entities and validate
	entities := make([]*db.Notification, len(ns))
	for i, n := range ns {
		entity, err := db.Notification{}.From(n, &batchID)
		if err != nil {
			msg := err.Error()
			results[i] = BatchResult{Index: i, Status: "rejected", Error: &msg}
			continue
		}
		entities[i] = entity
		results[i] = BatchResult{Index: i, Status: "accepted", ID: &entity.ID}
	}

	// Collect valid entities and persist in batch
	acceptedEntities := make([]*db.Notification, 0, len(ns))
	acceptedIndices := make([]int, 0, len(ns))
	for i, entity := range entities {
		if entity != nil {
			acceptedEntities = append(acceptedEntities, entity)
			acceptedIndices = append(acceptedIndices, i)
		}
	}

	if len(acceptedEntities) > 0 {
		err := s.txManager.RunInTx(ctx, func(ctx context.Context) error {
			if err := s.repo.CreateBatch(ctx, acceptedEntities); err != nil {
				msg := fmt.Sprintf("create notifications: %v", err)
				for _, i := range acceptedIndices {
					results[i].Status = "rejected"
					results[i].Error = &msg
					results[i].ID = nil
				}
				return err
			}

			// Separate scheduled and immediate notifications
			var scheduledItems []events.ScheduledNotificationItem
			immediate := make([]*db.Notification, 0)

			for _, entity := range acceptedEntities {
				if entity.DeliverAfter != nil {
					scheduledItems = append(scheduledItems, events.ScheduledNotificationItem{
						NotificationID: entity.ID.String(),
						ScheduledAt:    *entity.DeliverAfter,
					})
				} else {
					immediate = append(immediate, entity)
				}
			}

			// Write scheduled notifications event to outbox
			if len(scheduledItems) > 0 {
				outboxEvent, err := BuildNotificationScheduledEvent(ctx, batchID.String(), scheduledItems, "notify.scheduled")
				if err != nil {
					return fmt.Errorf("write scheduled events to outbox: %w", err)
				}
				if err := s.outboxRepo.Save(ctx, outboxEvent); err != nil {
					return fmt.Errorf("write scheduled events to outbox: %w", err)
				}
			}

			// Write immediate notifications to outbox
			for _, entity := range immediate {
				dn := entity.ToDomain()
				outboxEvent, err := BuildNotificationReadyEvent(ctx, entity.ID.String(), dn, priorityToTopic(entity.Priority))
				if err != nil {
					return fmt.Errorf("write ready event to outbox: %w", err)
				}
				if err := s.outboxRepo.Save(ctx, outboxEvent); err != nil {
					return fmt.Errorf("write ready event to outbox: %w", err)
				}
			}

			return nil
		})
		if err != nil {
			// Results already updated in the transaction
			return batchID, results, err
		}
	}

	return batchID, results, nil
}

func (s *notificationService) persist(ctx context.Context, n notification.Notification, batchID *uuid.UUID) (*db.Notification, error) {
	entity, err := db.Notification{}.From(n, batchID)
	if err != nil {
		return nil, err
	}
	if err := s.repo.Create(ctx, entity); err != nil {
		return nil, fmt.Errorf("create notification: %w", err)
	}
	return entity, nil
}

func (s *notificationService) writeToOutbox(ctx context.Context, entity *db.Notification) error {
	dn := entity.ToDomain()
	if entity.DeliverAfter != nil {
		// Scheduled notification
		outboxEvent, err := BuildNotificationScheduledEvent(ctx, entity.ID.String(), []events.ScheduledNotificationItem{
			{
				NotificationID: entity.ID.String(),
				ScheduledAt:    *entity.DeliverAfter,
			},
		}, "notify.scheduled")
		if err != nil {
			return err
		}
		return s.outboxRepo.Save(ctx, outboxEvent)
	}

	// Immediate notification
	outboxEvent, err := BuildNotificationReadyEvent(ctx, entity.ID.String(), dn, priorityToTopic(entity.Priority))
	if err != nil {
		return err
	}
	return s.outboxRepo.Save(ctx, outboxEvent)
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

// BuildNotificationReadyEvent creates an OutboxEvent for NotificationReady.
// Takes the notification ID (from persistence) and domain notification for the payload data.
func BuildNotificationReadyEvent(ctx context.Context, notificationID string, n *notification.Notification, topic string) (messaging.OutboxEvent, error) {
	data := events.NotificationReadyData{
		NotificationID: notificationID,
		Channel:        string(n.GetChannel()),
		Recipient:      n.GetRecipient(),
		Content:        n.GetContent(),
		Priority:       string(n.GetPriority()),
		MaxAttempts:    n.GetMaxAttempts(),
	}
	return messaging.BuildOutboxEvent(ctx, data, events.TypeNotificationReady, events.SourceAPI, notificationID, "notification", topic)
}

// BuildNotificationScheduledEvent creates an OutboxEvent for NotificationsScheduled (batch).
func BuildNotificationScheduledEvent(ctx context.Context, batchID string, items []events.ScheduledNotificationItem, topic string) (messaging.OutboxEvent, error) {
	data := events.NotificationsScheduledData{
		Notifications: items,
	}
	return messaging.BuildOutboxEvent(ctx, data, events.TypeNotificationsScheduled, events.SourceAPI, batchID, "batch", topic)
}

// BuildNotificationCancelledEvent creates an OutboxEvent for NotificationScheduleCancelled.
func BuildNotificationCancelledEvent(ctx context.Context, notificationID, topic string) (messaging.OutboxEvent, error) {
	data := events.NotificationScheduleCancelledData{
		NotificationID: notificationID,
	}
	return messaging.BuildOutboxEvent(ctx, data, events.TypeNotificationScheduleCancelled, events.SourceAPI, notificationID, "notification", topic)
}