package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/uptrace/bun"

	"github.com/barkin94/insider-notification/deliveryscheduler/internal/db"
	"github.com/barkin94/insider-notification/shared/txmanager/buntx"
)

type pgScheduledNotificationRepo struct {
	store buntx.Store
}

var _ db.ScheduledNotificationRepository = (*pgScheduledNotificationRepo)(nil)

func NewScheduledNotificationRepository(db bun.IDB) db.ScheduledNotificationRepository {
	return &pgScheduledNotificationRepo{store: buntx.NewStore(db)}
}

func (r *pgScheduledNotificationRepo) UpsertAll(ctx context.Context, notifications []*db.ScheduledNotification) error {
	if len(notifications) == 0 {
		return nil
	}
	tx, err := r.store.Tx(ctx)
	if err != nil {
		return err
	}
	_, err = tx.NewInsert().
		Model(&notifications).
		On("CONFLICT (notification_id) DO UPDATE").
		Set("scheduled_at = EXCLUDED.scheduled_at").
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("pg batch upsert scheduled notifications: %w", err)
	}
	return nil
}

func (r *pgScheduledNotificationRepo) DeleteByNotificationID(ctx context.Context, notificationID string) error {
	tx, err := r.store.Tx(ctx)
	if err != nil {
		return err
	}
	_, err = tx.NewDelete().
		Model((*db.ScheduledNotification)(nil)).
		Where("notification_id = ?", notificationID).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("pg delete scheduled notification: %w", err)
	}
	return nil
}

func (r *pgScheduledNotificationRepo) DeleteByScheduledAtBeforeReturning(ctx context.Context, before time.Time, limit int) ([]*db.ScheduledNotification, error) {
	if limit < 1 {
		limit = 1
	}
	tx, err := r.store.Tx(ctx)
	if err != nil {
		return nil, err
	}
	var notifications []*db.ScheduledNotification
	err = tx.NewRaw(`
		DELETE FROM scheduled_notifications
		WHERE notification_id IN (
			SELECT notification_id FROM scheduled_notifications
			WHERE scheduled_at IS NOT NULL AND scheduled_at <= ?
			ORDER BY scheduled_at ASC
			LIMIT ?
			FOR UPDATE SKIP LOCKED
		)
		RETURNING *
		`,
		before, limit,
	).Scan(ctx, &notifications)
	if err != nil {
		return nil, fmt.Errorf("pg find and delete due scheduled notifications: %w", err)
	}
	return notifications, nil
}
