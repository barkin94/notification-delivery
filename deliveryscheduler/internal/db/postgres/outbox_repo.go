package postgres

import (
	"github.com/uptrace/bun"

	"github.com/barkin94/notification-system/deliveryscheduler/internal/db"
	"github.com/barkin94/notification-system/shared/messaging"
)

// OutboxEvent represents an event stored in the outbox table (exported for adapter)
type OutboxEvent = messaging.OutboxEvent

func NewOutboxRepository(db *bun.DB) db.OutboxRepository {
	return messaging.NewOutboxRepository(db)
}
