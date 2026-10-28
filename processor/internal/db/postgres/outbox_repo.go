package postgres

import (
	"github.com/barkin94/notification-system/processor/internal/db"
	"github.com/barkin94/notification-system/shared/messaging"
	"github.com/uptrace/bun"
)

// OutboxEvent represents an event stored in the outbox table (exported for adapter)
type OutboxEvent = messaging.OutboxEvent

func NewOutboxRepository(db bun.IDB) db.OutboxRepository {
	return messaging.NewOutboxRepository(db)
}