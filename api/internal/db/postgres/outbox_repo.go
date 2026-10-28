package postgres

import (
	"github.com/barkin94/insider-notification/api/internal/db"
	"github.com/barkin94/insider-notification/shared/messaging"
	"github.com/uptrace/bun"
)

// OutboxEvent represents an event stored in the outbox table (exported for adapter)
type OutboxEvent = messaging.OutboxEvent

func NewOutboxRepository(db *bun.DB) db.OutboxRepository {
	return messaging.NewOutboxRepository(db)
}