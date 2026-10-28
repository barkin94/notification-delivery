package db

import (
	"github.com/barkin94/notification-system/shared/messaging"
)

// OutboxEvent is an alias for the shared outbox event type
type OutboxEvent = messaging.OutboxEvent

// OutboxRepository is the port for outbox persistence.
type OutboxRepository = messaging.OutboxRepository