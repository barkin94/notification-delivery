package db

import (
	"time"

	sharedbun "github.com/barkin94/notification-system/shared/bun"
	"github.com/uptrace/bun"
)

type ScheduledNotification struct {
	bun.BaseModel `bun:"table:scheduled_notifications"`
	sharedbun.TraceMetadataModel

	NotificationID string     `bun:"notification_id,pk,type:uuid,notnull"`
	ScheduledAt    *time.Time `bun:"scheduled_at,notnull"`
}
