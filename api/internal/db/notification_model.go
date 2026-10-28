package db

import (
	"time"

	"github.com/google/uuid"
	"github.com/uptrace/bun"

	"github.com/barkin94/notification-system/api/internal/domain/notification"
	apipub "github.com/barkin94/notification-system/api/public"
	sharedbun "github.com/barkin94/notification-system/shared/bun"
)

type Notification struct {
	bun.BaseModel `bun:"table:notifications"`
	sharedbun.IDModel
	sharedbun.TimestampModel
	BatchID      *uuid.UUID `bun:"batch_id"`
	Recipient    string     `bun:"recipient"`
	Channel      string     `bun:"channel"`
	Content      string     `bun:"content"`
	Priority     string     `bun:"priority"`
	Status       string     `bun:"status"`
	DeliverAfter *time.Time `bun:"deliver_after"`
	MaxAttempts  int        `bun:"max_attempts"`
}

func (Notification) From(n notification.Notification, batchID *uuid.UUID) (*Notification, error) {
	idModel, err := sharedbun.NewIDModel()
	if err != nil {
		return nil, err
	}
	return &Notification{
		IDModel:      idModel,
		BatchID:      batchID,
		Recipient:    n.GetRecipient(),
		Channel:      string(n.GetChannel()),
		Content:      n.GetContent(),
		Priority:     string(n.GetPriority()),
		Status:       string(apipub.StatusPending),
		MaxAttempts:  n.GetMaxAttempts(),
		DeliverAfter: n.GetDeliverAfter(),
	}, nil
}

func (n *Notification) ToDomain() *notification.Notification {
	return notification.New(
		apipub.Channel(n.Channel),
		n.Recipient,
		n.Content,
		apipub.Priority(n.Priority),
		apipub.Status(n.Status),
		n.DeliverAfter,
		n.MaxAttempts,
	)
}

func (n *Notification) GetID() string        { return n.ID.String() }
func (n *Notification) GetChannel() string   { return n.Channel }
func (n *Notification) GetRecipient() string { return n.Recipient }
func (n *Notification) GetContent() string   { return n.Content }
func (n *Notification) GetPriority() string  { return n.Priority }
func (n *Notification) GetMaxAttempts() int  { return n.MaxAttempts }
