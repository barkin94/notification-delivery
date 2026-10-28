package db

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// ListFilter holds query parameters for listing notifications.
// Set CursorID for keyset pagination; leave nil for offset pagination.
type ListFilter struct {
	Status   string
	Channel  string
	BatchID  *uuid.UUID
	DateFrom *time.Time
	DateTo   *time.Time
	PageSize int
	// keyset pagination — takes precedence over Page/Sort/Order when set
	CursorID *uuid.UUID
	// offset pagination
	Page  int
	Sort  string
	Order string
}

// NotificationRepository is the port for notification persistence.
// Transaction is retrieved from context via txmanager.GetTx(ctx).
// If no transaction in context, uses default repository connection.
type NotificationRepository interface {
	Create(ctx context.Context, n *Notification) error
	CreateBatch(ctx context.Context, ns []*Notification) error
	GetByID(ctx context.Context, id uuid.UUID) (*Notification, error)
	GetByIDs(ctx context.Context, ids []uuid.UUID) ([]*Notification, error)
	List(ctx context.Context, f ListFilter) ([]*Notification, int, *uuid.UUID, error)
	UpdateStatus(ctx context.Context, id uuid.UUID, status string) (*Notification, error)
	FindScheduledDue(ctx context.Context) ([]*Notification, error)
}
