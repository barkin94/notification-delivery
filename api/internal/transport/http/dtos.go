package handler

import (
	"time"

	"github.com/barkin94/notification-system/api/internal/db"
	"github.com/barkin94/notification-system/api/internal/domain/notification"
	apipub "github.com/barkin94/notification-system/api/public"
	sharedhandler "github.com/barkin94/notification-system/shared/handler"
)

type createRequest struct {
	Recipient    string  `json:"recipient"`
	Channel      string  `json:"channel"`
	Content      string  `json:"content"`
	Priority     string  `json:"priority"`
	MaxAttempts  *int    `json:"max_attempts"`
	DeliverAfter *string `json:"deliver_after"`
}

func (r createRequest) ToNotification() (notification.Notification, error) {
	var n notification.Notification

	if err := n.SetChannel(apipub.Channel(r.Channel)); err != nil {
		return n, err
	}
	if err := n.SetRecipient(r.Recipient); err != nil {
		return n, err
	}
	if err := n.SetContent(r.Content); err != nil {
		return n, err
	}
	if err := n.SetPriority(apipub.Priority(r.Priority)); err != nil {
		return n, err
	}
	if r.MaxAttempts != nil {
		if err := n.SetMaxAttempts(*r.MaxAttempts); err != nil {
			return n, err
		}
	}
	if r.DeliverAfter != nil {
		t, err := time.Parse(time.RFC3339, *r.DeliverAfter)
		if err != nil {
			return n, notification.ErrInvalidDeliverAfter()
		}
		n.SetDeliverAfter(&t)
	}

	return n, nil
}

type notificationResponse struct {
	ID           string  `json:"id"`
	BatchID      any     `json:"batch_id"`
	Recipient    string  `json:"recipient"`
	Channel      string  `json:"channel"`
	Content      string  `json:"content"`
	Priority     string  `json:"priority"`
	Status       string  `json:"status"`
	MaxAttempts  int     `json:"max_attempts"`
	DeliverAfter *string `json:"deliver_after"`
	CreatedAt    string  `json:"created_at"`
	UpdatedAt    string  `json:"updated_at"`
}

type listResponse struct {
	Data       []notificationResponse `json:"data"`
	Pagination paginationMeta         `json:"pagination"`
}

type paginationMeta struct {
	Page       int     `json:"page"`
	PageSize   int     `json:"page_size"`
	Total      int     `json:"total"`
	TotalPages int     `json:"total_pages"`
	NextCursor *string `json:"next_cursor"`
}

type cancelResponse struct {
	ID        string `json:"id"`
	Status    string `json:"status"`
	UpdatedAt string `json:"updated_at"`
}

type batchRequest struct {
	Notifications []createRequest `json:"notifications"`
}

type batchItemResult struct {
	Index  int                      `json:"index"`
	Status string                   `json:"status"`
	ID     *string                  `json:"id,omitempty"`
	Error  *sharedhandler.ErrorBody `json:"error,omitempty"`
}

type batchResponse struct {
	BatchID  string            `json:"batch_id"`
	Total    int               `json:"total"`
	Accepted int               `json:"accepted"`
	Rejected int               `json:"rejected"`
	Results  []batchItemResult `json:"results"`
}

func toNotificationResponse(n *db.Notification) notificationResponse {
	var batchID any
	if n.BatchID != nil {
		batchID = n.BatchID.String()
	}
	var deliverAfter *string
	if n.DeliverAfter != nil {
		s := n.DeliverAfter.Format(time.RFC3339)
		deliverAfter = &s
	}
	return notificationResponse{
		ID:           n.ID.String(),
		BatchID:      batchID,
		Recipient:    n.Recipient,
		Channel:      n.Channel,
		Content:      n.Content,
		Priority:     n.Priority,
		Status:       n.Status,
		MaxAttempts:  n.MaxAttempts,
		DeliverAfter: deliverAfter,
		CreatedAt:    n.CreatedAt.Format(time.RFC3339),
		UpdatedAt:    n.UpdatedAt.Format(time.RFC3339),
	}
}

func toNotificationResponses(ns []*db.Notification) []notificationResponse {
	data := make([]notificationResponse, len(ns))
	for i, n := range ns {
		data[i] = toNotificationResponse(n)
	}
	return data
}
