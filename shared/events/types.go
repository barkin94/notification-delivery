package events

import (
	"time"
)

// Event types as constants for CloudEvents type attribute
const (
	TypeNotificationReady         = "com.barkin94.notification-system.notification.ready.v1"
	TypeNotificationsScheduled    = "com.barkin94.notification-system.notification.scheduled.v1"
	TypeNotificationScheduleCancelled = "com.barkin94.notification-system.notification.schedule.cancelled.v1"
	TypeScheduledNotificationDue  = "com.barkin94.notification-system.notification.scheduled.due.v1"
	TypeNotificationDeliveryResult = "com.barkin94.notification-system.notification.delivery.result.v1"
)

// Source identifiers per service
const (
	SourceAPI        = "notification-system-api"
	SourceScheduler  = "notification-system-scheduler"
	SourceProcessor  = "notification-system-processor"
)

// Delivery status constants
const (
	DeliveryStatusDelivered = "delivered"
	DeliveryStatusFailed    = "failed"
)

// NATS subject constants
const (
	SubjectHigh   = "notify.high"
	SubjectNormal = "notify.normal"
	SubjectLow    = "notify.low"
)

// Consumer group constants
const (
	ConsumerGroupHigh   = "processor-high"
	ConsumerGroupNormal = "processor-normal"
	ConsumerGroupLow    = "processor-low"
)

// NotificationReadyData represents the data payload for NotificationReady event
type NotificationReadyData struct {
	NotificationID string `json:"notificationId"`
	Channel        string `json:"channel"`
	Recipient      string `json:"recipient"`
	Content        string `json:"content"`
	Priority       string `json:"priority"`
	MaxAttempts    int    `json:"maxAttempts"`
}

func (n *NotificationReadyData) GetNotificationID() string { return n.NotificationID }
func (n *NotificationReadyData) GetChannel() string        { return n.Channel }
func (n *NotificationReadyData) GetRecipient() string      { return n.Recipient }
func (n *NotificationReadyData) GetContent() string        { return n.Content }
func (n *NotificationReadyData) GetPriority() string       { return n.Priority }
func (n *NotificationReadyData) GetMaxAttempts() int       { return n.MaxAttempts }

// NotificationsScheduledData represents the data payload for NotificationsScheduled event
type NotificationsScheduledData struct {
	Notifications []ScheduledNotificationItem `json:"notifications"`
}

type ScheduledNotificationItem struct {
	NotificationID string    `json:"notificationId"`
	ScheduledAt    time.Time `json:"scheduledAt"`
}

// NotificationScheduleCancelledData represents the data payload for NotificationScheduleCancelled event
type NotificationScheduleCancelledData struct {
	NotificationID string `json:"notificationId"`
}

// ScheduledNotificationDueData represents the data payload for ScheduledNotificationDue event
type ScheduledNotificationDueData struct {
	NotificationID string `json:"notificationId"`
}

// NotificationDeliveryResultData represents the data payload for NotificationDeliveryResult event
type NotificationDeliveryResultData struct {
	NotificationID    string `json:"notificationId"`
	Status            string `json:"status"`
	AttemptNumber     int    `json:"attemptNumber"`
	HTTPStatusCode    int    `json:"httpStatusCode"`
	ErrorMessage      string `json:"errorMessage,omitempty"`
	ProviderMessageID string `json:"providerMessageId,omitempty"`
	LatencyMS         int    `json:"latencyMs"`
}