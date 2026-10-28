package messaging_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"

	cloudevents "github.com/cloudevents/sdk-go/v2"

	"github.com/barkin94/notification-system/deliveryscheduler/internal/db"
	"github.com/barkin94/notification-system/shared/events"
)

// makeCloudEvent creates a CloudEvent for testing with the given data, type, and source.
func makeCloudEvent(data any, eventType, source string) cloudevents.Event {
	ce := cloudevents.NewEvent()
	ce.SetID(uuid.New().String())
	ce.SetSource(source)
	ce.SetType(eventType)
	ce.SetTime(time.Now().UTC())
	payload, _ := json.Marshal(data)
	if err := ce.SetData(cloudevents.ApplicationJSON, payload); err != nil {
		panic(err)
	}
	return ce
}

// MockRepository mocks ScheduledNotificationRepository for testing
type MockRepository struct {
	upsertedAll []*db.ScheduledNotification
}

func (m *MockRepository) UpsertAll(ctx context.Context, notifications []*db.ScheduledNotification) error {
	m.upsertedAll = append(m.upsertedAll, notifications...)
	return nil
}

func (m *MockRepository) DeleteByScheduledAtBeforeReturning(ctx context.Context, before time.Time, limit int) ([]*db.ScheduledNotification, error) {
	return nil, nil
}

func (m *MockRepository) DeleteByNotificationID(_ context.Context, _ string) error {
	return nil
}

func TestConsumer_BatchesNotifications_CloudEvents(t *testing.T) {
	items := []events.ScheduledNotificationItem{
		{
			NotificationID: uuid.New().String(),
			ScheduledAt:    time.Now().Add(time.Hour),
		},
		{
			NotificationID: uuid.New().String(),
			ScheduledAt:    time.Now().Add(2 * time.Hour),
		},
		{
			NotificationID: uuid.New().String(),
			ScheduledAt:    time.Now().Add(3 * time.Hour),
		},
	}

	// Create CloudEvent with NotificationsScheduledData
	scheduledData := events.NotificationsScheduledData{
		Notifications: items,
	}
	event := makeCloudEvent(scheduledData, events.TypeNotificationsScheduled, events.SourceAPI)

	// Unmarshal the CloudEvent data (not the full CloudEvent)
	var parsed events.NotificationsScheduledData
	if err := json.Unmarshal(event.Data(), &parsed); err != nil {
		t.Fatalf("unmarshal event data: %v", err)
	}

	if len(parsed.Notifications) != len(items) {
		t.Errorf("expected %d notifications, got %d", len(items), len(parsed.Notifications))
	}

	for i, item := range items {
		if parsed.Notifications[i].NotificationID != item.NotificationID {
			t.Errorf("item %d: expected ID %s, got %s", i, item.NotificationID, parsed.Notifications[i].NotificationID)
		}
	}

	// Verify the CloudEvent has correct type
	if event.Type() != events.TypeNotificationsScheduled {
		t.Errorf("CloudEvent type = %q, want %q", event.Type(), events.TypeNotificationsScheduled)
	}
	if event.Source() != events.SourceAPI {
		t.Errorf("CloudEvent source = %q, want %q", event.Source(), events.SourceAPI)
	}
}

func TestConsumer_CancelEvent_CloudEvents(t *testing.T) {
	cancelData := events.NotificationScheduleCancelledData{
		NotificationID: uuid.New().String(),
	}
	event := makeCloudEvent(cancelData, events.TypeNotificationScheduleCancelled, events.SourceAPI)

	// Unmarshal the CloudEvent data
	var parsed events.NotificationScheduleCancelledData
	if err := json.Unmarshal(event.Data(), &parsed); err != nil {
		t.Fatalf("unmarshal event data: %v", err)
	}

	if parsed.NotificationID != cancelData.NotificationID {
		t.Errorf("expected ID %s, got %s", cancelData.NotificationID, parsed.NotificationID)
	}

	// Verify the CloudEvent has correct type
	if event.Type() != events.TypeNotificationScheduleCancelled {
		t.Errorf("CloudEvent type = %q, want %q", event.Type(), events.TypeNotificationScheduleCancelled)
	}
	if event.Source() != events.SourceAPI {
		t.Errorf("CloudEvent source = %q, want %q", event.Source(), events.SourceAPI)
	}
}

func TestConsumer_ScheduledNotificationDue_CloudEvents(t *testing.T) {
	dueData := events.ScheduledNotificationDueData{
		NotificationID: uuid.New().String(),
	}
	event := makeCloudEvent(dueData, events.TypeScheduledNotificationDue, events.SourceScheduler)

	// Unmarshal the CloudEvent data
	var parsed events.ScheduledNotificationDueData
	if err := json.Unmarshal(event.Data(), &parsed); err != nil {
		t.Fatalf("unmarshal event data: %v", err)
	}

	if parsed.NotificationID != dueData.NotificationID {
		t.Errorf("expected ID %s, got %s", dueData.NotificationID, parsed.NotificationID)
	}

	// Verify the CloudEvent has correct type and source
	if event.Type() != events.TypeScheduledNotificationDue {
		t.Errorf("CloudEvent type = %q, want %q", event.Type(), events.TypeScheduledNotificationDue)
	}
	if event.Source() != events.SourceScheduler {
		t.Errorf("CloudEvent source = %q, want %q", event.Source(), events.SourceScheduler)
	}
}
