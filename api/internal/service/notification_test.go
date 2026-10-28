package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	cloudevents "github.com/cloudevents/sdk-go/v2"

	"github.com/barkin94/notification-system/api/internal/db"
	"github.com/barkin94/notification-system/api/internal/domain/notification"
	"github.com/barkin94/notification-system/api/internal/service"
	apipub "github.com/barkin94/notification-system/api/public"
	"github.com/barkin94/notification-system/shared/events"
	sharedErrors "github.com/barkin94/notification-system/shared/genericerrors"
)

// --- mock repo ---

type mockNotifRepo struct {
	createFn           func(ctx context.Context, n *db.Notification) error
	createBatchFn      func(ctx context.Context, ns []*db.Notification) error
	getByIDFn          func(ctx context.Context, id uuid.UUID) (*db.Notification, error)
	getByIDsFn         func(ctx context.Context, ids []uuid.UUID) ([]*db.Notification, error)
	listFn             func(ctx context.Context, f db.ListFilter) ([]*db.Notification, int, *uuid.UUID, error)
	updateStatusFn     func(ctx context.Context, id uuid.UUID, status string) (*db.Notification, error)
	findScheduledDueFn func(ctx context.Context) ([]*db.Notification, error)
}

func (m *mockNotifRepo) Create(ctx context.Context, n *db.Notification) error {
	if m.createFn != nil {
		return m.createFn(ctx, n)
	}
	return nil
}
func (m *mockNotifRepo) CreateBatch(ctx context.Context, ns []*db.Notification) error {
	if m.createBatchFn != nil {
		return m.createBatchFn(ctx, ns)
	}
	return nil
}
func (m *mockNotifRepo) GetByID(ctx context.Context, id uuid.UUID) (*db.Notification, error) {
	if m.getByIDFn != nil {
		return m.getByIDFn(ctx, id)
	}
	return nil, db.ErrNotFound
}
func (m *mockNotifRepo) GetByIDs(ctx context.Context, ids []uuid.UUID) ([]*db.Notification, error) {
	if m.getByIDsFn != nil {
		return m.getByIDsFn(ctx, ids)
	}
	return nil, nil
}
func (m *mockNotifRepo) List(ctx context.Context, f db.ListFilter) ([]*db.Notification, int, *uuid.UUID, error) {
	if m.listFn != nil {
		return m.listFn(ctx, f)
	}
	return nil, 0, nil, nil
}
func (m *mockNotifRepo) UpdateStatus(ctx context.Context, id uuid.UUID, status string) (*db.Notification, error) {
	if m.updateStatusFn != nil {
		return m.updateStatusFn(ctx, id, status)
	}
	n := &db.Notification{Status: status}
	n.ID = id
	return n, nil
}
func (m *mockNotifRepo) FindScheduledDue(ctx context.Context) ([]*db.Notification, error) {
	if m.findScheduledDueFn != nil {
		return m.findScheduledDueFn(ctx)
	}
	return nil, nil
}

// --- mock outbox repo ---

type mockOutboxRepo struct {
	saveFn           func(ctx context.Context, events ...db.OutboxEvent) error
	getUnpublishedFn func(ctx context.Context, limit int) ([]db.OutboxEvent, error)
	markPublishedFn  func(ctx context.Context, ids []string, publishedAt time.Time) error
	incrementRetryFn func(ctx context.Context, ids []string) error
}

func (m *mockOutboxRepo) Save(ctx context.Context, events ...db.OutboxEvent) error {
	if m.saveFn != nil {
		return m.saveFn(ctx, events...)
	}
	return nil
}

func (m *mockOutboxRepo) GetUnpublished(ctx context.Context, limit int) ([]db.OutboxEvent, error) {
	if m.getUnpublishedFn != nil {
		return m.getUnpublishedFn(ctx, limit)
	}
	return nil, nil
}

func (m *mockOutboxRepo) MarkPublished(ctx context.Context, ids []string, publishedAt time.Time) error {
	if m.markPublishedFn != nil {
		return m.markPublishedFn(ctx, ids, publishedAt)
	}
	return nil
}

func (m *mockOutboxRepo) IncrementRetryCount(ctx context.Context, ids []string) error {
	if m.incrementRetryFn != nil {
		return m.incrementRetryFn(ctx, ids)
	}
	return nil
}

// --- helpers ---

type mockTxManager struct {
	runFn func(ctx context.Context, fn func(ctx context.Context) error) error
}

func (m *mockTxManager) RunInTx(ctx context.Context, fn func(ctx context.Context) error) error {
	if m.runFn != nil {
		return m.runFn(ctx, fn)
	}
	return fn(ctx)
}

func okRepo() *mockNotifRepo {
	return &mockNotifRepo{
		createFn: func(_ context.Context, _ *db.Notification) error { return nil },
	}
}

func okOutboxRepo() *mockOutboxRepo {
	return &mockOutboxRepo{}
}

func newSvc(repo db.NotificationRepository, outboxRepo db.OutboxRepository) service.NotificationService {
	txManager := &mockTxManager{
		runFn: func(ctx context.Context, fn func(ctx context.Context) error) error {
			return fn(ctx)
		},
	}
	return service.NewNotificationService(repo, outboxRepo, txManager)
}

func validNotification(channel apipub.Channel, priority apipub.Priority) notification.Notification {
	var n notification.Notification
	n.SetChannel(channel)          //nolint:errcheck,gosec
	n.SetRecipient("+15551234567") //nolint:errcheck,gosec
	n.SetContent("hello")          //nolint:errcheck,gosec
	n.SetPriority(priority)        //nolint:errcheck,gosec
	return n
}

func assertCloudEvent(t *testing.T, payload []byte, expectedType, expectedTopic string) {
	var ce cloudevents.Event
	if err := json.Unmarshal(payload, &ce); err != nil {
		t.Fatalf("failed to unmarshal CloudEvent: %v", err)
	}
	if ce.Type() != expectedType {
		t.Errorf("CloudEvent type = %q, want %q", ce.Type(), expectedType)
	}
	// Verify data can be unmarshaled
	switch expectedType {
	case events.TypeNotificationReady:
		var data events.NotificationReadyData
		if err := json.Unmarshal(ce.Data(), &data); err != nil {
			t.Errorf("failed to unmarshal ready data: %v", err)
		}
	case events.TypeNotificationsScheduled:
		var data events.NotificationsScheduledData
		if err := json.Unmarshal(ce.Data(), &data); err != nil {
			t.Errorf("failed to unmarshal scheduled data: %v", err)
		}
	case events.TypeNotificationScheduleCancelled:
		var data events.NotificationScheduleCancelledData
		if err := json.Unmarshal(ce.Data(), &data); err != nil {
			t.Errorf("failed to unmarshal cancelled data: %v", err)
		}
	}
}

func pendingNotif(id uuid.UUID) *db.Notification {
	n := &db.Notification{Status: "pending"}
	n.ID = id
	return n
}

// --- tests ---

func TestCreate_success(t *testing.T) {
	var capturedEvent db.OutboxEvent
	outboxRepo := okOutboxRepo()
	outboxRepo.saveFn = func(_ context.Context, events ...db.OutboxEvent) error {
		capturedEvent = events[0]
		return nil
	}

	svc := newSvc(okRepo(), outboxRepo)

	n, err := svc.Create(context.Background(), validNotification(apipub.ChannelSMS, apipub.PriorityHigh))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n.Status != string(apipub.StatusPending) {
		t.Errorf("status = %q, want pending", n.Status)
	}
	assertCloudEvent(t, capturedEvent.Payload, events.TypeNotificationReady, "notify.high")
	if capturedEvent.Topic != "notify.high" {
		t.Errorf("topic = %q, want notify.high", capturedEvent.Topic)
	}
}

func TestCreate_publishFailure(t *testing.T) {
	outboxRepo := okOutboxRepo()
	outboxRepo.saveFn = func(_ context.Context, _ ...db.OutboxEvent) error {
		return errors.New("db error")
	}
	svc := newSvc(okRepo(), outboxRepo)
	_, err := svc.Create(context.Background(), validNotification(apipub.ChannelSMS, apipub.PriorityNormal))
	if err == nil {
		t.Fatal("expected error from outbox")
	}
}

func TestCreate_scheduled(t *testing.T) {
	var capturedEvent db.OutboxEvent
	outboxRepo := okOutboxRepo()
	outboxRepo.saveFn = func(_ context.Context, events ...db.OutboxEvent) error {
		capturedEvent = events[0]
		return nil
	}

	svc := newSvc(okRepo(), outboxRepo)
	scheduledTime := time.Now().Add(time.Hour)
	n := validNotification(apipub.ChannelEmail, apipub.PriorityLow)
	n.SetDeliverAfter(&scheduledTime) //nolint:errcheck,gosec

	result, err := svc.Create(context.Background(), n)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertCloudEvent(t, capturedEvent.Payload, events.TypeNotificationsScheduled, "notify.scheduled")
	if capturedEvent.Topic != "notify.scheduled" {
		t.Errorf("topic = %q, want notify.scheduled", capturedEvent.Topic)
	}
	if result.DeliverAfter == nil {
		t.Error("expected DeliverAfter to be set")
	}
}

func TestCancel_success(t *testing.T) {
	id := uuid.New()
	repo := okRepo()
	repo.getByIDFn = func(_ context.Context, _ uuid.UUID) (*db.Notification, error) {
		return pendingNotif(id), nil
	}
	var capturedEvent db.OutboxEvent
	outboxRepo := okOutboxRepo()
	outboxRepo.saveFn = func(_ context.Context, events ...db.OutboxEvent) error {
		capturedEvent = events[0]
		return nil
	}
	svc := newSvc(repo, outboxRepo)
	n, err := svc.Cancel(context.Background(), id)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n.Status != string(apipub.StatusCancelled) {
		t.Errorf("status = %q, want cancelled", n.Status)
	}
	assertCloudEvent(t, capturedEvent.Payload, events.TypeNotificationScheduleCancelled, "notify.schedule.cancelled")
	if capturedEvent.Topic != "notify.schedule.cancelled" {
		t.Errorf("topic = %q, want notify.schedule.cancelled", capturedEvent.Topic)
	}
}

func TestCancel_publishFailure(t *testing.T) {
	id := uuid.New()
	repo := okRepo()
	repo.getByIDFn = func(_ context.Context, _ uuid.UUID) (*db.Notification, error) {
		return pendingNotif(id), nil
	}
	outboxRepo := okOutboxRepo()
	outboxRepo.saveFn = func(_ context.Context, _ ...db.OutboxEvent) error {
		return errors.New("db error")
	}
	svc := newSvc(repo, outboxRepo)
	_, err := svc.Cancel(context.Background(), id)
	if err == nil {
		t.Fatal("expected error from outbox")
	}
}

func TestCancel_notFound(t *testing.T) {
	repo := okRepo()
	repo.getByIDFn = func(_ context.Context, _ uuid.UUID) (*db.Notification, error) {
		return nil, db.ErrNotFound
	}
	svc := newSvc(repo, okOutboxRepo())
	_, err := svc.Cancel(context.Background(), uuid.New())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var notFoundErr *sharedErrors.NotFoundError
	if !errors.As(err, &notFoundErr) {
		t.Fatalf("expected NotFoundError, got %T: %v", err, err)
	}
}

func TestCancel_invalidTransition(t *testing.T) {
	id := uuid.New()
	repo := okRepo()
	repo.getByIDFn = func(_ context.Context, _ uuid.UUID) (*db.Notification, error) {
		n := &db.Notification{Status: "delivered"}
		n.ID = id
		return n, nil
	}
	svc := newSvc(repo, okOutboxRepo())
	_, err := svc.Cancel(context.Background(), id)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var conflictErr *sharedErrors.ConflictError
	if !errors.As(err, &conflictErr) {
		t.Fatalf("expected ConflictError, got %T: %v", err, err)
	}
}
