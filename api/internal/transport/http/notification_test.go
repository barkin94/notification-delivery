package handler_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/barkin94/notification-system/api/internal/db"
	"github.com/barkin94/notification-system/api/internal/domain/notification"
	"github.com/barkin94/notification-system/api/internal/service"
	handler "github.com/barkin94/notification-system/api/internal/transport/http"
	sharedErrors "github.com/barkin94/notification-system/shared/genericerrors"
)

// --- mock service ---

type mockService struct {
	createFn       func(ctx context.Context, n notification.Notification) (*db.Notification, error)
	getByIDFn      func(ctx context.Context, id uuid.UUID) (*db.Notification, error)
	listFn         func(ctx context.Context, f db.ListFilter) ([]*db.Notification, int, *uuid.UUID, error)
	cancelFn       func(ctx context.Context, id uuid.UUID) (*db.Notification, error)
	updateStatusFn func(ctx context.Context, id uuid.UUID, status string) error
	createBatchFn  func(ctx context.Context, ns []notification.Notification) (uuid.UUID, []service.BatchResult, error)
}

func (m *mockService) Create(ctx context.Context, n notification.Notification) (*db.Notification, error) {
	return m.createFn(ctx, n)
}
func (m *mockService) GetByID(ctx context.Context, id uuid.UUID) (*db.Notification, error) {
	return m.getByIDFn(ctx, id)
}
func (m *mockService) List(ctx context.Context, f db.ListFilter) ([]*db.Notification, int, *uuid.UUID, error) {
	return m.listFn(ctx, f)
}
func (m *mockService) Cancel(ctx context.Context, id uuid.UUID) (*db.Notification, error) {
	return m.cancelFn(ctx, id)
}
func (m *mockService) UpdateStatus(ctx context.Context, id uuid.UUID, status string) error {
	if m.updateStatusFn != nil {
		return m.updateStatusFn(ctx, id, status)
	}
	return nil
}
func (m *mockService) CreateBatch(ctx context.Context, ns []notification.Notification) (uuid.UUID, []service.BatchResult, error) {
	return m.createBatchFn(ctx, ns)
}

func newRouter(svc service.NotificationService) http.Handler {
	return handler.NewRouter(handler.Deps{
		Service: svc,
		DB:      nil,
	})
}

func newNotif() *db.Notification {
	now := time.Now().UTC()
	n := &db.Notification{
		Recipient:   "+15551234567",
		Channel:     "sms",
		Content:     "hi",
		Priority:    "normal",
		Status:      "pending",
		MaxAttempts: 4,
	}
	n.ID = uuid.New()
	n.CreatedAt = now
	n.UpdatedAt = now
	return n
}

// --- POST /notifications ---

func TestCreateNotification_201(t *testing.T) {
	n := newNotif()
	svc := &mockService{createFn: func(_ context.Context, _ notification.Notification) (*db.Notification, error) {
		return n, nil
	}}

	body := `{"recipient":"+15551234567","channel":"sms","content":"hi","priority":"normal"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/notifications", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	newRouter(svc).ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201", w.Code)
	}
	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp) //nolint:errcheck,gosec
	if resp["id"] == nil {
		t.Error("response missing id field")
	}
	if resp["status"] != "pending" {
		t.Errorf("status = %v, want pending", resp["status"])
	}
}

func TestCreateNotification_422_missingContent(t *testing.T) {
	svc := &mockService{}

	body := `{"recipient":"+15551234567","channel":"sms","priority":"normal"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/notifications", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	newRouter(svc).ServeHTTP(w, req)

	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422", w.Code)
	}
}

func TestCreateNotification_422_missingPriority(t *testing.T) {
	svc := &mockService{}

	body := `{"recipient":"+15551234567","channel":"sms","content":"hi"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/notifications", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	newRouter(svc).ServeHTTP(w, req)

	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422", w.Code)
	}
}

func TestCreateNotification_422_invalidChannel(t *testing.T) {
	svc := &mockService{}

	body := `{"recipient":"+15551234567","channel":"fax","content":"hi","priority":"normal"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/notifications", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	newRouter(svc).ServeHTTP(w, req)

	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422", w.Code)
	}
}

// --- GET /notifications ---

func TestListNotifications_pagination(t *testing.T) {
	svc := &mockService{listFn: func(_ context.Context, f db.ListFilter) ([]*db.Notification, int, *uuid.UUID, error) {
		return []*db.Notification{newNotif()}, 42, nil, nil
	}}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/notifications?page_size=10", nil)
	w := httptest.NewRecorder()

	newRouter(svc).ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp) //nolint:errcheck,gosec
	pg, _ := resp["pagination"].(map[string]any)
	if pg["total"] != float64(42) {
		t.Errorf("total = %v, want 42", pg["total"])
	}
	if pg["page_size"] != float64(10) {
		t.Errorf("page_size = %v, want 10", pg["page_size"])
	}
	if pg["next_cursor"] != nil {
		t.Errorf("next_cursor = %v, want null on offset path", pg["next_cursor"])
	}
}

func TestListNotifications_filterByStatus(t *testing.T) {
	var gotFilter db.ListFilter
	svc := &mockService{listFn: func(_ context.Context, f db.ListFilter) ([]*db.Notification, int, *uuid.UUID, error) {
		gotFilter = f
		return nil, 0, nil, nil
	}}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/notifications?status=delivered", nil)
	w := httptest.NewRecorder()

	newRouter(svc).ServeHTTP(w, req)

	if gotFilter.Status != "delivered" {
		t.Errorf("filter status = %q, want delivered", gotFilter.Status)
	}
}

// --- GET /notifications/:id ---

func TestGetNotification_200(t *testing.T) {
	n := newNotif()
	svc := &mockService{getByIDFn: func(_ context.Context, _ uuid.UUID) (*db.Notification, error) {
		return n, nil
	}}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/notifications/"+n.ID.String(), nil)
	w := httptest.NewRecorder()

	newRouter(svc).ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp) //nolint:errcheck,gosec
	if resp["id"] != n.ID.String() {
		t.Errorf("id = %v, want %v", resp["id"], n.ID.String())
	}
}

func TestGetNotification_404(t *testing.T) {
	svc := &mockService{getByIDFn: func(_ context.Context, _ uuid.UUID) (*db.Notification, error) {
		return nil, db.ErrNotFound
	}}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/notifications/"+uuid.New().String(), nil)
	w := httptest.NewRecorder()

	newRouter(svc).ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.Code)
	}
}

// --- POST /notifications/:id/cancel ---

func TestCancelNotification_200(t *testing.T) {
	n := newNotif()
	n.Status = "cancelled"
	svc := &mockService{cancelFn: func(_ context.Context, _ uuid.UUID) (*db.Notification, error) {
		return n, nil
	}}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/notifications/"+n.ID.String()+"/cancel", nil)
	w := httptest.NewRecorder()

	newRouter(svc).ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp) //nolint:errcheck,gosec
	if resp["status"] != "cancelled" {
		t.Errorf("status = %v, want cancelled", resp["status"])
	}
}

func TestCancelNotification_409(t *testing.T) {
	svc := &mockService{cancelFn: func(_ context.Context, _ uuid.UUID) (*db.Notification, error) {
		return nil, &sharedErrors.ConflictError{Message: "invalid status transition"}
	}}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/notifications/"+uuid.New().String()+"/cancel", nil)
	w := httptest.NewRecorder()

	newRouter(svc).ServeHTTP(w, req)

	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", w.Code)
	}
}

// --- POST /notifications/batch ---

func TestCreateBatch_207(t *testing.T) {
	id1 := uuid.New()
	svc := &mockService{createBatchFn: func(_ context.Context, ns []notification.Notification) (uuid.UUID, []service.BatchResult, error) {
		return uuid.New(), []service.BatchResult{
			{Index: 0, Status: "accepted", ID: &id1},
		}, nil
	}}

	body, _ := json.Marshal(map[string]any{
		"notifications": []map[string]any{
			{"recipient": "+15551234567", "channel": "sms", "content": "ok", "priority": "normal"},
			{"recipient": "+2", "channel": "fax", "content": "bad", "priority": "normal"},
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/notifications/batch", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	newRouter(svc).ServeHTTP(w, req)

	if w.Code != http.StatusMultiStatus {
		t.Fatalf("status = %d, want 207", w.Code)
	}
	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp) //nolint:errcheck,gosec
	if resp["accepted"] != float64(1) {
		t.Errorf("accepted = %v, want 1", resp["accepted"])
	}
	if resp["rejected"] != float64(1) {
		t.Errorf("rejected = %v, want 1", resp["rejected"])
	}
}

func TestCreateBatch_400_tooLarge(t *testing.T) {
	svc := &mockService{}

	items := make([]map[string]any, 1001)
	for i := range items {
		items[i] = map[string]any{"recipient": "+1", "channel": "sms", "content": "hi", "priority": "normal"}
	}
	body, _ := json.Marshal(map[string]any{"notifications": items})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/notifications/batch", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	newRouter(svc).ServeHTTP(w, req)

	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422", w.Code)
	}
}

// --- GET /notifications cursor pagination ---

func encodeCursorForTest(id uuid.UUID) string {
	return id.String()
}

func TestListNotifications_NoCursor(t *testing.T) {
	n := newNotif()
	svc := &mockService{
		listFn: func(_ context.Context, _ db.ListFilter) ([]*db.Notification, int, *uuid.UUID, error) {
			return []*db.Notification{n}, 1, nil, nil
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/notifications", nil)
	w := httptest.NewRecorder()
	newRouter(svc).ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp) //nolint:errcheck,gosec
	pagination := resp["pagination"].(map[string]any)
	if pagination["next_cursor"] != nil {
		t.Errorf("next_cursor = %v, want null", pagination["next_cursor"])
	}
}

func TestListNotifications_WithCursor_NextPageExists(t *testing.T) {
	n := newNotif()
	nextID, _ := uuid.NewV7()
	svc := &mockService{
		listFn: func(_ context.Context, f db.ListFilter) ([]*db.Notification, int, *uuid.UUID, error) {
			return []*db.Notification{n}, 50, &nextID, nil
		},
	}

	cursorID, _ := uuid.NewV7()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/notifications?cursor="+encodeCursorForTest(cursorID), nil)
	w := httptest.NewRecorder()
	newRouter(svc).ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp) //nolint:errcheck,gosec
	pagination := resp["pagination"].(map[string]any)
	if pagination["next_cursor"] == nil {
		t.Error("expected next_cursor, got null")
	}
}

func TestListNotifications_WithCursor_LastPage(t *testing.T) {
	n := newNotif()
	svc := &mockService{
		listFn: func(_ context.Context, f db.ListFilter) ([]*db.Notification, int, *uuid.UUID, error) {
			return []*db.Notification{n}, 10, nil, nil
		},
	}

	cursorID, _ := uuid.NewV7()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/notifications?cursor="+encodeCursorForTest(cursorID), nil)
	w := httptest.NewRecorder()
	newRouter(svc).ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp) //nolint:errcheck,gosec
	pagination := resp["pagination"].(map[string]any)
	if pagination["next_cursor"] != nil {
		t.Errorf("next_cursor = %v, want null on last page", pagination["next_cursor"])
	}
}

func TestListNotifications_InvalidCursor(t *testing.T) {
	svc := &mockService{}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/notifications?cursor=notvalidbase64!!!", nil)
	w := httptest.NewRecorder()
	newRouter(svc).ServeHTTP(w, req)

	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422", w.Code)
	}
}

func TestListNotifications_FiltersPreservedWithCursor(t *testing.T) {
	var capturedFilter db.ListFilter
	svc := &mockService{
		listFn: func(_ context.Context, f db.ListFilter) ([]*db.Notification, int, *uuid.UUID, error) {
			capturedFilter = f
			return nil, 0, nil, nil
		},
	}

	cursorID, _ := uuid.NewV7()
	url := "/api/v1/notifications?cursor=" + encodeCursorForTest(cursorID) + "&status=pending&channel=sms"
	req := httptest.NewRequest(http.MethodGet, url, nil)
	w := httptest.NewRecorder()
	newRouter(svc).ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if capturedFilter.Status != "pending" {
		t.Errorf("Status = %q, want pending", capturedFilter.Status)
	}
	if capturedFilter.Channel != "sms" {
		t.Errorf("Channel = %q, want sms", capturedFilter.Channel)
	}
	if capturedFilter.CursorID == nil {
		t.Error("CursorID should be set when cursor param is present")
	}
}
