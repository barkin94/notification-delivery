package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/barkin94/notification-system/api/internal/db"
	apipub "github.com/barkin94/notification-system/api/public"
	"github.com/barkin94/notification-system/shared/txmanager"
	"github.com/barkin94/notification-system/shared/txmanager/buntx"
)

func newNotification() *db.Notification {
	now := time.Now().UTC().Truncate(time.Millisecond)
	n := &db.Notification{
		Recipient:   "+905551234567",
		Channel:     string(apipub.ChannelSMS),
		Content:     "test content",
		Priority:    string(apipub.PriorityNormal),
		Status:      string(apipub.StatusPending),
		MaxAttempts: 4,
	}
	n.ID = mustV7()
	n.CreatedAt = now
	n.UpdatedAt = now
	return n
}

func mustV7() uuid.UUID {
	id, err := uuid.NewV7()
	if err != nil {
		panic(err)
	}
	return id
}

func TestNotificationRepo_Create_GetByID(t *testing.T) {
	ctx := context.Background()
	txMgr := buntx.NewManager(testDB)
	repo := NewNotificationRepository(testDB)

	n := newNotification()
	if err := txMgr.RunInTx(ctx, func(ctx context.Context) error {
		return repo.Create(ctx, n)
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, err := repo.GetByID(ctx, n.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.ID != n.ID {
		t.Errorf("ID = %v, want %v", got.ID, n.ID)
	}
	if got.Recipient != n.Recipient {
		t.Errorf("Recipient = %q, want %q", got.Recipient, n.Recipient)
	}
	if got.Status != string(apipub.StatusPending) {
		t.Errorf("Status = %q, want pending", got.Status)
	}
}

func TestNotificationRepo_GetByID_notFound(t *testing.T) {
	ctx := context.Background()
	repo := NewNotificationRepository(testDB)

	_, err := repo.GetByID(ctx, mustV7())
	if err != db.ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestNotificationRepo_UpdateStatus(t *testing.T) {
	ctx := context.Background()
	txMgr := buntx.NewManager(testDB)
	repo := NewNotificationRepository(testDB)

	n := newNotification()
	if err := txMgr.RunInTx(ctx, func(ctx context.Context) error {
		return repo.Create(ctx, n)
	}); err != nil {
		t.Fatalf("create notification: %v", err)
	}

	var updated *db.Notification
	if err := txMgr.RunInTx(ctx, func(ctx context.Context) error {
		u, err := repo.UpdateStatus(ctx, n.ID, string(apipub.StatusCancelled))
		if err != nil {
			return err
		}
		updated = u
		return nil
	}); err != nil {
		t.Fatalf("UpdateStatus: %v", err)
	}
	if updated.Status != string(apipub.StatusCancelled) {
		t.Errorf("Status = %q, want cancelled", updated.Status)
	}
}

func TestList_offset_pagination(t *testing.T) {
	ctx := context.Background()
	txMgr := buntx.NewManager(testDB)
	repo := NewNotificationRepository(testDB)

	batchID := mustV7()
	for range 5 {
		n := newNotification()
		n.BatchID = &batchID
		if err := txMgr.RunInTx(ctx, func(ctx context.Context) error {
			return repo.Create(ctx, n)
		}); err != nil {
			t.Fatalf("create notification: %v", err)
		}
	}

	results, total, nextCursor, err := repo.List(ctx, db.ListFilter{BatchID: &batchID, Page: 1, PageSize: 3})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if total != 5 {
		t.Errorf("total = %d, want 5", total)
	}
	if len(results) != 3 {
		t.Errorf("len(results) = %d, want 3", len(results))
	}
	if nextCursor == nil {
		t.Error("offset mode should return nextCursor when more pages exist")
	}
}

func TestList_offset_filterByStatus(t *testing.T) {
	ctx := context.Background()
	txMgr := buntx.NewManager(testDB)
	repo := NewNotificationRepository(testDB)

	batchID := mustV7()
	for range 3 {
		n := newNotification()
		n.BatchID = &batchID
		if err := txMgr.RunInTx(ctx, func(ctx context.Context) error {
			return repo.Create(ctx, n)
		}); err != nil {
			t.Fatalf("create notification: %v", err)
		}
	}
	n := newNotification()
	n.BatchID = &batchID
	n.Status = string(apipub.StatusDelivered)
	if err := txMgr.RunInTx(ctx, func(ctx context.Context) error {
		return repo.Create(ctx, n)
	}); err != nil {
		t.Fatalf("create notification: %v", err)
	}

	results, total, _, err := repo.List(ctx, db.ListFilter{
		BatchID: &batchID,
		Status:  string(apipub.StatusDelivered),
		Page:    1, PageSize: 20,
	})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if total != 1 || len(results) != 1 {
		t.Errorf("expected 1 delivered, got total=%d len=%d", total, len(results))
	}
}

func seed5(t *testing.T, txMgr txmanager.TransactionManager, repo db.NotificationRepository, batchID uuid.UUID) []*db.Notification {
	t.Helper()
	ctx := context.Background()
	for range 5 {
		n := newNotification()
		n.BatchID = &batchID
		if err := txMgr.RunInTx(ctx, func(ctx context.Context) error {
			return repo.Create(ctx, n)
		}); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	maxUUID := uuid.UUID{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	all, _, _, err := repo.List(ctx, db.ListFilter{BatchID: &batchID, PageSize: 10, CursorID: &maxUUID})
	if err != nil {
		t.Fatalf("seed fetch: %v", err)
	}
	return all
}

func TestList_cursor_firstPage(t *testing.T) {
	ctx := context.Background()
	txMgr := buntx.NewManager(testDB)
	repo := NewNotificationRepository(testDB)

	batchID := mustV7()
	all := seed5(t, txMgr, repo, batchID)

	maxUUID := uuid.UUID{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	results, total, nextCursor, err := repo.List(ctx, db.ListFilter{
		BatchID:  &batchID,
		PageSize: 3,
		CursorID: &maxUUID,
	})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if total != 5 {
		t.Errorf("total = %d, want 5", total)
	}
	if len(results) != 3 {
		t.Errorf("len(results) = %d, want 3", len(results))
	}
	if nextCursor == nil {
		t.Error("nextCursor should not be nil when more pages exist")
	}
	seededIDs := map[uuid.UUID]bool{}
	for _, n := range all {
		seededIDs[n.ID] = true
	}
	for _, n := range results {
		if !seededIDs[n.ID] {
			t.Errorf("result ID %v not in seeded set", n.ID)
		}
	}
}

func TestList_cursor_secondPage(t *testing.T) {
	ctx := context.Background()
	txMgr := buntx.NewManager(testDB)
	repo := NewNotificationRepository(testDB)

	batchID := mustV7()
	all := seed5(t, txMgr, repo, batchID)

	cursorID := all[2].ID
	page2, total, nextCursor, err := repo.List(ctx, db.ListFilter{
		BatchID:  &batchID,
		PageSize: 3,
		CursorID: &cursorID,
	})
	if err != nil {
		t.Fatalf("page2 List: %v", err)
	}
	if total != 5 {
		t.Errorf("total = %d, want 5", total)
	}
	if len(page2) != 2 {
		t.Errorf("len(page2) = %d, want 2", len(page2))
	}
	if nextCursor != nil {
		t.Error("nextCursor should be nil on last page")
	}
	if page2[0].ID != all[3].ID || page2[1].ID != all[4].ID {
		t.Error("page2 items do not match expected IDs")
	}
}

func TestList_cursor_lastPage_noNextCursor(t *testing.T) {
	ctx := context.Background()
	txMgr := buntx.NewManager(testDB)
	repo := NewNotificationRepository(testDB)

	batchID := mustV7()
	seed5(t, txMgr, repo, batchID)

	maxUUID := uuid.UUID{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	results, total, nextCursor, err := repo.List(ctx, db.ListFilter{
		BatchID:  &batchID,
		PageSize: 10,
		CursorID: &maxUUID,
	})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if total != 5 {
		t.Errorf("total = %d, want 5", total)
	}
	if len(results) != 5 {
		t.Errorf("len(results) = %d, want 5", len(results))
	}
	if nextCursor != nil {
		t.Error("nextCursor should be nil when all results fit in one page")
	}
}

func TestList_cursor_filtersPreserved(t *testing.T) {
	ctx := context.Background()
	txMgr := buntx.NewManager(testDB)
	repo := NewNotificationRepository(testDB)

	batchID := mustV7()
	for range 3 {
		n := newNotification()
		n.BatchID = &batchID
		n.Channel = string(apipub.ChannelSMS)
		if err := txMgr.RunInTx(ctx, func(ctx context.Context) error {
			return repo.Create(ctx, n)
		}); err != nil {
			t.Fatalf("create notification: %v", err)
		}
	}
	for range 4 {
		n := newNotification()
		n.BatchID = &batchID
		n.Channel = string(apipub.ChannelEmail)
		if err := txMgr.RunInTx(ctx, func(ctx context.Context) error {
			return repo.Create(ctx, n)
		}); err != nil {
			t.Fatalf("create notification: %v", err)
		}
	}

	maxUUID := uuid.UUID{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	results, total, _, err := repo.List(ctx, db.ListFilter{
		BatchID:  &batchID,
		Channel:  string(apipub.ChannelSMS),
		PageSize: 10,
		CursorID: &maxUUID,
	})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if total != 3 {
		t.Errorf("total = %d, want 3", total)
	}
	if len(results) != 3 {
		t.Errorf("len(results) = %d, want 3", len(results))
	}
	for _, n := range results {
		if n.Channel != string(apipub.ChannelSMS) {
			t.Errorf("expected channel sms, got %q", n.Channel)
		}
	}
}
