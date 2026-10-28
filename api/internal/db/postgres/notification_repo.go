package postgres

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"github.com/google/uuid"
	"github.com/uptrace/bun"

	"github.com/barkin94/insider-notification/api/internal/db"
	"github.com/barkin94/insider-notification/shared/txmanager/buntx"
)

type bunNotificationRepo struct {
	store buntx.Store
}

var _ db.NotificationRepository = (*bunNotificationRepo)(nil)

func NewNotificationRepository(db bun.IDB) db.NotificationRepository {
	return &bunNotificationRepo{store: buntx.NewStore(db)}
}

func (r *bunNotificationRepo) Create(ctx context.Context, n *db.Notification) error {
	tx, err := r.store.Tx(ctx)
	if err != nil {
		return err
	}
	_, err = tx.NewInsert().Model(n).Exec(ctx)
	return err
}

func (r *bunNotificationRepo) CreateBatch(ctx context.Context, ns []*db.Notification) error {
	if len(ns) == 0 {
		return nil
	}
	tx, err := r.store.Tx(ctx)
	if err != nil {
		return err
	}
	_, err = tx.NewInsert().Model(&ns).Exec(ctx)
	return err
}

func (r *bunNotificationRepo) GetByID(ctx context.Context, id uuid.UUID) (*db.Notification, error) {
	n := &db.Notification{}
	n.ID = id

	err := r.store.Conn(ctx).NewSelect().Model(n).WherePK().Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, db.ErrNotFound
	}
	return n, err
}

func (r *bunNotificationRepo) GetByIDs(ctx context.Context, ids []uuid.UUID) ([]*db.Notification, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	var ns []*db.Notification
	err := r.store.Conn(ctx).NewSelect().Model(&ns).Where("id IN (?)", bun.List(ids)).Scan(ctx)
	return ns, err
}

func (r *bunNotificationRepo) UpdateStatus(ctx context.Context, id uuid.UUID, status string) (*db.Notification, error) {
	tx, err := r.store.Tx(ctx)
	if err != nil {
		return nil, err
	}

	n := &db.Notification{}
	n.ID = id
	n.Status = status

	err = tx.NewUpdate().
		Model(n).
		OmitZero().
		WherePK().
		Returning("*").
		Scan(ctx)
	if err != nil {
		return nil, err
	}
	return n, err
}

func (r *bunNotificationRepo) FindScheduledDue(ctx context.Context) ([]*db.Notification, error) {
	var rows []*db.Notification
	err := r.store.Conn(ctx).NewSelect().
		Model(&rows).
		Where("deliver_after IS NOT NULL").
		Where("deliver_after <= NOW()").
		Where("status = 'pending'").
		OrderExpr("deliver_after ASC").
		Limit(500).
		Scan(ctx)
	return rows, err
}

func applyFilters(q *bun.SelectQuery, f db.ListFilter) *bun.SelectQuery {
	if f.Status != "" {
		q = q.Where("status = ?", f.Status)
	}
	if f.Channel != "" {
		q = q.Where("channel = ?", f.Channel)
	}
	if f.BatchID != nil {
		q = q.Where("batch_id = ?", f.BatchID)
	}
	if f.DateFrom != nil {
		q = q.Where("created_at >= ?", f.DateFrom)
	}
	if f.DateTo != nil {
		q = q.Where("created_at <= ?", f.DateTo)
	}
	return q
}

func (r *bunNotificationRepo) List(ctx context.Context, f db.ListFilter) ([]*db.Notification, int, *uuid.UUID, error) {
	pageSize := f.PageSize
	if pageSize < 1 {
		pageSize = 20
	}

	total64, err := applyFilters(r.store.Conn(ctx).NewSelect().Model((*db.Notification)(nil)), f).Count(ctx)
	if err != nil {
		return nil, 0, nil, err
	}
	total := int(total64)

	var ns []*db.Notification
	q := applyFilters(r.store.Conn(ctx).NewSelect().Model(&ns), f)

	if f.CursorID != nil {
		q = q.Where("id < ?", f.CursorID).OrderExpr("id DESC").Limit(int64(pageSize + 1))
	} else {
		sort := "created_at"
		if f.Sort == "updated_at" {
			sort = "updated_at"
		}
		order := "DESC"
		if strings.EqualFold(f.Order, "asc") {
			order = "ASC"
		}
		page := f.Page
		if page < 1 {
			page = 1
		}
		q = q.OrderExpr(sort + " " + order).Limit(int64(pageSize + 1)).Offset(int64((page - 1) * pageSize))
	}

	if err := q.Scan(ctx); err != nil {
		return nil, 0, nil, err
	}

	var nextCursor *uuid.UUID
	if len(ns) == pageSize+1 {
		id := ns[pageSize-1].ID
		nextCursor = &id
		ns = ns[:pageSize]
	}

	return ns, total, nextCursor, nil
}