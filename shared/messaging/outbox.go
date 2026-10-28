package messaging

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/uptrace/bun"

	"github.com/barkin94/insider-notification/shared/txmanager/buntx"
)

// OutboxEvent represents an event stored in the outbox table
type OutboxEvent struct {
	bun.BaseModel `bun:"table:outbox"`
	ID            uuid.UUID  `bun:"id,pk,type:uuid,default:gen_random_uuid()"`
	AggregateType string     `bun:"aggregate_type,notnull"`
	AggregateID   uuid.UUID  `bun:"aggregate_id,notnull,type:uuid"`
	EventType     string     `bun:"event_type,notnull"`
	Payload       []byte     `bun:"payload,notnull,type:jsonb"`
	Topic         string     `bun:"topic,notnull"`
	TraceHeaders  []byte     `bun:"trace_headers,type:jsonb"`
	CreatedAt     time.Time  `bun:"created_at,notnull,default:current_timestamp"`
	PublishedAt   *time.Time `bun:"published_at,type:timestamptz"`
	RetryCount    int        `bun:"retry_count,notnull,default:0"`
}

// OutboxWriter is the port for writing outbox events.
// Save requires an active transaction (via Store.Tx).
type OutboxWriter interface {
	Save(ctx context.Context, events ...OutboxEvent) error
}

// OutboxReader is the port for reading outbox events (used by the relay).
// GetUnpublished, MarkPublished, IncrementRetryCount use the fallback pool (Store.Conn).
type OutboxReader interface {
	GetUnpublished(ctx context.Context, limit int) ([]OutboxEvent, error)
	MarkPublished(ctx context.Context, ids []string, publishedAt time.Time) error
	IncrementRetryCount(ctx context.Context, ids []string) error
}

// OutboxRepository is kept so existing references still compile.
type OutboxRepository interface {
	OutboxWriter
	OutboxReader
}

type bunOutboxRepo struct {
	store buntx.Store
}

var _ OutboxRepository = (*bunOutboxRepo)(nil)

func NewOutboxRepository(db bun.IDB) OutboxRepository {
	return &bunOutboxRepo{store: buntx.NewStore(db)}
}

func (r *bunOutboxRepo) Save(ctx context.Context, events ...OutboxEvent) error {
	tx, err := r.store.Tx(ctx)
	if err != nil {
		return err
	}
	if len(events) == 0 {
		return nil
	}
	_, err = tx.NewInsert().Model(&events).Exec(ctx)
	return err
}

func (r *bunOutboxRepo) GetUnpublished(ctx context.Context, limit int) ([]OutboxEvent, error) {
	var events []OutboxEvent
	err := r.store.Conn(ctx).NewSelect().
		Model(&events).
		Where("published_at IS NULL").
		Order("created_at ASC").
		Limit(int64(limit)).
		Scan(ctx)
	return events, err
}

func (r *bunOutboxRepo) MarkPublished(ctx context.Context, ids []string, publishedAt time.Time) error {
	if len(ids) == 0 {
		return nil
	}
	uuidIDs := make([]uuid.UUID, len(ids))
	for i, id := range ids {
		uuidIDs[i] = uuid.MustParse(id)
	}
	_, err := r.store.Conn(ctx).NewUpdate().
		Model((*OutboxEvent)(nil)).
		Set("published_at = ?", publishedAt).
		Where("id IN (?)", bun.In(uuidIDs)).
		Exec(ctx)
	return err
}

func (r *bunOutboxRepo) IncrementRetryCount(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	uuidIDs := make([]uuid.UUID, len(ids))
	for i, id := range ids {
		uuidIDs[i] = uuid.MustParse(id)
	}
	_, err := r.store.Conn(ctx).NewUpdate().
		Model((*OutboxEvent)(nil)).
		Set("retry_count = retry_count + 1").
		Where("id IN (?)", bun.In(uuidIDs)).
		Exec(ctx)
	return err
}