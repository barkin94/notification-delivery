package buntx

import (
	"context"

	"github.com/uptrace/bun"

	"github.com/barkin94/insider-notification/shared/txmanager"
)

type ctxKey struct{}

func txFrom(ctx context.Context) (bun.Tx, bool) {
	tx, ok := ctx.Value(ctxKey{}).(bun.Tx)
	return tx, ok
}

// Store wraps a bun.IDB and provides access to either the transaction
// from context (if present) or the fallback pool.
type Store struct {
	db bun.IDB
}

// NewStore creates a Store wrapping the given bun.IDB (typically *bun.DB).
func NewStore(db bun.IDB) Store {
	return Store{db: db}
}

// Conn returns the transaction carried by ctx, or the fallback pool.
// Use for reads and the outbox relay.
func (s Store) Conn(ctx context.Context) bun.IDB {
	if tx, ok := txFrom(ctx); ok {
		return tx
	}
	return s.db
}

// Tx returns the transaction carried by ctx, or txmanager.ErrNoTx.
// Use for writes that must be atomic with other operations.
func (s Store) Tx(ctx context.Context) (bun.Tx, error) {
	tx, ok := txFrom(ctx)
	if !ok {
		return bun.Tx{}, txmanager.ErrNoTx
	}
	return tx, nil
}

// ErrNoTx is re-exported for convenience.
var ErrNoTx = txmanager.ErrNoTx

type manager struct{ db *bun.DB }

var _ txmanager.TransactionManager = (*manager)(nil)

// NewManager creates a TransactionManager backed by the given *bun.DB.
func NewManager(db *bun.DB) txmanager.TransactionManager {
	return &manager{db: db}
}

func (m *manager) RunInTx(ctx context.Context, fn func(context.Context) error) error {
	if _, ok := txFrom(ctx); ok {
		return fn(ctx) // join the outer transaction
	}
	return m.db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		return fn(context.WithValue(ctx, ctxKey{}, tx))
	})
}