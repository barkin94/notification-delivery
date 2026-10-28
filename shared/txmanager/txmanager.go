package txmanager

import (
	"context"
	"errors"
)

// TransactionManager runs fn atomically. Repositories taking part in the
// transaction get it from the ctx passed to fn, so their signatures don't change.
// If ctx already carries a transaction, fn joins it instead of opening a new one.
type TransactionManager interface {
	RunInTx(ctx context.Context, fn func(ctx context.Context) error) error
}

// ErrNoTx is returned by writers that are only correct inside a transaction.
var ErrNoTx = errors.New("txmanager: operation requires an active transaction")
