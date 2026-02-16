package dbc

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
)

type autoRebindTransaction struct {
	base Transaction
}

var _ Transaction = &autoRebindTransaction{}

func (tx *autoRebindTransaction) GetContext(
	ctx context.Context, dest any, query string, args ...any,
) error {
	query = tx.base.Rebind(query)
	return tx.base.GetContext(ctx, dest, query, args...)
}

func (tx *autoRebindTransaction) SelectContext(
	ctx context.Context, dest any, query string, args ...any,
) error {
	query = tx.base.Rebind(query)
	return tx.base.SelectContext(ctx, dest, query, args...)
}

func (tx *autoRebindTransaction) QueryxContext(
	ctx context.Context, query string, args ...any,
) (*sqlx.Rows, error) {
	query = tx.base.Rebind(query)
	return tx.base.QueryxContext(ctx, query, args...)
}

func (tx *autoRebindTransaction) Rebind(query string) string {
	return query
}

func (tx *autoRebindTransaction) ExecContext(
	ctx context.Context, query string, args ...any,
) (sql.Result, error) {
	query = tx.base.Rebind(query)
	return tx.base.ExecContext(ctx, query, args...)
}

func (tx *autoRebindTransaction) NamedExecContext(
	ctx context.Context, query string, arg any,
) (sql.Result, error) {
	query = tx.base.Rebind(query)
	return tx.base.NamedExecContext(ctx, query, arg)
}
