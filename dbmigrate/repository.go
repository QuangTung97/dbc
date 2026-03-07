package dbmigrate

import (
	"context"

	"github.com/jmoiron/sqlx"

	"github.com/QuangTung97/dbc"
	"github.com/QuangTung97/dbc/null"
)

type repository struct {
	provider dbc.Provider
	exec     *dbc.Executor[SchemaMigration]
}

func newRepository(db *sqlx.DB, dialect dbc.DatabaseDialect) *repository {
	exec, err := dbc.NewExecutor(dialect, SchemaMigrationSchema)
	if err != nil {
		panic(err)
	}

	return &repository{
		provider: dbc.NewProvider(db),
		exec:     exec,
	}
}

func (r *repository) getRow() (null.Null[SchemaMigration], error) {
	readCtx := r.provider.Readonly(context.Background())
	return r.exec.GetByID(readCtx, SchemaMigration{ID: migrationID})
}

func (r *repository) upsertRow(row SchemaMigration) error {
	ctx := r.provider.Autocommit(context.Background())

	return r.exec.InsertOrUpdateMulti(
		ctx,
		[]SchemaMigration{row},
		func(b *dbc.UpdateMultiBuilder[SchemaMigration], table *SchemaMigration) {
			dbc.UpdateMultiAssign(b, &table.Version)
			dbc.UpdateMultiAssign(b, &table.Filename)
			dbc.UpdateMultiAssign(b, &table.IsDirty)
		},
	)
}
