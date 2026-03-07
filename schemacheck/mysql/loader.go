package mysqlcheck

import (
	"context"

	"github.com/jmoiron/sqlx"

	"github.com/QuangTung97/dbc"
	"github.com/QuangTung97/dbc/schemacheck"
)

type InformationColumn struct {
	TableSchema string `db:"table_schema"`
	Table       string `db:"table_name"`
	ColumnName  string `db:"column_name"`
	DataType    string `db:"data_type"`
	IsNullable  string `db:"is_nullable"`
}

func (InformationColumn) TableName() string {
	return "information_schema.columns"
}

var InformationColumnSchema = dbc.RegisterSchema(
	func(s *dbc.Schema[InformationColumn], table *InformationColumn) {
		dbc.SchemaPrimaryKey(s, &table.TableSchema)
		dbc.SchemaPrimaryKey(s, &table.Table)
		dbc.SchemaPrimaryKey(s, &table.ColumnName)

		dbc.SchemaConst(s, &table.DataType)
		dbc.SchemaConst(s, &table.IsNullable)
	},
	dbc.WithSchemaNoRegistering(),
)

func NewLoader(db *sqlx.DB) schemacheck.TableLoader {
	exec, err := dbc.NewExecutor(dbc.DialectMySQL, InformationColumnSchema)
	if err != nil {
		panic(err)
	}

	return &mysqlLoader{
		provider: dbc.NewProvider(db),
		exec:     exec,
	}
}

type mysqlLoader struct {
	provider dbc.Provider
	exec     *dbc.Executor[InformationColumn]
}

func (s *mysqlLoader) LoadAll(ctx context.Context) ([]schemacheck.TableInfo, error) {
	return nil, nil
}
