package mysqlcheck

import "github.com/QuangTung97/dbc"

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
