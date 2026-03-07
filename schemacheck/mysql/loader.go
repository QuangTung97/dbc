package mysqlcheck

import (
	"context"

	"github.com/jmoiron/sqlx"

	"github.com/QuangTung97/dbc"
	"github.com/QuangTung97/dbc/schemacheck"
)

type InformationColumn struct {
	TableSchema string `db:"TABLE_SCHEMA"`
	Table       string `db:"TABLE_NAME"`
	ColumnName  string `db:"COLUMN_NAME"`
	DataType    string `db:"DATA_TYPE"`
	IsNullable  string `db:"IS_NULLABLE"`
}

func (InformationColumn) TableName() string {
	return "INFORMATION_SCHEMA.COLUMNS"
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

func NewLoader(db *sqlx.DB, databaseName string) schemacheck.TableLoader {
	exec, err := dbc.NewExecutor(dbc.DialectMySQL, InformationColumnSchema)
	if err != nil {
		panic(err)
	}

	return &mysqlLoader{
		provider:     dbc.NewProvider(db),
		exec:         exec,
		databaseName: databaseName,
	}
}

type mysqlLoader struct {
	provider     dbc.Provider
	exec         *dbc.Executor[InformationColumn]
	databaseName string
}

func (s *mysqlLoader) LoadAll(ctx context.Context) ([]schemacheck.TableInfo, error) {
	ctx = s.provider.Readonly(ctx)
	columnList, err := s.exec.SelectCond(ctx, func(b *dbc.CondBuilder[InformationColumn], table *InformationColumn) {
		dbc.CondEqual(b, &table.TableSchema, s.databaseName)
	})
	if err != nil {
		return nil, err
	}

	columnsByTable := map[string][]InformationColumn{}
	var tableList []string
	for _, col := range columnList {
		prev := columnsByTable[col.Table]
		if len(prev) == 0 {
			tableList = append(tableList, col.Table)
		}
		columnsByTable[col.Table] = append(prev, col)
	}

	result := make([]schemacheck.TableInfo, 0, len(tableList))
	for _, table := range tableList {
		tableCols := columnsByTable[table]

		columns := make([]schemacheck.ColumnInfo, 0, len(tableCols))
		for _, col := range tableCols {
			isNullable := false
			if col.IsNullable != "NO" {
				isNullable = true
			}

			columns = append(columns, schemacheck.ColumnInfo{
				Name:     col.ColumnName,
				DataType: col.DataType,
				Nullable: isNullable,
			})
		}

		result = append(result, schemacheck.TableInfo{
			Name:    table,
			Columns: columns,
		})
	}

	return result, nil
}
