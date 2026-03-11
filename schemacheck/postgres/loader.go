package postgrescheck

import (
	"context"

	"github.com/jmoiron/sqlx"

	"github.com/QuangTung97/dbc"
	"github.com/QuangTung97/dbc/schemacheck"
)

type InformationColumn struct {
	Database   string `db:"table_catalog"`
	Table      string `db:"table_name"`
	ColumnName string `db:"column_name"`
	DataType   string `db:"data_type"`
	IsNullable string `db:"is_nullable"`
}

func (InformationColumn) TableName() string {
	return "information_schema.columns"
}

var InformationColumnSchema = dbc.RegisterSchema(
	func(s *dbc.Schema[InformationColumn], table *InformationColumn) {
		dbc.SchemaPrimaryKey(s, &table.Database)
		dbc.SchemaPrimaryKey(s, &table.Table)
		dbc.SchemaPrimaryKey(s, &table.ColumnName)

		dbc.SchemaConst(s, &table.DataType)
		dbc.SchemaConst(s, &table.IsNullable)
	},
	dbc.WithSchemaNoRegistering(),
)

// -----------------------------------------------------------------------------------

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

	result, err := s.loadTableColumnsInfo(ctx)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (s *mysqlLoader) loadTableColumnsInfo(ctx context.Context) ([]schemacheck.TableInfo, error) {
	columnList, err := s.exec.SelectCond(ctx, func(b *dbc.CondBuilder[InformationColumn], table *InformationColumn) {
		dbc.CondEqual(b, &table.Database, s.databaseName)
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
