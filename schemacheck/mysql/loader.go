package mysqlcheck

import (
	"context"
	"fmt"

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

// -----------------------------------------------------------------------------------

type InformationStat struct {
	TableSchema string `db:"TABLE_SCHEMA"`
	Table       string `db:"TABLE_NAME"`
	IndexName   string `db:"INDEX_NAME"`
	ColumnName  string `db:"COLUMN_NAME"`

	NonUnique int `db:"NON_UNIQUE"`
	Seq       int `db:"SEQ_IN_INDEX"`
}

func (InformationStat) TableName() string {
	return "INFORMATION_SCHEMA.STATISTICS"
}

var InformationStatSchema = dbc.RegisterSchema(func(s *dbc.Schema[InformationStat], table *InformationStat) {
	dbc.SchemaPrimaryKey(s, &table.TableSchema)
	dbc.SchemaPrimaryKey(s, &table.Table)
	dbc.SchemaPrimaryKey(s, &table.IndexName)
	dbc.SchemaPrimaryKey(s, &table.ColumnName)

	dbc.SchemaConst(s, &table.NonUnique)
	dbc.SchemaConst(s, &table.Seq)
})

// -----------------------------------------------------------------------------------

func NewLoader(db *sqlx.DB, databaseName string) schemacheck.TableLoader {
	exec, err := dbc.NewExecutor(dbc.DialectMySQL, InformationColumnSchema)
	if err != nil {
		panic(err)
	}

	statExec, err := dbc.NewExecutor(dbc.DialectMySQL, InformationStatSchema)
	if err != nil {
		panic(err)
	}

	return &mysqlLoader{
		provider:     dbc.NewProvider(db),
		exec:         exec,
		statExec:     statExec,
		databaseName: databaseName,
	}
}

type mysqlLoader struct {
	provider     dbc.Provider
	exec         *dbc.Executor[InformationColumn]
	statExec     *dbc.Executor[InformationStat]
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

	allStats, err := s.statExec.SelectCond(ctx, func(b *dbc.CondBuilder[InformationStat], table *InformationStat) {
		dbc.CondEqual(b, &table.TableSchema, s.databaseName)
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("ALL STATS", allStats)

	return result, nil
}
