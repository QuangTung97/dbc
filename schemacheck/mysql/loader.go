package mysqlcheck

import (
	"cmp"
	"context"
	"slices"

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

type schemaIndexKey struct {
	Table     string
	IndexName string
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

	result, err := s.loadTableColumnsInfo(ctx)
	if err != nil {
		return nil, err
	}

	if err := s.loadIndexInfos(ctx, result); err != nil {
		return nil, err
	}

	return result, nil
}

func (s *mysqlLoader) loadTableColumnsInfo(ctx context.Context) ([]schemacheck.TableInfo, error) {
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

func (s *mysqlLoader) loadIndexInfos(ctx context.Context, result []schemacheck.TableInfo) error {
	allStats, err := s.statExec.SelectCond(ctx, func(b *dbc.CondBuilder[InformationStat], table *InformationStat) {
		dbc.CondEqual(b, &table.TableSchema, s.databaseName)
	})
	if err != nil {
		return err
	}

	statMap := map[schemaIndexKey][]InformationStat{}
	var indexKeys []schemaIndexKey
	for _, stat := range allStats {
		key := schemaIndexKey{
			Table:     stat.Table,
			IndexName: stat.IndexName,
		}
		prev := statMap[key]
		if len(prev) == 0 {
			indexKeys = append(indexKeys, key)
		}
		statMap[key] = append(prev, stat)
	}

	tableMap := map[string]*schemacheck.TableInfo{}
	for i := range result {
		table := &result[i]
		tableMap[table.Name] = table
	}

	const primaryIndexName = "PRIMARY"
	for _, key := range indexKeys {
		stats := statMap[key]
		slices.SortFunc(stats, func(a, b InformationStat) int {
			return cmp.Compare(a.Seq, b.Seq)
		})

		columns := make([]string, 0, len(stats))
		isUnique := true
		for _, stat := range stats {
			columns = append(columns, stat.ColumnName)
			if stat.NonUnique != 0 {
				isUnique = false
			}
		}

		table := tableMap[key.Table]

		if key.IndexName == primaryIndexName {
			table.PrimaryKeys = columns
		} else if isUnique {
			uniqueKey := schemacheck.UniqueKeyInfo{Columns: columns}
			table.UniqueKeys = append(table.UniqueKeys, uniqueKey)
		} else {
			index := schemacheck.IndexInfo{
				Name:    key.IndexName,
				Columns: columns,
			}
			table.Indexes = append(table.Indexes, index)
		}
	}

	return nil
}
