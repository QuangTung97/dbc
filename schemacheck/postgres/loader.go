package postgrescheck

import (
	"cmp"
	"context"
	"slices"

	"github.com/jmoiron/sqlx"

	"github.com/QuangTung97/dbc"
	"github.com/QuangTung97/dbc/schemacheck"
)

type InformationColumn struct {
	Database   string `db:"table_catalog"`
	Schema     string `db:"table_schema"`
	Table      string `db:"table_name"`
	ColumnName string `db:"column_name"`
	DataType   string `db:"udt_name"`
	IsNullable string `db:"is_nullable"`
}

func (InformationColumn) TableName() string {
	return "information_schema.columns"
}

var InformationColumnSchema = dbc.RegisterSchema(
	func(s *dbc.Schema[InformationColumn], table *InformationColumn) {
		dbc.SchemaPrimaryKey(s, &table.Database)
		dbc.SchemaPrimaryKey(s, &table.Schema)
		dbc.SchemaPrimaryKey(s, &table.Table)
		dbc.SchemaPrimaryKey(s, &table.ColumnName)

		dbc.SchemaConst(s, &table.DataType)
		dbc.SchemaConst(s, &table.IsNullable)
	},
	dbc.WithSchemaNoRegistering(),
)

// -----------------------------------------------------------------------------------

type IndexColumn struct {
	SchemaName string `db:"schema_name"`
	IndexName  string `db:"index_name"`
	TableName  string `db:"table_name"`
	ColumnName string `db:"column_name"`
	Position   int    `db:"column_position"`
	IsUnique   bool   `db:"is_unique"`
	IsPrimary  bool   `db:"is_primary"`
}

// -----------------------------------------------------------------------------------

func NewLoader(db *sqlx.DB, databaseName string) schemacheck.TableLoader {
	exec, err := dbc.NewExecutor(dbc.DialectPostgres, InformationColumnSchema)
	if err != nil {
		panic(err)
	}

	return &postgresLoader{
		provider:     dbc.NewProvider(db),
		exec:         exec,
		databaseName: databaseName,
	}
}

type postgresLoader struct {
	provider     dbc.Provider
	exec         *dbc.Executor[InformationColumn]
	databaseName string
}

func (s *postgresLoader) LoadAll(ctx context.Context) ([]schemacheck.TableInfo, error) {
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

func (s *postgresLoader) loadTableColumnsInfo(ctx context.Context) ([]schemacheck.TableInfo, error) {
	columnList, err := s.exec.SelectCond(ctx, func(b *dbc.CondBuilder[InformationColumn], table *InformationColumn) {
		dbc.CondEqual(b, &table.Database, s.databaseName)
		dbc.CondEqual(b, &table.Schema, "public")
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

type schemaIndexKey struct {
	Table     string
	IndexName string
}

func (s *postgresLoader) loadIndexInfos(ctx context.Context, result []schemacheck.TableInfo) error {
	query := `
SELECT
    ns.nspname AS schema_name,
    i.relname AS index_name,
    t.relname AS table_name,
    a.attname AS column_name,
    k.n AS column_position,
    ix.indisunique AS is_unique,
    ix.indisprimary AS is_primary
FROM
    pg_index ix
    JOIN pg_class i ON i.oid = ix.indexrelid
    JOIN pg_class t ON t.oid = ix.indrelid
    JOIN pg_namespace ns ON ns.oid = t.relnamespace
    JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(attnum, n) ON TRUE
    JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum
WHERE
    ns.nspname = 'public'
`
	var allIndexColumns []IndexColumn
	err := dbc.GetReadonly(ctx).SelectContext(ctx, &allIndexColumns, query)
	if err != nil {
		return err
	}

	indexColMap := map[schemaIndexKey][]IndexColumn{}
	var indexKeys []schemaIndexKey
	for _, col := range allIndexColumns {
		key := schemaIndexKey{
			Table:     col.TableName,
			IndexName: col.IndexName,
		}
		prev := indexColMap[key]
		if len(prev) == 0 {
			indexKeys = append(indexKeys, key)
		}
		indexColMap[key] = append(prev, col)
	}

	tableMap := map[string]*schemacheck.TableInfo{}
	for i := range result {
		table := &result[i]
		tableMap[table.Name] = table
	}

	for _, key := range indexKeys {
		colList := indexColMap[key]
		slices.SortFunc(colList, func(a, b IndexColumn) int {
			return cmp.Compare(a.Position, b.Position)
		})

		columns := make([]string, 0, len(colList))

		isPrimary := false
		isUnique := false

		for _, col := range colList {
			columns = append(columns, col.ColumnName)
			if col.IsPrimary {
				isPrimary = true
			}
			if col.IsUnique {
				isUnique = true
			}
		}

		table := tableMap[key.Table]

		if isPrimary {
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
