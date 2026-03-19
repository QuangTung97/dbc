package sqlitecheck

import (
	"cmp"
	"context"
	"database/sql"
	"fmt"
	"slices"

	"github.com/jmoiron/sqlx"

	"github.com/QuangTung97/dbc"
	"github.com/QuangTung97/dbc/schemacheck"
)

func NewLoader(db *sqlx.DB, _ string) schemacheck.TableLoader {
	return &sqlite3Loader{
		provider: dbc.NewProvider(db),
	}
}

type sqlite3Loader struct {
	provider dbc.Provider
}

func (s *sqlite3Loader) LoadAll(ctx context.Context) ([]schemacheck.TableInfo, error) {
	ctx = s.provider.Readonly(ctx)
	tx := dbc.GetReadonly(ctx)

	var tables []string
	err := tx.SelectContext(ctx, &tables, `SELECT name FROM sqlite_master WHERE type='table'`)
	if err != nil {
		return nil, err
	}

	var result []schemacheck.TableInfo
	for _, tableName := range tables {
		// get column list
		cols, err := s.getTableColumns(ctx, tableName)
		if err != nil {
			return nil, err
		}

		resultColumns := make([]schemacheck.ColumnInfo, 0, len(cols))
		var primaryKeys []columnInfo
		for _, col := range cols {
			if col.primaryKey > 0 {
				primaryKeys = append(primaryKeys, col)
			}

			resultColumns = append(resultColumns, schemacheck.ColumnInfo{
				Name:     col.name,
				DataType: col.colType,
				Nullable: col.isNotNull == 0,
			})
		}

		// get unique keys and indexes
		indexList, err := s.getTableIndexes(ctx, tableName)
		if err != nil {
			return nil, err
		}

		var resultIndexes []schemacheck.IndexInfo
		var resultUniqueKeys []schemacheck.UniqueKeyInfo
		for _, info := range indexList {
			if info.unique == 1 {
				resultUniqueKeys = append(resultUniqueKeys, schemacheck.UniqueKeyInfo{
					Columns: info.columns,
				})
			} else {
				resultIndexes = append(resultIndexes, schemacheck.IndexInfo{
					Name:    info.name,
					Columns: info.columns,
				})
			}
		}

		// sort primary key columns by position
		slices.SortFunc(primaryKeys, func(a, b columnInfo) int {
			return cmp.Compare(a.primaryKey, b.primaryKey)
		})
		primaryKeyCols := make([]string, 0, len(primaryKeys))
		for _, col := range primaryKeys {
			primaryKeyCols = append(primaryKeyCols, col.name)
		}

		result = append(result, schemacheck.TableInfo{
			Name:        tableName,
			Columns:     resultColumns,
			PrimaryKeys: primaryKeyCols,
			UniqueKeys:  resultUniqueKeys,
			Indexes:     resultIndexes,
		})
	}

	return result, nil
}

type columnInfo struct {
	num          int64
	name         string
	colType      string
	isNotNull    int64
	defaultValue sql.NullString
	primaryKey   int64
}

func (*sqlite3Loader) getTableColumns(ctx context.Context, tableName string) ([]columnInfo, error) {
	tx := dbc.GetReadonly(ctx)

	query := fmt.Sprintf(`PRAGMA table_info("%s")`, tableName)
	rows, err := tx.QueryxContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var result []columnInfo
	for rows.Next() {
		var info columnInfo
		err := rows.Scan(
			&info.num, &info.name, &info.colType,
			&info.isNotNull, &info.defaultValue, &info.primaryKey,
		)
		if err != nil {
			return nil, err
		}
		result = append(result, info)
	}

	return result, rows.Err()
}

type indexInfo struct {
	seq     int64
	name    string
	unique  int64
	origin  string
	partial int64

	columns []string
}

func (s *sqlite3Loader) getTableIndexes(ctx context.Context, tableName string) ([]indexInfo, error) {
	tx := dbc.GetReadonly(ctx)

	query := fmt.Sprintf(`PRAGMA index_list("%s")`, tableName)
	rows, err := tx.QueryxContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var result []indexInfo
	for rows.Next() {
		var info indexInfo
		err = rows.Scan(&info.seq, &info.name, &info.unique, &info.origin, &info.partial)
		if err != nil {
			return nil, err
		}

		if info.origin == "pk" {
			continue
		}
		result = append(result, info)
	}

	for i := range result {
		info := &result[i]
		cols, err := s.getIndexColumns(ctx, info.name)
		if err != nil {
			return nil, err
		}
		info.columns = cols
	}

	return result, rows.Err()
}

func (*sqlite3Loader) getIndexColumns(ctx context.Context, indexName string) ([]string, error) {
	tx := dbc.GetReadonly(ctx)

	query := fmt.Sprintf(`PRAGMA index_info("%s")`, indexName)
	rows, err := tx.QueryxContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var result []string
	for rows.Next() {
		var seq int64
		var cid int64
		var colName string
		if err := rows.Scan(&seq, &cid, &colName); err != nil {
			return nil, err
		}
		result = append(result, colName)
	}
	return result, rows.Err()

}
