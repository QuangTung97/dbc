package sqlitecheck

import (
	"context"
	"database/sql"
	"fmt"

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
		cols, err := s.getTableColumns(ctx, tableName)
		if err != nil {
			return nil, err
		}

		resultColumns := make([]schemacheck.ColumnInfo, 0, len(cols))
		for _, col := range cols {
			resultColumns = append(resultColumns, schemacheck.ColumnInfo{
				Name:     col.name,
				DataType: col.colType,
				Nullable: col.isNotNull == 0,
			})
		}

		result = append(result, schemacheck.TableInfo{
			Name:    tableName,
			Columns: resultColumns,
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

	return result, nil
}
