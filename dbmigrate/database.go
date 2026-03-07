package dbmigrate

import (
	"fmt"

	"github.com/jmoiron/sqlx"

	"github.com/QuangTung97/dbc"
)

const sqlite3CreateTableQuery = `
CREATE TABLE IF NOT EXISTS schema_migration (
    id INTEGER NOT NULL PRIMARY KEY,
    version INTEGER NOT NULL,
    filename TEXT NOT NULL,
	is_dirty INTEGER NOT NULL
) STRICT;
`

const mysqlAndPostgresCreateTableQuery = `
CREATE TABLE IF NOT EXISTS schema_migration (
	id INTEGER NOT NULL PRIMARY KEY,
    version INTEGER NOT NULL,
    filename VARCHAR(1024) NOT NULL,
	is_dirty BOOLEAN NOT NULL
);
`

func createTableFunc(db *sqlx.DB, dialect dbc.DatabaseDialect) error {
	var query string
	switch dialect {
	case dbc.DialectSQLite3:
		query = sqlite3CreateTableQuery
	case dbc.DialectMySQL, dbc.DialectMySQL5x, dbc.DialectPostgres:
		query = mysqlAndPostgresCreateTableQuery
	default:
		return fmt.Errorf("unsupported database dialect: %v", dialect)
	}

	_, err := db.Exec(query)
	return err
}
