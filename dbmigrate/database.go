package dbmigrate

import (
	"fmt"

	"github.com/jmoiron/sqlx"

	"github.com/QuangTung97/dbc"
)

type DatabaseType int

const (
	DatabaseSQLite3 DatabaseType = iota + 1
	DatabaseMySQL
)

func (dt DatabaseType) ToDialect() dbc.DatabaseDialect {
	switch dt {
	case DatabaseMySQL:
		return dbc.DialectMySQL
	case DatabaseSQLite3:
		return dbc.DialectPostgres
	default:
		return -1
	}
}

const SQLiteCreateTableQuery = `
CREATE TABLE IF NOT EXISTS schema_migration (
    id INTEGER NOT NULL PRIMARY KEY,
    version INTEGER NOT NULL,
    filename TEXT NOT NULL,
	is_dirty INTEGER NOT NULL
) STRICT;
`

const MySQLCreateTableQuery = `
CREATE TABLE IF NOT EXISTS schema_migration (
	id INTEGER NOT NULL PRIMARY KEY,
    version INTEGER NOT NULL,
    filename VARCHAR(1024) NOT NULL,
	is_dirty BOOLEAN NOT NULL
);
`

func createTableFunc(db *sqlx.DB, dbType DatabaseType) error {
	var query string
	switch dbType {
	case DatabaseSQLite3:
		query = SQLiteCreateTableQuery
	case DatabaseMySQL:
		query = MySQLCreateTableQuery
	default:
		return fmt.Errorf("unsupported database type: %v", dbType)
	}

	_, err := db.Exec(query)
	return err
}
