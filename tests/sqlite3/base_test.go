package sqlite3_test

import (
	"embed"
	"fmt"
	"testing"

	"github.com/QuangTung97/dbc"
	sqlitecheck "github.com/QuangTung97/dbc/schemacheck/sqlite"
	"tests"

	_ "github.com/mattn/go-sqlite3"
)

//go:embed migrations/*
var migrationsDir embed.FS

func newTestConfig() tests.TestConfig {
	dbPath := "/tmp/dbc_test_sqlite.db"
	return tests.TestConfig{
		MigrationsDir: migrationsDir,
		SubDir:        "migrations",
		Dialect:       dbc.DialectSQLite3,

		DriverName: "sqlite3",
		DSN:        fmt.Sprintf("file://%s?_loc=UTC", dbPath),

		SQLitePath: dbPath,
	}
}

func TestValidateSchemas(t *testing.T) {
	tests.RunTestValidateSchemas(
		t,
		newTestConfig(),
		sqlitecheck.NewLoader,
		sqlitecheck.DefaultMatchColumnType,
	)
}

func TestInsertAuthUser(t *testing.T) {
	tests.RunAllAuthUserTests(t, newTestConfig())
}
