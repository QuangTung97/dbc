package mysql_test

import (
	"embed"
	"testing"

	"github.com/QuangTung97/dbc"
	mysqlcheck "github.com/QuangTung97/dbc/schemacheck/mysql"
	"tests"

	_ "github.com/go-sql-driver/mysql"
)

//go:embed migrations/*
var migrationsDir embed.FS

func newTestConfig() tests.TestConfig {
	return tests.TestConfig{
		MigrationsDir: migrationsDir,
		SubDir:        "migrations",
		Dialect:       dbc.DialectMySQL,
	}
}

func TestValidateSchemas(t *testing.T) {
	tests.RunTestValidateSchemas(
		t,
		newTestConfig(),
		mysqlcheck.NewLoader,
		mysqlcheck.DefaultMatchColumnType,
	)
}

func TestInsertAuthUser(t *testing.T) {
	tests.RunAllAuthUserTests(t, newTestConfig())
}
