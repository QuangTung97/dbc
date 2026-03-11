package postgres_test

import (
	"embed"
	"testing"

	"github.com/QuangTung97/dbc"
	postgrescheck "github.com/QuangTung97/dbc/schemacheck/postgres"
	"tests"

	_ "github.com/go-sql-driver/mysql"
)

//go:embed migrations/*
var migrationsDir embed.FS

func newTestConfig() tests.TestConfig {
	return tests.TestConfig{
		MigrationsDir: migrationsDir,
		SubDir:        "migrations",
		Dialect:       dbc.DialectPostgres,

		DriverName: "mysql",
	}
}

func TestValidateSchemas(t *testing.T) {
	tests.RunTestValidateSchemas(
		t,
		newTestConfig(),
		postgrescheck.NewLoader,
		postgrescheck.DefaultMatchColumnType,
	)
}

func TestInsertAuthUser(t *testing.T) {
	tests.RunAllAuthUserTests(t, newTestConfig())
}
