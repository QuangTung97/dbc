package mysql_test

import (
	"embed"
	"sync"
	"testing"

	"github.com/jmoiron/sqlx"

	"github.com/QuangTung97/dbc"
	"github.com/QuangTung97/dbc/dbmigrate"
)

//go:embed migrations/*
var migrationsDir embed.FS

type testCase struct {
	db *sqlx.DB
}

var getNewDB = sync.OnceValue(func() *sqlx.DB {
	db := sqlx.MustConnect("mysql", "") // TODO
	dbmigrate.MigrateUp(db, migrationsDir, "migrations", dbc.DialectMySQL)
	return db
})

func newTestCase(_ *testing.T) *testCase {
	tc := &testCase{}
	tc.db = getNewDB()

	// TODO drop all tables

	return tc
}

func TestValidateSchemas(t *testing.T) {
	newTestCase(t)
}
