package mysql_test

import (
	"context"
	"embed"
	"sync"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"

	"github.com/QuangTung97/dbc"
	"github.com/QuangTung97/dbc/dbmigrate"
	"github.com/QuangTung97/dbc/schemacheck"
	mysqlcheck "github.com/QuangTung97/dbc/schemacheck/mysql"

	_ "github.com/go-sql-driver/mysql"
)

//go:embed migrations/*
var migrationsDir embed.FS

type testCase struct {
	db *sqlx.DB
}

var getNewDB = sync.OnceValue(func() *sqlx.DB {
	db := sqlx.MustConnect(
		"mysql",
		"root:pass@tcp(localhost:3306)/testdb?parseTime=true&multiStatements=true",
	)
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
	tc := newTestCase(t)

	val := schemacheck.NewValidator(
		mysqlcheck.NewLoader(tc.db),
		mysqlcheck.DefaultMatchColumnType,
	)

	currentPkg := getCurrentPackage()

	var schemaList []dbc.SchemaInterface
	for _, schema := range dbc.GetAllSchemas() {
		if schema.GetPackagePath() != currentPkg {
			continue
		}
		schemaList = append(schemaList, schema)
	}
	schemaList = append(schemaList, dbmigrate.SchemaMigrationSchema) // add migration schema

	err := val.ValidateSchemas(context.Background(), schemaList)
	assert.Equal(t, nil, err)
}
