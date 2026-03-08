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

func getAllSchemas() []dbc.SchemaInterface {
	currentPkg := getCurrentPackage()
	var result []dbc.SchemaInterface
	for _, schema := range dbc.GetAllSchemas() {
		if schema.GetPackagePath() != currentPkg {
			continue
		}
		result = append(result, schema)
	}
	return result
}

func getAllSchemasIncludeMigration() []dbc.SchemaInterface {
	result := getAllSchemas()
	result = append(result, dbmigrate.SchemaMigrationSchema) // add migration schema
	return result
}

var getNewDB = sync.OnceValue(func() *sqlx.DB {
	db := sqlx.MustConnect(
		"mysql",
		"root:pass@tcp(localhost:3306)/testdb?parseTime=true&multiStatements=true",
	)

	for _, schema := range getAllSchemasIncludeMigration() {
		db.MustExec(`DROP TABLE IF EXISTS ` + schema.GetTableName())
	}

	dbmigrate.MigrateUp(db, migrationsDir, "migrations", dbc.DialectMySQL)
	return db
})

type testCase struct {
	db  *sqlx.DB
	ctx context.Context

	userExec *dbc.Executor[AuthUser]
}

func newTestCase(_ *testing.T) *testCase {
	tc := &testCase{}
	tc.db = getNewDB()

	provider := dbc.NewProvider(tc.db)
	tc.ctx = provider.Autocommit(context.Background())

	// truncate all normal table
	for _, schema := range getAllSchemas() {
		tc.db.MustExec(`TRUNCATE TABLE ` + schema.GetTableName())
	}

	// init executors
	tc.userExec, _ = dbc.NewExecutor(dbc.DialectMySQL, AuthUserSchema)

	return tc
}

func TestValidateSchemas(t *testing.T) {
	tc := newTestCase(t)

	val := schemacheck.NewValidator(
		mysqlcheck.NewLoader(tc.db, "testdb"),
		mysqlcheck.DefaultMatchColumnType,
		schemacheck.WithValidatePrimaryKey(true),
		schemacheck.WithValidateUniqueKey(true),
	)

	err := val.ValidateSchemas(context.Background(), getAllSchemasIncludeMigration())
	assert.Equal(t, nil, err)
}
