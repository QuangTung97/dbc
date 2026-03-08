package tests

import (
	"context"
	"embed"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"

	"github.com/QuangTung97/dbc"
	"github.com/QuangTung97/dbc/schemacheck"
)

type TestCase struct {
	DB  *sqlx.DB
	Ctx context.Context

	UserExec *dbc.Executor[AuthUser]
}

type TestConfig struct {
	MigrationsDir embed.FS
	SubDir        string
	Dialect       dbc.DatabaseDialect
}

func NewTestCase(_ *testing.T, conf TestConfig) *TestCase {
	tc := &TestCase{}
	tc.DB = GetNewDB(conf.MigrationsDir, conf.SubDir, conf.Dialect)

	provider := dbc.NewProvider(tc.DB)
	tc.Ctx = provider.Autocommit(context.Background())

	// truncate all normal table
	for _, schema := range GetAllSchemas() {
		tc.DB.MustExec(`TRUNCATE TABLE ` + schema.GetTableName())
	}

	// init executors
	tc.UserExec, _ = dbc.NewExecutor(conf.Dialect, AuthUserSchema)

	return tc
}

func RunTestValidateSchemas(
	t *testing.T, conf TestConfig,
	newLoaderFunc func(db *sqlx.DB, dbname string) schemacheck.TableLoader,
	columnCheckFunc schemacheck.ColumnTypeMatchFunc,
) {
	tc := NewTestCase(t, conf)

	val := schemacheck.NewValidator(
		newLoaderFunc(tc.DB, "testdb"),
		columnCheckFunc,
		schemacheck.WithValidatePrimaryKey(true),
		schemacheck.WithValidateUniqueKeys(true),
		schemacheck.WithValidateIndexes(true),
	)

	err := val.ValidateSchemas(context.Background(), GetAllSchemasIncludeMigration())
	assert.Equal(t, nil, err)
}
