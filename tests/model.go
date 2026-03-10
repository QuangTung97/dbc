package tests

import (
	"embed"
	"reflect"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/QuangTung97/dbc"
	"github.com/QuangTung97/dbc/dbmigrate"
	"github.com/QuangTung97/dbc/null"
)

type UserID int64

type Timestamps struct {
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`
}

func TimestampsSchema[T dbc.TableNamer](s *dbc.Schema[T], ts *Timestamps) {
	dbc.SchemaIgnore(s, &ts.CreatedAt)
	dbc.SchemaIgnore(s, &ts.UpdatedAt)
}

type AuthUser struct {
	ID       UserID `db:"id"`
	Username string `db:"username"`
	Age      int32  `db:"age"`

	Timestamps
}

func (AuthUser) TableName() string {
	return "auth_user"
}

var AuthUserSchema = dbc.RegisterSchema(func(s *dbc.Schema[AuthUser], table *AuthUser) {
	dbc.SchemaIDAutoInc(s, &table.ID)
	dbc.SchemaConst(s, &table.Username)
	dbc.SchemaEditable(s, &table.Age)

	TimestampsSchema(s, &table.Timestamps)

	dbc.SchemaUniqueKey(s, func(g *dbc.ColumnGetter[AuthUser], table *AuthUser) {
		dbc.ReturnColumn(g, &table.Username)
	})

	dbc.SchemaIndex(s, "idx_age_username", func(g *dbc.ColumnGetter[AuthUser], table *AuthUser) {
		dbc.ReturnColumn(g, &table.Age)
		dbc.ReturnColumn(g, &table.Username)
	})
})

// ---------------------------------------------------------------------------

type UserPermission struct {
	UserID UserID            `db:"user_id"`
	Perm   string            `db:"perm"`
	Desc   null.Null[string] `db:"perm_desc"`

	Timestamps
}

func (u UserPermission) TableName() string {
	return "user_permission"
}

var UserPermissionSchema = dbc.RegisterSchema(func(s *dbc.Schema[UserPermission], table *UserPermission) {
	dbc.SchemaPrimaryKey(s, &table.UserID)
	dbc.SchemaPrimaryKey(s, &table.Perm)

	dbc.SchemaEditable(s, &table.Desc)
	dbc.ValidateOptional(s, &table.Desc)

	TimestampsSchema(s, &table.Timestamps)
})

// ---------------------------------------------------------------------------

func GetAllSchemas() []dbc.SchemaInterface {
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

func GetAllSchemasIncludeMigration() []dbc.SchemaInterface {
	result := GetAllSchemas()
	result = append(result, dbmigrate.SchemaMigrationSchema) // add migration schema
	return result
}

type simpleType int

func getCurrentPackage() string {
	return reflect.TypeOf(simpleType(0)).PkgPath()
}

// ---------------------------------------------------------------------------

var globalOnce sync.Once
var globalDB *sqlx.DB

func GetNewDB(migrationsDir embed.FS, subDir string, dialect dbc.DatabaseDialect) *sqlx.DB {
	globalOnce.Do(func() {
		db := sqlx.MustConnect(
			"mysql",
			"root:pass@tcp(localhost:3306)/testdb?parseTime=true&multiStatements=true",
		)

		for _, schema := range GetAllSchemasIncludeMigration() {
			db.MustExec(`DROP TABLE IF EXISTS ` + schema.GetTableName())
		}

		dbmigrate.MigrateUp(db, migrationsDir, subDir, dialect)

		globalDB = db
	})
	return globalDB
}
