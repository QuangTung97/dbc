package dbc

import (
	"time"

	"github.com/QuangTung97/dbc/null"
)

// ------------------------------

type tableTest01 struct {
}

func (tableTest01) TableName() string {
	return "table_test01"
}

// ------------------------------

type tableTest02 struct {
	ID       int64 `db:"id"`
	Username string
}

func (tableTest02) TableName() string {
	return "table_test02"
}

// ------------------------------

type testRoleID int64

type tableTest03 struct {
	ID        int64      `db:"id"`
	RoleID    testRoleID `db:"role_id"`
	Username  string     `db:"username"`
	Age       int        `db:"age"`
	CreatedAt time.Time  `db:"created_at"`
	UpdatedAt time.Time  `db:"updated_at"`
}

func (tableTest03) TableName() string {
	return "table_test03"
}

var tableTest03Schema = RegisterSchema(func(s *Schema[tableTest03], table *tableTest03) {
	SchemaIDAutoInc(s, &table.ID)

	SchemaConst(s, &table.RoleID)
	SchemaConst(s, &table.Username)
	SchemaEditable(s, &table.Age)

	SchemaIgnore(s, &table.CreatedAt)
	SchemaIgnore(s, &table.UpdatedAt)
})

// ------------------------------

type tableTest04 struct {
	RoleID    testRoleID `db:"role_id"`
	Username  string     `db:"username"`
	Age       int        `db:"age"`
	Desc      string     `db:"desc"`
	CreatedAt time.Time  `db:"created_at"`
}

func (tableTest04) TableName() string {
	return "table_test04"
}

// ------------------------------

type tableTest05 struct {
	ID        int64                 `db:"id"`
	RoleID    null.Null[testRoleID] `db:"role_id"`
	Username  string                `db:"username"`
	Age       int                   `db:"age"`
	CreatedAt time.Time             `db:"created_at"`
}

func (tableTest05) TableName() string {
	return "table_test05"
}

var tableTest05Schema = RegisterSchema(func(s *Schema[tableTest05], table *tableTest05) {
	SchemaIDAutoInc(s, &table.ID)

	SchemaConst(s, &table.RoleID)
	SchemaConst(s, &table.Username)
	SchemaEditable(s, &table.Age)

	SchemaIgnore(s, &table.CreatedAt)
})

// ------------------------------

type commonTimestamps struct {
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`
}

type subSchemaStruct01 struct {
	Username string `db:"username"`
	Age      int    `db:"age"`
	commonTimestamps
}

func commonTimestampsSchema[T TableNamer](s *Schema[T], common *commonTimestamps) {
	SchemaIgnore(s, &common.CreatedAt)
	SchemaIgnore(s, &common.UpdatedAt)
}

type tableTest06 struct {
	ID     int64                 `db:"id"`
	RoleID null.Null[testRoleID] `db:"role_id"`
	subSchemaStruct01
}

func (tableTest06) TableName() string {
	return "table_test06"
}

var tableTest06Schema = RegisterSchema(func(s *Schema[tableTest06], table *tableTest06) {
	SchemaIDAutoInc(s, &table.ID)

	SchemaConst(s, &table.RoleID)
	SchemaConst(s, &table.Username)
	SchemaEditable(s, &table.Age)

	commonTimestampsSchema(s, &table.commonTimestamps)
})
