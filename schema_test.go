package dbc

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type tableTest01 struct {
}

func (tableTest01) TableName() string {
	return "example_table"
}

type tableTest02 struct {
	ID       int64 `db:"id"`
	Username string
}

func (tableTest02) TableName() string {
	return "example_table"
}

// ===================================================
// Schema Test Object
// ===================================================

type schemaTest struct {
}

func newTestSchema(t *testing.T) *schemaTest {
	s := &schemaTest{}
	return s
}

func TestRegisterSchema_Missing_ID(t *testing.T) {
	newTestSchema(t)
	assert.PanicsWithValue(t, "missing 'id' column definition in type 'dbc.tableTest01'", func() {
		RegisterSchema(func(s *Schema[tableTest01], table *tableTest01) {
		})
	})
}

func TestRegisterSchema_Not_Found_Struct_Tag_DB(t *testing.T) {
	newTestSchema(t)
	assert.PanicsWithValue(t, "missing struct tag of field 'Username' in type 'dbc.tableTest02'", func() {
		RegisterSchema(func(s *Schema[tableTest02], table *tableTest02) {
			SchemaIDInt64(s, &table.ID)
		})
	})
}
