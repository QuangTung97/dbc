package dbc

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type schemaTest struct {
}

func newTestSchema(_ *testing.T) *schemaTest {
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

func TestRegisterSchema_Normal(t *testing.T) {
	newTestSchema(t)
	RegisterSchema(func(s *Schema[tableTest03], table *tableTest03) {
		SchemaIDInt64(s, &table.ID)
		SchemaConst(s, &table.RoleID)

		SchemaEditable(s, &table.Username)
		SchemaEditable(s, &table.Age)

		SchemaIgnore(s, &table.CreatedAt)
		SchemaIgnore(s, &table.UpdatedAt)
	})
}

func TestRegisterSchema_Missing_Col_Spec(t *testing.T) {
	newTestSchema(t)
	assert.PanicsWithValue(t, "missing column spec of field 'Username' in type 'dbc.tableTest03'", func() {
		RegisterSchema(func(s *Schema[tableTest03], table *tableTest03) {
			SchemaIDInt64(s, &table.ID)
			SchemaConst(s, &table.RoleID)

			// missing username
			SchemaEditable(s, &table.Age)

			SchemaIgnore(s, &table.CreatedAt)
			SchemaIgnore(s, &table.UpdatedAt)
		})
	})
}
