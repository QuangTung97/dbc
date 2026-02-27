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
	assert.PanicsWithValue(
		t,
		"missing 'id' column or primary key definition of struct 'dbc.tableTest01'",
		func() {
			RegisterSchema(func(s *Schema[tableTest01], table *tableTest01) {
			})
		},
	)
}

func TestRegisterSchema_Not_Found_Struct_Tag_DB(t *testing.T) {
	newTestSchema(t)
	assert.PanicsWithValue(t, "missing struct tag of field 'Username' of struct 'dbc.tableTest02'", func() {
		RegisterSchema(func(s *Schema[tableTest02], table *tableTest02) {
			SchemaIDInt64(s, &table.ID)
		})
	})
}

func TestRegisterSchema_Normal(t *testing.T) {
	newTestSchema(t)
	s := RegisterSchema(func(s *Schema[tableTest03], table *tableTest03) {
		SchemaIDInt64(s, &table.ID)
		SchemaConst(s, &table.RoleID)

		SchemaEditable(s, &table.Username)
		SchemaEditable(s, &table.Age)

		SchemaIgnore(s, &table.CreatedAt)
		SchemaIgnore(s, &table.UpdatedAt)
	})

	// get columns
	cols := s.GetColumnNames(func(g *ColumnGetter[tableTest03], table *tableTest03) {
		ReturnColumn(g, &table.Age)
		ReturnColumn(g, &table.Username)
	})
	assert.Equal(t, []string{"age", "username"}, cols)

	// schema interface func
	var schema SchemaInterface = s
	assert.Equal(t, "tableTest03", schema.GetTypeName())
	assert.Equal(t, "dbc.tableTest03", schema.GetTypeString())
	assert.Equal(t, "table_test03", schema.GetTableName())
	assert.Equal(t, "github.com/QuangTung97/dbc", schema.GetPackagePath())

	var infoList []FieldTraverseInfo
	for info := range schema.TraverseFields() {
		infoList = append(infoList, info)
	}
	assert.Equal(t, 6, len(infoList))

	assert.Equal(t, "ID", infoList[0].Name)
	assert.Equal(t, "id", infoList[0].DBName)
	assert.Equal(t, "int64", infoList[0].Type.String())

	assert.Equal(t, "RoleID", infoList[1].Name)
	assert.Equal(t, "role_id", infoList[1].DBName)
}

func TestRegisterSchema_Missing_Col_Spec(t *testing.T) {
	newTestSchema(t)
	assert.PanicsWithValue(t, "missing column spec of field 'Username' of struct 'dbc.tableTest03'", func() {
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

func TestRegisterSchema_Composite_Primary_Key__Normal(t *testing.T) {
	newTestSchema(t)
	RegisterSchema(func(s *Schema[tableTest04], table *tableTest04) {
		SchemaPrimaryKey(s, &table.RoleID)
		SchemaPrimaryKey(s, &table.Username)

		SchemaEditable(s, &table.Age)
		SchemaEditable(s, &table.Desc)

		SchemaIgnore(s, &table.CreatedAt)
	})
}

func TestRegisterSchema_Duplicated_Definition(t *testing.T) {
	newTestSchema(t)
	assert.PanicsWithValue(t, "field 'Age' of struct 'dbc.tableTest04' has already been specified", func() {
		RegisterSchema(func(s *Schema[tableTest04], table *tableTest04) {
			SchemaPrimaryKey(s, &table.RoleID)
			SchemaPrimaryKey(s, &table.Username)

			SchemaEditable(s, &table.Age)
			SchemaConst(s, &table.Age) // failed
			SchemaEditable(s, &table.Desc)

			SchemaIgnore(s, &table.CreatedAt)
		})
	})
}

func TestRegisterSchema_Normal__Invalid_Address(t *testing.T) {
	newTestSchema(t)
	assert.PanicsWithValue(t, "invalid field address value", func() {
		RegisterSchema(func(s *Schema[tableTest03], table *tableTest03) {
			SchemaIDInt64(s, &table.ID)
			SchemaConst(s, &table.RoleID)

			SchemaEditable(s, &table.Username)
			SchemaEditable(s, &table.Age)

			// invalid
			ptr := new(int)
			SchemaEditable(s, ptr)

			SchemaIgnore(s, &table.CreatedAt)
			SchemaIgnore(s, &table.UpdatedAt)
		})
	})
}

func TestRegisterSchema_Normal__Use_Definition_Function_Outside(t *testing.T) {
	newTestSchema(t)
	assert.PanicsWithValue(t, "function is not allowed to run outside of schema definition callback", func() {
		s := RegisterSchema(func(s *Schema[tableTest03], table *tableTest03) {
			SchemaIDInt64(s, &table.ID)
			SchemaConst(s, &table.RoleID)

			SchemaEditable(s, &table.Username)
			SchemaEditable(s, &table.Age)

			SchemaIgnore(s, &table.CreatedAt)
			SchemaIgnore(s, &table.UpdatedAt)
		})
		SchemaEditable(s, new(int))
	})
}

func TestRegisterSchema__Duplicated_Validate_Optional(t *testing.T) {
	newTestSchema(t)
	assert.PanicsWithValue(t, "field 'Username' of struct 'dbc.tableTest03' has already been specified", func() {
		RegisterSchema(func(s *Schema[tableTest03], table *tableTest03) {
			SchemaIDInt64(s, &table.ID)
			SchemaConst(s, &table.RoleID)

			SchemaEditable(s, &table.Username)
			SchemaEditable(s, &table.Age)

			ValidateOptional(s, &table.Username)
			ValidateOptional(s, &table.Age)
			ValidateOptional(s, &table.Username)

			SchemaIgnore(s, &table.CreatedAt)
			SchemaIgnore(s, &table.UpdatedAt)
		})
	})
}

func TestRegisterSchema__Optional_Of_ID_Column__Failed(t *testing.T) {
	newTestSchema(t)
	assert.PanicsWithValue(
		t,
		"can not config optional for primary column 'ID' of struct 'dbc.tableTest03'",
		func() {
			RegisterSchema(func(s *Schema[tableTest03], table *tableTest03) {
				SchemaIDInt64(s, &table.ID)
				SchemaConst(s, &table.RoleID)

				SchemaEditable(s, &table.Username)
				SchemaEditable(s, &table.Age)

				ValidateOptional(s, &table.ID)
				ValidateOptional(s, &table.Username)
				ValidateOptional(s, &table.Age)

				SchemaIgnore(s, &table.CreatedAt)
				SchemaIgnore(s, &table.UpdatedAt)
			})
		},
	)
}

func TestRegisterSchema__Nested__Normal(t *testing.T) {
	newTestSchema(t)

	// get columns
	cols := tableTest06Schema.GetColumnNames(func(g *ColumnGetter[tableTest06], table *tableTest06) {
		ReturnColumn(g, &table.Age)
		ReturnColumn(g, &table.Username)
		ReturnColumn(g, &table.CreatedAt)
		ReturnColumn(g, &table.UpdatedAt)
	})
	assert.Equal(t, []string{"age", "username", "created_at", "updated_at"}, cols)
}

func TestRegisterSchema__Duplicated_DB_Name__Failed(t *testing.T) {
	newTestSchema(t)
	assert.PanicsWithValue(
		t,
		"duplicated column name 'role_id' in struct 'dbc.tableTest07'",
		func() {
			RegisterSchema(func(s *Schema[tableTest07], table *tableTest07) {
				SchemaIDInt64(s, &table.ID)
				SchemaConst(s, &table.RoleID)
				SchemaEditable(s, &table.Username)
			})
		},
	)
}
