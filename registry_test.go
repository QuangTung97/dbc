package dbc

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetAllSchemas(t *testing.T) {
	clearAllSchemas()

	schemaList := GetAllSchemas()
	assert.Equal(t, 0, len(schemaList))

	RegisterSchema(func(s *Schema[tableTest03], table *tableTest03) {
		SchemaIDInt64(s, &table.ID)
		SchemaConst(s, &table.RoleID)

		SchemaEditable(s, &table.Username)
		SchemaEditable(s, &table.Age)

		SchemaIgnore(s, &table.CreatedAt)
		SchemaIgnore(s, &table.UpdatedAt)
	})

	RegisterSchema(func(s *Schema[tableTest04], table *tableTest04) {
		SchemaPrimaryKey(s, &table.RoleID)

		SchemaEditable(s, &table.Username)
		SchemaEditable(s, &table.Age)
		SchemaEditable(s, &table.Desc)

		SchemaIgnore(s, &table.CreatedAt)
	})

	// get again
	schemaList = GetAllSchemas()
	assert.Equal(t, 2, len(schemaList))
	assert.Equal(t, "dbc.tableTest03", schemaList[0].GetTypeString())
	assert.Equal(t, "dbc.tableTest04", schemaList[1].GetTypeString())
}

func TestGetAllSchemas__Without_Registering(t *testing.T) {
	clearAllSchemas()

	schemaList := GetAllSchemas()
	assert.Equal(t, 0, len(schemaList))

	RegisterSchema(func(s *Schema[tableTest03], table *tableTest03) {
		SchemaIDInt64(s, &table.ID)
		SchemaConst(s, &table.RoleID)

		SchemaEditable(s, &table.Username)
		SchemaEditable(s, &table.Age)

		SchemaIgnore(s, &table.CreatedAt)
		SchemaIgnore(s, &table.UpdatedAt)
	}, WithSchemaNoRegistering())

	RegisterSchema(func(s *Schema[tableTest04], table *tableTest04) {
		SchemaPrimaryKey(s, &table.RoleID)

		SchemaEditable(s, &table.Username)
		SchemaEditable(s, &table.Age)
		SchemaEditable(s, &table.Desc)

		SchemaIgnore(s, &table.CreatedAt)
	})

	// get again
	schemaList = GetAllSchemas()
	assert.Equal(t, 1, len(schemaList))
	assert.Equal(t, "dbc.tableTest04", schemaList[0].GetTypeString())
}
