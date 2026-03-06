package dbc

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSchema_GetUniqueKeys(t *testing.T) {
	var schema SchemaInterface = tableTest05Schema
	assert.Equal(t, []UniqueKeyInfo{
		{Columns: []string{"role_id", "username"}},
	}, schema.GetUniqueKeys())
}
