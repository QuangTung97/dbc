package postgrescheck

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDefaultMatchColumnType(t *testing.T) {
	assert.Equal(t, true, DefaultMatchColumnType(reflect.TypeOf(""), "varchar"))
	assert.Equal(t, false, DefaultMatchColumnType(reflect.TypeOf(""), "int"))
	assert.Equal(t, true, DefaultMatchColumnType(reflect.TypeOf(""), "char"))
	assert.Equal(t, true, DefaultMatchColumnType(reflect.TypeOf(""), "text"))

	assert.Equal(t, true, DefaultMatchColumnType(reflect.TypeOf(int64(0)), "int"))
	assert.Equal(t, true, DefaultMatchColumnType(reflect.TypeOf(int64(0)), "integer"))
	assert.Equal(t, true, DefaultMatchColumnType(reflect.TypeOf(int64(0)), "smallint"))
	assert.Equal(t, true, DefaultMatchColumnType(reflect.TypeOf(int64(0)), "bigint"))

	assert.Equal(t, false, DefaultMatchColumnType(reflect.TypeOf(int64(0)), "boolean"))
	assert.Equal(t, true, DefaultMatchColumnType(reflect.TypeOf(true), "boolean"))
}
