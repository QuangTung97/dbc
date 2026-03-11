package postgrescheck

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDefaultMatchColumnType(t *testing.T) {
	assert.Equal(t, true, DefaultMatchColumnType(reflect.TypeOf(""), "varchar"))
	assert.Equal(t, false, DefaultMatchColumnType(reflect.TypeOf(""), "int4"))
	assert.Equal(t, true, DefaultMatchColumnType(reflect.TypeOf(""), "char"))
	assert.Equal(t, true, DefaultMatchColumnType(reflect.TypeOf(""), "text"))

	assert.Equal(t, true, DefaultMatchColumnType(reflect.TypeOf(int64(0)), "int4"))

	assert.Equal(t, false, DefaultMatchColumnType(reflect.TypeOf(int64(0)), "bool"))
	assert.Equal(t, true, DefaultMatchColumnType(reflect.TypeOf(true), "bool"))

	assert.Equal(t, true, DefaultMatchColumnType(reflect.TypeOf(time.Time{}), "timestamptz"))
}
