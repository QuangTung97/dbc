package mysqlcheck

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type Timestamp time.Time

func TestDefaultMatchColumnType(t *testing.T) {
	assert.Equal(t, true, DefaultMatchColumnType(reflect.TypeOf(""), "varchar"))
	assert.Equal(t, true, DefaultMatchColumnType(reflect.TypeOf(""), "char"))
	assert.Equal(t, true, DefaultMatchColumnType(reflect.TypeOf(""), "tinytext"))
	assert.Equal(t, true, DefaultMatchColumnType(reflect.TypeOf(""), "text"))
	assert.Equal(t, true, DefaultMatchColumnType(reflect.TypeOf(""), "mediumtext"))
	assert.Equal(t, true, DefaultMatchColumnType(reflect.TypeOf(""), "longtext"))

	assert.Equal(t, false, DefaultMatchColumnType(reflect.TypeOf(""), "int"))

	intType := reflect.TypeOf(int64(0))
	assert.Equal(t, true, DefaultMatchColumnType(intType, "int"))
	assert.Equal(t, true, DefaultMatchColumnType(intType, "tinyint"))
	assert.Equal(t, true, DefaultMatchColumnType(intType, "smallint"))
	assert.Equal(t, true, DefaultMatchColumnType(intType, "mediumint"))
	assert.Equal(t, true, DefaultMatchColumnType(intType, "bigint"))

	boolType := reflect.TypeOf(true)
	assert.Equal(t, true, DefaultMatchColumnType(boolType, "boolean"))

	timeType := reflect.TypeOf(time.Time{})
	assert.Equal(t, true, DefaultMatchColumnType(timeType, "timestamp"))

	tsType := reflect.TypeOf(Timestamp{})
	assert.Equal(t, true, DefaultMatchColumnType(tsType, "timestamp"))
	assert.Equal(t, false, DefaultMatchColumnType(tsType, "varchar"))
}
