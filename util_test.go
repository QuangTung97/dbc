package dbc

import (
	"reflect"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func joinString(values ...string) string {
	return strings.Join(values, " ")
}

func TestTraverseFieldsOfType(t *testing.T) {
	var empty tableTest06

	var names []string
	var indexList [][]int
	var offsetList []fieldOffsetType

	for info := range traverseFieldsOfType(reflect.TypeOf(empty)) {
		names = append(names, info.fieldName)
		indexList = append(indexList, info.indices)
		offsetList = append(offsetList, info.offset)
	}

	assert.Equal(t, []string{
		"ID", "RoleID",
		"Username", "Age",
		"CreatedAt", "UpdatedAt",
	}, names)

	assert.Equal(t, [][]int{
		{0},
		{1},
		{2, 0},
		{2, 1},
		{2, 2, 0},
		{2, 2, 1},
	}, indexList)

	assert.Equal(t, []fieldOffsetType{
		0,
		8,               // +int64
		8 + 16,          // +null.Null[int64]
		8 + 16 + 16,     // +string
		8 + 16 + 16 + 8, // +int
		8 + 16 + 16 + 8 + fieldOffsetType(unsafe.Sizeof(time.Time{})), // +time.Time
	}, offsetList)
}

func TestTraverseFieldsOfType__Exit_Early(t *testing.T) {
	var empty tableTest06

	var names []string
	var indexList [][]int

	for info := range traverseFieldsOfType(reflect.TypeOf(empty)) {
		names = append(names, info.fieldName)
		indexList = append(indexList, info.indices)
		if len(names) >= 3 {
			break
		}
	}

	assert.Equal(t, []string{
		"ID", "RoleID",
		"Username",
	}, names)
	assert.Equal(t, [][]int{
		{0},
		{1},
		{2, 0},
	}, indexList)
}

func TestCommonBuilder_quoteIdent(t *testing.T) {
	var empty tableTest03
	table := &empty
	b := commonBuilder[tableTest03]{
		basePtr: unsafe.Pointer(table),
		schema:  tableTest03Schema,
		dialect: DialectMySQL,
	}

	assert.Equal(t, "`username`", b.quoteIdent("username"))
	assert.Equal(t, "`information_schema`.`columns`", b.quoteIdent("information_schema.columns"))
}
