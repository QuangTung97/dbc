package dbc

import (
	"reflect"
	"strings"
	"unsafe"
)

type CondBuilder[T any] struct {
	basePtr      unsafe.Pointer
	offsetDBName map[fieldOffsetType]string

	condList []string
	args     []any
}

func NewCondBuilder[T any]() (*CondBuilder[T], *T) {
	var emptyVal T
	tablePtr := &emptyVal

	offsetDBName := map[fieldOffsetType]string{}
	tableType := reflect.TypeOf(emptyVal)
	for index := range tableType.NumField() {
		field := tableType.Field(index)
		offset := fieldOffsetType(field.Offset)
		offsetDBName[offset] = field.Tag.Get(DBTag)
	}

	return &CondBuilder[T]{
		basePtr:      unsafe.Pointer(tablePtr),
		offsetDBName: offsetDBName,
	}, tablePtr
}

type CondBuilderFunc[T any] = func(b *CondBuilder[T], table *T)

func (c *CondBuilder[T]) GetWhereCond() (string, []any) {
	return strings.Join(c.condList, " AND "), c.args
}

func CondEqual[T any, F any](c *CondBuilder[T], field *F, value F) {
	offset := unsafePointerSub(unsafe.Pointer(field), c.basePtr)
	dbName := c.offsetDBName[offset]
	c.condList = append(c.condList, c.quoteIdent(dbName)+" = ?")
	c.args = append(c.args, value)
}

func (c *CondBuilder[T]) quoteIdent(name string) string {
	// TODO refactor
	return "`" + name + "`"
}
