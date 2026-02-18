package dbc

import (
	"strings"
	"unsafe"

	"github.com/QuangTung97/dbc/null"
)

type CondBuilder[T TableNamer] struct {
	basePtr unsafe.Pointer
	schema  *Schema[T]
	dialect DatabaseDialect

	condList []string
	args     []any
}

func NewCondBuilder[T TableNamer](
	schema *Schema[T],
	dialect DatabaseDialect,
) (*CondBuilder[T], *T) {
	var emptyVal T
	tablePtr := &emptyVal

	return &CondBuilder[T]{
		basePtr: unsafe.Pointer(tablePtr),
		schema:  schema,

		dialect: dialect,
	}, tablePtr
}

type CondBuilderFunc[T TableNamer] = func(b *CondBuilder[T], table *T)

func (c *CondBuilder[T]) GetWhereCond() (string, []any) {
	return strings.Join(c.condList, " AND "), c.args
}

func CondEqual[T TableNamer, F any](c *CondBuilder[T], field *F, value F) {
	offset := unsafePointerSub(unsafe.Pointer(field), c.basePtr)
	dbName := c.getColumnName(offset)
	c.condList = append(c.condList, dbName+" = ?")
	c.args = append(c.args, value)
}

func CondColumnExpr[T TableNamer, F any](
	c *CondBuilder[T], field *F, fn func(col string) string, args ...any,
) {
	offset := unsafePointerSub(unsafe.Pointer(field), c.basePtr)
	dbName := c.getColumnName(offset)
	c.condList = append(c.condList, fn(dbName))
	c.args = append(c.args, args...)
}

func CondIsNull[T TableNamer, F any](c *CondBuilder[T], field *null.Null[F]) {
	offset := unsafePointerSub(unsafe.Pointer(field), c.basePtr)
	dbName := c.getColumnName(offset)
	c.condList = append(c.condList, dbName+" IS NULL")
}

func CondIsNotNull[T TableNamer, F any](c *CondBuilder[T], field *null.Null[F]) {
	offset := unsafePointerSub(unsafe.Pointer(field), c.basePtr)
	dbName := c.getColumnName(offset)
	c.condList = append(c.condList, dbName+" IS NOT NULL")
}

func (c *CondBuilder[T]) IsEmpty() bool {
	return len(c.condList) == 0
}

func (c *CondBuilder[T]) getColumnName(offset fieldOffsetType) string {
	return c.quoteIdent(c.schema.getFieldInfo(offset).dbName)
}

func (c *CondBuilder[T]) quoteIdent(name string) string {
	return quoteIdentWithDialect(c.dialect, name)
}
