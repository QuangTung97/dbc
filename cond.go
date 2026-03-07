package dbc

import (
	"strings"
	"unsafe"

	"github.com/QuangTung97/dbc/null"
)

type CondBuilder[T TableNamer] struct {
	common commonBuilder[T]

	condList []string
	args     []any

	withOrderBy null.Null[string]
	withLimit   null.Null[int]
}

func NewCondBuilder[T TableNamer](
	schema *Schema[T],
	dialect DatabaseDialect,
) (*CondBuilder[T], *T) {
	var emptyVal T
	tablePtr := &emptyVal

	return &CondBuilder[T]{
		common: commonBuilder[T]{
			basePtr: unsafe.Pointer(tablePtr),
			schema:  schema,
			dialect: dialect,
		},
	}, tablePtr
}

type CondBuilderFunc[T TableNamer] = func(b *CondBuilder[T], table *T)

func (c *CondBuilder[T]) GetWhereCond() (string, []any) {
	var buf strings.Builder
	buf.WriteString(strings.Join(c.condList, " AND "))

	args := c.args

	if c.withOrderBy.Valid {
		buf.WriteString(" ORDER BY ")
		buf.WriteString(c.withOrderBy.Data)
	}

	if c.withLimit.Valid {
		buf.WriteString(" LIMIT ?")
		args = append(args, c.withLimit.Data)
	}

	return buf.String(), args
}

func (c *CondBuilder[T]) IsEmptyWhere() bool {
	return len(c.condList) == 0
}

// --------------------------------------------------------------------------------------

func CondEqual[T TableNamer, F any](c *CondBuilder[T], field *F, value F) {
	condColumnUnsafeExpr(c, field, func(col string) string {
		return col + " = ?"
	}, value)
}

func CondNotEqual[T TableNamer, F any](c *CondBuilder[T], field *F, value F) {
	condColumnUnsafeExpr(c, field, func(col string) string {
		return col + " != ?"
	}, value)
}

func CondGreater[T TableNamer, F any](c *CondBuilder[T], field *F, value F) {
	condColumnUnsafeExpr(c, field, func(col string) string {
		return col + " > ?"
	}, value)
}

func CondGreaterOrEqual[T TableNamer, F any](c *CondBuilder[T], field *F, value F) {
	condColumnUnsafeExpr(c, field, func(col string) string {
		return col + " >= ?"
	}, value)
}

func CondLess[T TableNamer, F any](c *CondBuilder[T], field *F, value F) {
	condColumnUnsafeExpr(c, field, func(col string) string {
		return col + " < ?"
	}, value)
}

func CondLessOrEqual[T TableNamer, F any](c *CondBuilder[T], field *F, value F) {
	condColumnUnsafeExpr(c, field, func(col string) string {
		return col + " <= ?"
	}, value)
}

func CondColumnExpr[T TableNamer, F any](
	c *CondBuilder[T], field *F, fn func(col string) string, args ...any,
) {
	condColumnUnsafeExpr(c, field, func(col string) string {
		return "(" + fn(col) + ")"
	}, args...)
}

func condColumnUnsafeExpr[T TableNamer, F any](
	c *CondBuilder[T], field *F, fn func(col string) string, args ...any,
) {
	dbName := c.common.getColumnName(unsafe.Pointer(field))
	c.condList = append(c.condList, fn(dbName))
	c.args = append(c.args, args...)
}

func CondIsNull[T TableNamer, F any](c *CondBuilder[T], field *null.Null[F]) {
	condColumnUnsafeExpr(c, field, func(col string) string {
		return col + " IS NULL"
	})
}

func CondIsNotNull[T TableNamer, F any](c *CondBuilder[T], field *null.Null[F]) {
	condColumnUnsafeExpr(c, field, func(col string) string {
		return col + " IS NOT NULL"
	})
}

func CondWhereIn[T TableNamer](c *CondBuilder[T], values []T, columnFunc ColumnGetterFunc[T]) {
	getter, table := NewColumnGetter(c.common.schema)
	columnFunc(getter, table)

	if len(getter.columns) == 0 {
		panicFormat("where in column list must not be empty")
	}

	var buf strings.Builder
	args := c.common.buildColumnsWhereInCond(&buf, getter.columns, getter.columnOffsets, values)

	c.condList = append(c.condList, buf.String())
	c.args = append(c.args, args...)
}

func CondLimit[T TableNamer](c *CondBuilder[T], limit int) {
	c.withLimit = null.New(limit)
}

func CondOrderBy[T TableNamer](c *CondBuilder[T], colList func(b *OrderByBuilder[T])) {
	builder := NewOrderByBuilder(c.common.basePtr, c.common.schema, c.common.dialect)
	colList(builder)
	if builder.IsEmpty() {
		return
	}

	c.withOrderBy = null.New(builder.BuildQuery())
}
