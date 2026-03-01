package dbc

import (
	"strings"
	"unsafe"
)

type UpdateBuilder[T TableNamer] struct {
	common commonBuilder[T]

	exprList []string
	args     []any

	simpleUpdateList []simpleUpdateInfo
}

type simpleUpdateInfo struct {
	offset fieldOffsetType
	value  any
}

func NewUpdateBuilder[T TableNamer](
	schema *Schema[T],
	dialect DatabaseDialect,
) (*UpdateBuilder[T], *T) {
	var empty T
	obj := &empty

	b := &UpdateBuilder[T]{
		common: commonBuilder[T]{
			basePtr: unsafe.Pointer(obj),
			schema:  schema,
			dialect: dialect,
		},
	}

	return b, obj
}

type UpdateBuilderFunc[T TableNamer] = func(b *UpdateBuilder[T], table *T)

func UpdateAssign[T TableNamer, F any](b *UpdateBuilder[T], field *F, val F) {
	offset := unsafePointerSub(unsafe.Pointer(field), b.common.basePtr)
	info := b.common.schema.getFieldInfo(offset)
	colName := b.common.quoteIdent(info.dbName)

	b.exprList = append(b.exprList, colName+" = ?")
	b.args = append(b.args, val)

	// for validation
	b.simpleUpdateList = append(b.simpleUpdateList, simpleUpdateInfo{
		offset: offset,
		value:  val,
	})
}

func UpdateColumnExpr[T TableNamer, F any](
	b *UpdateBuilder[T], field *F,
	exprFunc func(col string) string,
	args ...any,
) {
	colName := b.common.getColumnName(unsafe.Pointer(field))
	b.exprList = append(b.exprList, colName+" = "+exprFunc(colName))
	b.args = append(b.args, args...)
}

// --------------------------------------------------------------------------------------

type UpdateMultiBuilder[T TableNamer] struct {
	common   commonBuilder[T]
	exprList []string
}

type UpdateMultiBuilderFunc[T TableNamer] = func(b *UpdateMultiBuilder[T], table *T)

func NewUpdateMultiBuilder[T TableNamer](
	schema *Schema[T],
	dialect DatabaseDialect,
) (*UpdateMultiBuilder[T], *T) {
	var empty T
	obj := &empty

	b := &UpdateMultiBuilder[T]{
		common: commonBuilder[T]{
			basePtr: unsafe.Pointer(obj),
			schema:  schema,
			dialect: dialect,
		},
	}

	return b, obj
}

func (b *UpdateMultiBuilder[T]) GetFullExpr() string {
	return strings.Join(b.exprList, ", ")
}

func UpdateMultiAssign[T TableNamer, F any](b *UpdateMultiBuilder[T], field *F) {
	dbName := b.common.getColumnName(unsafe.Pointer(field))
	var buf strings.Builder
	buf.WriteString(dbName)
	buf.WriteString(" = ")
	buf.WriteString(updateMultiNewValues)
	buf.WriteString(".")
	buf.WriteString(dbName)
	b.exprList = append(b.exprList, buf.String())
}

func UpdateMultiColumnExpr[T TableNamer, F any](
	b *UpdateMultiBuilder[T], field *F,
	exprFunc func(oldCol string, newCol string) string,
) {
	dbName := b.common.getColumnName(unsafe.Pointer(field))
	expr := dbName + " = " + exprFunc(dbName, updateMultiNewValues+"."+dbName)
	b.exprList = append(b.exprList, expr)
}
