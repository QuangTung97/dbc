package dbc

import "unsafe"

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
