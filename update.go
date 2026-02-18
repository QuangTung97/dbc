package dbc

import "unsafe"

type UpdateBuilder[T TableNamer] struct {
	basePtr unsafe.Pointer
	schema  *Schema[T]

	dialect DatabaseDialect

	exprList []string
	args     []any
}

func NewUpdateBuilder[T TableNamer](
	schema *Schema[T],
	dialect DatabaseDialect,
) (*UpdateBuilder[T], *T) {
	var empty T
	obj := &empty

	b := &UpdateBuilder[T]{
		basePtr: unsafe.Pointer(obj),
		schema:  schema,
		dialect: dialect,
	}

	return b, obj
}

type UpdateBuilderFunc[T TableNamer] = func(b *UpdateBuilder[T], table *T)

func (b *UpdateBuilder[T]) quoteIdent(name string) string {
	return quoteIdentWithDialect(b.dialect, name)
}

func UpdateAssign[T TableNamer, F any](b *UpdateBuilder[T], field *F, val F) {
	offset := unsafePointerSub(unsafe.Pointer(field), b.basePtr)
	info := b.schema.getFieldInfo(offset)
	b.exprList = append(b.exprList, b.quoteIdent(info.dbName)+" = ?")
	b.args = append(b.args, val)
}
