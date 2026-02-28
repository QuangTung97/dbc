package dbc

import (
	"strings"
	"unsafe"
)

type OrderByBuilder[T TableNamer] struct {
	common commonBuilder[T]

	columnList []string
}

func NewOrderByBuilder[T TableNamer](
	basePtr unsafe.Pointer,
	schema *Schema[T],
	dialect DatabaseDialect,
) *OrderByBuilder[T] {
	return &OrderByBuilder[T]{
		common: commonBuilder[T]{
			basePtr: basePtr,
			schema:  schema,
			dialect: dialect,
		},
	}
}

// --------------------------------------------------------------------------------------

func (b *OrderByBuilder[T]) IsEmpty() bool {
	return len(b.columnList) == 0
}

func (b *OrderByBuilder[T]) BuildQuery() string {
	return strings.Join(b.columnList, ", ")
}

// --------------------------------------------------------------------------------------

func OrderByAsc[T TableNamer, F any](b *OrderByBuilder[T], field *F) {
	col := b.common.getColumnName(unsafe.Pointer(field))
	b.columnList = append(b.columnList, col+" ASC")
}

func OrderByDesc[T TableNamer, F any](b *OrderByBuilder[T], field *F) {
	col := b.common.getColumnName(unsafe.Pointer(field))
	b.columnList = append(b.columnList, col+" DESC")
}
