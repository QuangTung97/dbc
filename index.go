package dbc

import "slices"

func (s *Schema[T]) GetUniqueKeys() []UniqueKeyInfo {
	return slices.Clone(s.uniqueKeys)
}

func SchemaUniqueKey[T TableNamer](
	schema *Schema[T], columnsFunc ColumnGetterFunc[T],
) {
	getter, table := NewColumnGetter(schema)
	columnsFunc(getter, table)
	schema.uniqueKeys = append(schema.uniqueKeys, UniqueKeyInfo{
		Columns: getter.columns,
	})
}
