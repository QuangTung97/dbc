package dbc

import "slices"

func (s *Schema[T]) GetUniqueKeys() []UniqueKeyInfo {
	return slices.Clone(s.uniqueKeys)
}

func (s *Schema[T]) GetIndexes() []IndexInfo {
	return slices.Clone(s.indexList)
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

func SchemaIndex[T TableNamer](
	schema *Schema[T],
	indexName string,
	columnsFunc ColumnGetterFunc[T],
) {
	getter, table := NewColumnGetter(schema)
	columnsFunc(getter, table)
	schema.indexList = append(schema.indexList, IndexInfo{
		Name:    indexName,
		Columns: getter.columns,
	})
}
