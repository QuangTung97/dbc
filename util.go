package dbc

import (
	"fmt"
	"iter"
	"reflect"
	"unsafe"
)

func panicFormat(format string, args ...any) {
	panic(fmt.Sprintf(format, args...))
}

func unsafePointerSub(a, b unsafe.Pointer) fieldOffsetType {
	return fieldOffsetType(a) - fieldOffsetType(b)
}

func quoteIdentWithDialect(dialect DatabaseDialect, name string) string {
	switch dialect {
	case DialectMysql:
		return "`" + name + "`"
	case DialectPostgres:
		return `"` + name + `"`
	default:
		return name
	}
}

func getValueOfStructFieldAt(val reflect.Value, indices structFieldIndices) reflect.Value {
	for len(indices) > 0 {
		val = val.Field(indices[0])
		indices = indices[1:]
	}
	return val
}

type fieldScanInfo struct {
	fieldName string
	dbName    string
	indices   structFieldIndices
	offset    uintptr
	fieldType reflect.Type
}

func traverseFieldsOfType(structType reflect.Type) iter.Seq[fieldScanInfo] {
	iterFunc := traverseFieldsOfTypeRecursive(structType, nil, 0)
	return func(yield func(fieldScanInfo) bool) {
		iterFunc(yield)
	}
}

func traverseFieldsOfTypeRecursive(
	structType reflect.Type, prevIndices []int,
	startOffset uintptr,
) func(yield func(fieldScanInfo) bool) bool {
	return func(yield func(fieldScanInfo) bool) bool {
		for index := range structType.NumField() {
			field := structType.Field(index)
			indices := append(prevIndices, index)

			// handle struct embedding
			if field.Anonymous {
				subIter := traverseFieldsOfTypeRecursive(
					field.Type, indices, startOffset+field.Offset,
				)
				if !subIter(yield) {
					return false
				}
				continue
			}

			info := fieldScanInfo{
				fieldName: field.Name,
				dbName:    field.Tag.Get(DBTag),
				indices:   structFieldIndices(indices),
				offset:    startOffset + field.Offset,
				fieldType: field.Type,
			}
			if !yield(info) {
				return false
			}
		}
		return true
	}
}

// --------------------------------------------------------------------------------------

type commonBuilder[T TableNamer] struct {
	basePtr unsafe.Pointer
	schema  *Schema[T]
	dialect DatabaseDialect
}

func (c *commonBuilder[T]) getColumnName(fieldPtr unsafe.Pointer) string {
	offset := unsafePointerSub(fieldPtr, c.basePtr)
	return c.quoteIdent(c.schema.getFieldInfo(offset).dbName)
}

func (c *commonBuilder[T]) quoteIdent(name string) string {
	return quoteIdentWithDialect(c.dialect, name)
}
