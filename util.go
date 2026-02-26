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
	fieldTag  reflect.StructTag
	indices   structFieldIndices
	offset    uintptr
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
				fieldTag:  field.Tag,
				indices:   structFieldIndices(indices),
				offset:    startOffset + field.Offset,
			}
			if !yield(info) {
				return false
			}
		}
		return true
	}
}
