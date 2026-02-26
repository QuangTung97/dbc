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

func getStructFieldAt(val reflect.Value, indices structFieldIndices) reflect.Value {
	// TODO allow nested
	return val.Field(indices[0])
}

func getStructFieldTypeAt(structType reflect.Type, indices structFieldIndices) reflect.StructField {
	// TODO allow nested
	return structType.Field(indices[0])
}

type fieldScanInfo struct {
	field   reflect.StructField
	indices structFieldIndices
}

func traverseFieldsOfType(structType reflect.Type) iter.Seq[fieldScanInfo] {
	return func(yield func(fieldScanInfo) bool) {
		for index := range structType.NumField() {
			info := fieldScanInfo{
				field:   structType.Field(index),
				indices: structFieldIndices{index}, // TODO support nested
			}
			if !yield(info) {
				return
			}
		}
	}
}
