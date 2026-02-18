package dbc

import (
	"fmt"
	"reflect"
	"unsafe"
)

func panicFormat(format string, args ...any) {
	panic(fmt.Sprintf(format, args...))
}

// unsafePointerSub TODO validate all usage
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
