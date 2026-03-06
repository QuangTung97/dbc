package dbc

import (
	"fmt"
	"iter"
	"reflect"
	"strings"
	"unsafe"
)

func panicFormat(format string, args ...any) {
	panic(fmt.Sprintf(format, args...))
}

func unsafePointerSub(a, b unsafe.Pointer) fieldOffsetType {
	return fieldOffsetType(a) - fieldOffsetType(b)
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
	offset    fieldOffsetType
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
				offset:    fieldOffsetType(startOffset + field.Offset),
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
	switch c.dialect {
	case DialectMySQL, DialectMySQL5x:
		return "`" + name + "`"
	case DialectPostgres:
		return `"` + name + `"`
	default:
		return name
	}
}

func (c *commonBuilder[T]) quoteIdentList(names []string) []string {
	result := make([]string, 0, len(names))
	for _, name := range names {
		result = append(result, c.quoteIdent(name))
	}
	return result
}

func (c *commonBuilder[T]) computeUpdateMultiNewColumn(col string) string {
	switch c.dialect {
	case DialectMySQL:
		return upsertMultiNewValues + "." + col
	case DialectMySQL5x:
		return "VALUES(" + col + ")"
	case DialectPostgres:
		return upsertMultiPostgresExcluded + "." + col
	default:
		return col
	}
}

func (c *commonBuilder[T]) computeUpdateMultiOldColumn(col string) string {
	switch c.dialect {
	case DialectMySQL, DialectMySQL5x:
		return col
	case DialectPostgres:
		return c.quoteIdent(c.schema.tableName) + "." + col
	default:
		return col
	}
}

func (c *commonBuilder[T]) buildWhereInMultiCols(buf *strings.Builder, cols []string) {
	buf.WriteString("(")
	for index, col := range cols {
		if index > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(c.quoteIdent(col))
	}
	buf.WriteString(")")
}

func (*commonBuilder[T]) buildPlaceholderTwoLevels(
	buf *strings.Builder, numInner int, numOuter int,
) {
	buf.WriteString("(")
	for y := range numOuter {
		if y > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString("(")
		for x := range numInner {
			if x > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString("?")
		}
		buf.WriteString(")")
	}
	buf.WriteString(")")
}

func (*commonBuilder[T]) buildPlaceholderLen(buf *strings.Builder, size int) {
	buf.WriteString("(")
	for index := range size {
		if index > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString("?")
	}
	buf.WriteString(")")
}

func (c *commonBuilder[T]) buildColumnsWhereInCond(
	buf *strings.Builder, columns []string, columnOffsets []fieldOffsetType, valueList []T,
) []any {
	if len(columns) > 1 {
		c.buildWhereInMultiCols(buf, columns)
		buf.WriteString(" IN ")
		c.buildPlaceholderTwoLevels(buf, len(columns), len(valueList))
	} else {
		buf.WriteString(c.quoteIdent(columns[0]))
		buf.WriteString(" IN ")
		c.buildPlaceholderLen(buf, len(valueList))
	}

	// build args
	args := make([]any, 0, len(columnOffsets)*len(valueList))
	for _, id := range valueList {
		idArgs := c.getValuesOfEntity(columnOffsets, reflect.ValueOf(id))
		args = append(args, idArgs...)
	}
	return args
}

func (c *commonBuilder[T]) getValuesOfEntity(
	offsetList []fieldOffsetType, entityVal reflect.Value,
) []any {
	result := make([]any, 0, len(offsetList))
	for _, offset := range offsetList {
		indices := c.schema.fieldInfos[offset].indices
		val := getValueOfStructFieldAt(entityVal, indices).Interface()
		result = append(result, val)
	}
	return result
}
