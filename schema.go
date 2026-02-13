package dbc

import (
	"fmt"
	"reflect"
	"unsafe"
)

type Schema[T TableNamer] struct {
	def *schemaDefinition[T]

	fieldInfos map[fieldOffsetType]fieldInfo

	primaryKeyOffset []fieldOffsetType
	primaryKeyType   primaryKeyType
}

// ========================================
// Private Types
// ========================================

type schemaDefinition[T any] struct {
	table          *T
	tableAddr      unsafe.Pointer
	tableType      reflect.Type
	fieldOffsetMap map[fieldOffsetType]reflect.StructField

	validate validateInfo
}

func newSchemaDefinition[T any]() *schemaDefinition[T] {
	var emptyValue T
	d := &schemaDefinition[T]{
		table:          &emptyValue,
		tableType:      reflect.TypeOf(emptyValue),
		fieldOffsetMap: map[fieldOffsetType]reflect.StructField{},
	}

	d.tableAddr = unsafe.Pointer(d.table)

	return d
}

type primaryKeyType int

const (
	primaryKeyInt64 primaryKeyType = iota + 1
	primaryKeyNonInt
)

type fieldOffsetType uintptr

type fieldInfo struct {
	dbName  string
	isConst bool
	ignored bool
}

type validateInfo struct {
}

func RegisterSchema[T TableNamer](
	definitionFn func(s *Schema[T], table *T),
) *Schema[T] {
	s := &Schema[T]{
		def:        newSchemaDefinition[T](),
		fieldInfos: map[fieldOffsetType]fieldInfo{},
	}

	for index := range s.def.tableType.NumField() {
		field := s.def.tableType.Field(index)
		offset := fieldOffsetType(field.Offset)
		s.def.fieldOffsetMap[offset] = field

		dbName := field.Tag.Get("db")
		if len(dbName) == 0 {
			panicFormat("missing struct tag of field '%s' in type '%s'", field.Name, s.def.tableType.String())
		}

		s.fieldInfos[offset] = fieldInfo{
			dbName: dbName,
		}
	}

	definitionFn(s, s.def.table)

	// do validate
	if s.primaryKeyType == 0 {
		panicFormat("missing 'id' column definition in type '%s'", s.def.tableType.String())
	}

	s.def = nil
	return s
}

func panicFormat(format string, args ...any) {
	panic(fmt.Sprintf(format, args...))
}

// ==========================================
// Schema Definition Functions
// ==========================================

func (s *Schema[T]) getDef() *schemaDefinition[T] {
	if s.def == nil {
		panic("function is not allowed to run outside schema definition callback")
	}
	return s.def
}

func SchemaIDInt64[T TableNamer, F ~int64](s *Schema[T], field *F) {
	def := s.getDef()
	offset := unsafePointerSub(unsafe.Pointer(field), def.tableAddr)
	_, ok := def.fieldOffsetMap[offset]
	if !ok {
		panicFormat("TODO invalid")
	}

	s.primaryKeyType = primaryKeyInt64
	s.primaryKeyOffset = []fieldOffsetType{offset}
}

func unsafePointerSub(a, b unsafe.Pointer) fieldOffsetType {
	return fieldOffsetType(a) - fieldOffsetType(b)
}
