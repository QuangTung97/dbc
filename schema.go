package dbc

import (
	"reflect"
	"unsafe"
)

type Schema[T TableNamer] struct {
	def *schemaDefinition[T]

	fieldInfos map[fieldOffsetType]fieldInfo
	allFields  []fieldOffsetType

	primaryKeyType primaryKeyType // TODO delete
}

// ========================================
// Private Types
// ========================================

func (s *Schema[T]) getTableType() string {
	return s.def.tableType.String()
}

type schemaDefinition[T any] struct {
	table          *T
	tableAddr      unsafe.Pointer
	tableType      reflect.Type
	fieldOffsetMap map[fieldOffsetType]reflect.StructField
	checkedFields  map[fieldOffsetType]struct{}
}

func newSchemaDefinition[T any]() *schemaDefinition[T] {
	var emptyValue T
	d := &schemaDefinition[T]{
		table:          &emptyValue,
		tableType:      reflect.TypeOf(emptyValue),
		fieldOffsetMap: map[fieldOffsetType]reflect.StructField{},
		checkedFields:  map[fieldOffsetType]struct{}{},
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

type fieldSpecType int

const (
	fieldSpecEditable fieldSpecType = iota + 1
	fieldSpecConst
	fieldSpecIgnored
)

func (t fieldSpecType) isVisible() bool {
	switch t {
	case fieldSpecEditable:
		return true
	case fieldSpecConst:
		return true
	default:
		return false
	}
}

type fieldInfo struct {
	dbName       string
	specType     fieldSpecType
	isAutoInc    bool
	isPrimaryKey bool
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
		s.allFields = append(s.allFields, offset)
		s.def.fieldOffsetMap[offset] = field

		dbName := field.Tag.Get("db")
		if len(dbName) == 0 {
			panicFormat("missing struct tag of field '%s' in type '%s'", field.Name, s.getTableType())
		}

		s.fieldInfos[offset] = fieldInfo{
			dbName: dbName,
		}
	}

	definitionFn(s, s.def.table)

	// do validate
	if s.primaryKeyType == 0 {
		panicFormat("missing 'id' column definition in type '%s'", s.getTableType())
	}

	for _, offset := range s.allFields {
		fieldType := s.def.fieldOffsetMap[offset]
		_, ok := s.def.checkedFields[offset]
		if !ok {
			panicFormat("missing column spec of field '%s' in type '%s'", fieldType.Name, s.getTableType())
		}
	}

	s.def = nil
	return s
}

// ==========================================
// Schema Definition Functions
// ==========================================

func (s *Schema[T]) getDef() *schemaDefinition[T] {
	if s.def == nil {
		// TODO add test
		panic("function is not allowed to run outside schema definition callback")
	}
	return s.def
}

func (s *Schema[T]) getOffsetOfField(fieldPtr unsafe.Pointer) fieldOffsetType {
	def := s.getDef()

	offset := unsafePointerSub(fieldPtr, def.tableAddr)
	fieldType, ok := def.fieldOffsetMap[offset]
	if !ok {
		// TODO testing
		panicFormat("TODO invalid")
	}

	if _, existed := def.checkedFields[offset]; existed {
		panicFormat("field '%s' in type '%s' has already been specified", fieldType.Name, s.getTableType())
	}

	def.checkedFields[offset] = struct{}{}
	return offset
}

func doSchemaIDInt64[T TableNamer, F ~int64](s *Schema[T], field *F) fieldOffsetType {
	offset := s.getOffsetOfField(unsafe.Pointer(field))
	s.primaryKeyType = primaryKeyInt64
	s.updateFieldInfo(offset, func(info *fieldInfo) {
		info.isPrimaryKey = true
		info.specType = fieldSpecConst
	})
	return offset
}

func SchemaIDInt64[T TableNamer, F ~int64](s *Schema[T], field *F) {
	doSchemaIDInt64(s, field)
}

func SchemaIDAutoInc[T TableNamer, F ~int64](s *Schema[T], field *F) {
	offset := doSchemaIDInt64(s, field)
	s.updateFieldInfo(offset, func(info *fieldInfo) {
		info.isAutoInc = true
	})
}

func SchemaCompositePrimaryKey[T TableNamer, F any](s *Schema[T], field *F) {
	offset := s.getOffsetOfField(unsafe.Pointer(field))
	s.primaryKeyType = primaryKeyNonInt
	s.updateFieldInfo(offset, func(info *fieldInfo) {
		info.isPrimaryKey = true
		info.specType = fieldSpecConst
	})
}

func (s *Schema[T]) updateFieldInfo(offset fieldOffsetType, fn func(info *fieldInfo)) {
	info := s.fieldInfos[offset]
	fn(&info)
	s.fieldInfos[offset] = info
}

func SchemaConst[T TableNamer, F any](s *Schema[T], field *F) {
	offset := s.getOffsetOfField(unsafe.Pointer(field))
	s.updateFieldInfo(offset, func(info *fieldInfo) {
		info.specType = fieldSpecConst
	})
}

func SchemaEditable[T TableNamer, F any](s *Schema[T], field *F) {
	offset := s.getOffsetOfField(unsafe.Pointer(field))
	s.updateFieldInfo(offset, func(info *fieldInfo) {
		info.specType = fieldSpecEditable
	})
}

func SchemaIgnore[T TableNamer, F any](s *Schema[T], field *F) {
	offset := s.getOffsetOfField(unsafe.Pointer(field))
	s.updateFieldInfo(offset, func(info *fieldInfo) {
		info.specType = fieldSpecIgnored
	})
}
