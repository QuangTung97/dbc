package dbc

import (
	"fmt"
	"reflect"
	"unsafe"
)

type Schema[T TableNamer] struct {
	def *schemaDefinition[T]

	fieldInfos map[fieldOffsetType]fieldInfo
	allFields  []fieldOffsetType

	primaryKeyDefined bool
}

func (s *Schema[T]) getFieldInfo(offset fieldOffsetType) fieldInfo {
	info, ok := s.fieldInfos[offset]
	if !ok {
		panicFormat("not found field at offset: %d", offset)
	}
	return info
}

// TODO support struct embedding

// ========================================
// Private Types
// ========================================

func (s *Schema[T]) getTableTypeName() string {
	return s.def.tableType.String()
}

type schemaDefinition[T any] struct {
	table          *T
	tableAddr      unsafe.Pointer
	tableType      reflect.Type
	fieldOffsetMap map[fieldOffsetType]reflect.StructField
	checkedFields  map[checkFieldKey]struct{}
}

type checkFieldKey struct {
	offset fieldOffsetType
	typ    checkType
}

type checkType int

const (
	checkTypeSpec checkType = iota + 1
	checkTypeValidateOptional
	checkTypeValidateFunc
)

func newSchemaDefinition[T any]() *schemaDefinition[T] {
	var emptyValue T
	d := &schemaDefinition[T]{
		table:          &emptyValue,
		tableType:      reflect.TypeOf(emptyValue),
		fieldOffsetMap: map[fieldOffsetType]reflect.StructField{},
		checkedFields:  map[checkFieldKey]struct{}{},
	}

	d.tableAddr = unsafe.Pointer(d.table)

	return d
}

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

func (t fieldSpecType) isIgnored() bool {
	switch t {
	case fieldSpecIgnored:
		return true
	default:
		return false
	}
}

type structFieldIndices []int

type fieldInfo struct {
	indices      structFieldIndices
	dbName       string
	specType     fieldSpecType
	isAutoInc    bool
	isPrimaryKey bool

	isOptional    bool
	validatorList []func(val any) error
}

// TODO optional can not be set for primary key

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

		dbName := field.Tag.Get(DBTag)
		if len(dbName) == 0 {
			panicFormat("missing struct tag of field '%s' of struct '%s'", field.Name, s.getTableTypeName())
		}

		s.fieldInfos[offset] = fieldInfo{
			indices: structFieldIndices{index}, // TODO handle nested
			dbName:  dbName,
		}
	}

	definitionFn(s, s.def.table)

	// do validate
	if !s.primaryKeyDefined {
		panicFormat("missing 'id' column or primary key definition of struct '%s'", s.getTableTypeName())
	}

	for _, offset := range s.allFields {
		fieldType := s.def.fieldOffsetMap[offset]
		key := checkFieldKey{
			offset: offset,
			typ:    checkTypeSpec,
		}
		_, ok := s.def.checkedFields[key]
		if !ok {
			panicFormat("missing column spec of field '%s' of struct '%s'", fieldType.Name, s.getTableTypeName())
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
		panic("function is not allowed to run outside of schema definition callback")
	}
	return s.def
}

func (s *Schema[T]) getOffsetOfField(fieldPtr unsafe.Pointer, checkFieldType checkType) fieldOffsetType {
	def := s.getDef()

	offset := unsafePointerSub(fieldPtr, def.tableAddr)
	fieldType, ok := def.fieldOffsetMap[offset]
	if !ok {
		panicFormat("invalid field address value")
	}

	checkKey := checkFieldKey{
		offset: offset,
		typ:    checkFieldType,
	}
	if _, existed := def.checkedFields[checkKey]; existed {
		panicFormat("field '%s' of struct '%s' has already been specified", fieldType.Name, s.getTableTypeName())
	}
	def.checkedFields[checkKey] = struct{}{}

	return offset
}

func doSchemaIDInt64[T TableNamer, F ~int64](s *Schema[T], field *F) fieldOffsetType {
	offset := s.getOffsetOfField(unsafe.Pointer(field), checkTypeSpec)
	s.primaryKeyDefined = true
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
	offset := s.getOffsetOfField(unsafe.Pointer(field), checkTypeSpec)
	s.primaryKeyDefined = true
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
	offset := s.getOffsetOfField(unsafe.Pointer(field), checkTypeSpec)
	s.updateFieldInfo(offset, func(info *fieldInfo) {
		info.specType = fieldSpecConst
	})
}

func SchemaEditable[T TableNamer, F any](s *Schema[T], field *F) {
	offset := s.getOffsetOfField(unsafe.Pointer(field), checkTypeSpec)
	s.updateFieldInfo(offset, func(info *fieldInfo) {
		info.specType = fieldSpecEditable
	})
}

func SchemaIgnore[T TableNamer, F any](s *Schema[T], field *F) {
	offset := s.getOffsetOfField(unsafe.Pointer(field), checkTypeSpec)
	s.updateFieldInfo(offset, func(info *fieldInfo) {
		info.specType = fieldSpecIgnored
	})
}

// ==========================================
// Schema Validation Functions
// ==========================================

func ValidateOptional[T TableNamer, F any](s *Schema[T], field *F) {
	offset := s.getOffsetOfField(unsafe.Pointer(field), checkTypeValidateOptional)
	s.updateFieldInfo(offset, func(info *fieldInfo) {
		info.isOptional = true
	})
}

func ValidateFunc[T TableNamer, F any](s *Schema[T], field *F, fn func(value F) error) {
	offset := s.getOffsetOfField(unsafe.Pointer(field), checkTypeValidateFunc)

	validateFunc := func(val any) error {
		fieldVal, ok := val.(F)
		if !ok {
			// TODO testing
			var fieldObj F
			return fmt.Errorf(
				"can not convert to field value type: '%s'",
				reflect.TypeOf(fieldObj).String(),
			)
		}
		return fn(fieldVal)
	}

	s.updateFieldInfo(offset, func(info *fieldInfo) {
		info.validatorList = append(info.validatorList, validateFunc)
	})
}

// ==========================================
// Getter Functions
// ==========================================

type ColumnGetter[T TableNamer] struct {
	schema   *Schema[T]
	baseAddr unsafe.Pointer
	columns  []string
}

func (s *Schema[T]) GetColumnNames(fn func(g *ColumnGetter[T], table *T)) []string {
	var empty T
	obj := &empty

	getter := &ColumnGetter[T]{
		schema:   s,
		baseAddr: unsafe.Pointer(obj),
	}
	fn(getter, obj)

	return getter.columns
}

func ReturnColumn[T TableNamer, F any](g *ColumnGetter[T], field *F) {
	offset := unsafePointerSub(unsafe.Pointer(field), g.baseAddr)
	colName := g.schema.getFieldInfo(offset).dbName
	g.columns = append(g.columns, colName)
}
