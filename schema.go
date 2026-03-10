package dbc

import (
	"iter"
	"reflect"
	"unsafe"
)

type Schema[T TableNamer] struct {
	def *schemaDefinition[T]

	fieldInfos map[fieldOffsetType]fieldInfo
	allFields  []fieldOffsetType

	typeString        string
	tableName         string
	primaryKeyDefined bool

	uniqueKeys []UniqueKeyInfo
	indexList  []IndexInfo
}

func (s *Schema[T]) getFieldInfo(offset fieldOffsetType) fieldInfo {
	info, ok := s.fieldInfos[offset]
	if !ok {
		panicFormat("not found field at offset: %d", offset)
	}
	return info
}

// ========================================
// Private Types
// ========================================

type schemaDefinition[T any] struct {
	table         *T
	tableAddr     unsafe.Pointer
	tableType     reflect.Type
	checkedFields map[checkFieldKey]struct{}
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
		table:         &emptyValue,
		tableType:     reflect.TypeOf(emptyValue),
		checkedFields: map[checkFieldKey]struct{}{},
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
	fieldName    string
	indices      structFieldIndices
	dbName       string
	specType     fieldSpecType
	isAutoInc    bool
	isPrimaryKey bool

	isOptional    bool
	validatorList []func(val any) error
}

func RegisterSchema[T TableNamer](
	definitionFn func(s *Schema[T], table *T),
	options ...SchemaOption,
) *Schema[T] {
	s := &Schema[T]{
		def:        newSchemaDefinition[T](),
		fieldInfos: map[fieldOffsetType]fieldInfo{},
	}

	conf := newSchemaConfig(options)

	s.typeString = s.def.tableType.String()
	s.tableName = (*s.def.table).TableName()

	for scanInfo := range traverseFieldsOfType(s.def.tableType) {
		offset := fieldOffsetType(scanInfo.offset)
		s.allFields = append(s.allFields, offset)

		dbName := scanInfo.dbName
		if len(dbName) == 0 {
			panicFormat("missing struct tag of field '%s' of struct '%s'", scanInfo.fieldName, s.typeString)
		}

		s.fieldInfos[offset] = fieldInfo{
			fieldName: scanInfo.fieldName,
			indices:   scanInfo.indices,
			dbName:    dbName,
		}
	}

	definitionFn(s, s.def.table)

	// do validate
	if !s.primaryKeyDefined {
		panicFormat("missing 'id' column or primary key definition of struct '%s'", s.typeString)
	}

	allDBName := map[string]struct{}{}
	for _, offset := range s.allFields {
		info := s.fieldInfos[offset]
		key := checkFieldKey{
			offset: offset,
			typ:    checkTypeSpec,
		}
		_, ok := s.def.checkedFields[key]
		if !ok {
			panicFormat("missing column spec of field '%s' of struct '%s'", info.fieldName, s.typeString)
		}

		// validate duplicated db name
		_, existed := allDBName[info.dbName]
		if existed {
			panicFormat("duplicated column name '%s' in struct '%s'", info.dbName, s.typeString)
		}
		allDBName[info.dbName] = struct{}{}

		if info.isPrimaryKey && info.isOptional {
			panicFormat(
				"can not config optional for primary column '%s' of struct '%s'",
				info.fieldName, s.typeString,
			)
		}
	}

	s.def = nil

	// add to global registry
	if !conf.withoutRegistering {
		addToGlobalSchema(s)
	}

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
	info, ok := s.fieldInfos[offset]
	if !ok {
		panicFormat("invalid field address value")
	}

	checkKey := checkFieldKey{
		offset: offset,
		typ:    checkFieldType,
	}
	if _, existed := def.checkedFields[checkKey]; existed {
		panicFormat("field '%s' of struct '%s' has already been specified", info.fieldName, s.typeString)
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

func SchemaPrimaryKey[T TableNamer, F any](s *Schema[T], field *F) {
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
			var fieldObj F
			return Errorf(
				errorCodeValidateConvert,
				"can not convert from '%s' to '%s'",
				reflect.TypeOf(val).String(),
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

	columns       []string
	columnOffsets []fieldOffsetType
}

type ColumnGetterFunc[T TableNamer] = func(g *ColumnGetter[T], table *T)

func NewColumnGetter[T TableNamer](schema *Schema[T]) (*ColumnGetter[T], *T) {
	var empty T
	obj := &empty
	return &ColumnGetter[T]{
		schema:   schema,
		baseAddr: unsafe.Pointer(obj),
	}, obj
}

func (s *Schema[T]) GetColumnNames(fn ColumnGetterFunc[T]) []string {
	getter, obj := NewColumnGetter(s)
	fn(getter, obj)
	return getter.columns
}

func ReturnColumn[T TableNamer, F any](g *ColumnGetter[T], field *F) {
	offset := unsafePointerSub(unsafe.Pointer(field), g.baseAddr)
	colName := g.schema.getFieldInfo(offset).dbName
	g.columns = append(g.columns, colName)
	g.columnOffsets = append(g.columnOffsets, offset)
}

// ==========================================
// Implementation of SchemaInterface
// ==========================================

func (s *Schema[T]) GetTypeName() string {
	var empty T
	typ := reflect.TypeOf(empty)
	return typ.Name()
}

func (s *Schema[T]) GetTypeString() string {
	return s.typeString
}

func (s *Schema[T]) GetReflectType() reflect.Type {
	var empty T
	return reflect.TypeOf(empty)
}

func (s *Schema[T]) GetTableName() string {
	return s.tableName
}

func (s *Schema[T]) GetPackagePath() string {
	var empty T
	typ := reflect.TypeOf(empty)
	return typ.PkgPath()
}

func (s *Schema[T]) GetPrimaryKeys() []string {
	var result []string
	for _, offset := range s.allFields {
		info := s.getFieldInfo(offset)
		if info.isPrimaryKey {
			result = append(result, info.dbName)
		}
	}
	return result
}

func (s *Schema[T]) TraverseFields() iter.Seq[FieldTraverseInfo] {
	return func(yield func(FieldTraverseInfo) bool) {
		var empty T
		typ := reflect.TypeOf(empty)
		traverseFieldsOfType(typ)(func(info fieldScanInfo) bool {
			schemaFieldInfo := s.getFieldInfo(info.offset)

			return yield(FieldTraverseInfo{
				Name:      info.fieldName,
				DBName:    info.dbName,
				Type:      info.fieldType,
				IsIgnored: schemaFieldInfo.specType == fieldSpecIgnored,
			})
		})
	}
}
