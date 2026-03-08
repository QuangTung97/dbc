package schemacheck

import (
	"context"
	"errors"
	"iter"
	"reflect"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/QuangTung97/dbc"
	"github.com/QuangTung97/dbc/null"
)

type fakeSchema struct {
	dbc.SchemaInterface

	tableName   string
	fields      []dbc.FieldTraverseInfo
	primaryKeys []string
	uniqueKeys  []dbc.UniqueKeyInfo
}

func newFakeSchema(tableName string, fields ...dbc.FieldTraverseInfo) *fakeSchema {
	return &fakeSchema{
		tableName: tableName,
		fields:    fields,
	}
}

func newFieldInfo(col string, typ reflect.Type) dbc.FieldTraverseInfo {
	return dbc.FieldTraverseInfo{
		Name:   col + "_field",
		DBName: col,
		Type:   typ,
	}
}

func (s *fakeSchema) GetTableName() string {
	return s.tableName
}

func (s *fakeSchema) GetTypeString() string {
	return s.tableName + "_type"
}

func (s *fakeSchema) TraverseFields() iter.Seq[dbc.FieldTraverseInfo] {
	return slices.Values(s.fields)
}

func (s *fakeSchema) GetPrimaryKeys() []string {
	return s.primaryKeys
}

func (s *fakeSchema) GetUniqueKeys() []dbc.UniqueKeyInfo {
	return s.uniqueKeys
}

type matchEntry struct {
	schemaType reflect.Type
	dbType     string
}

type validatorTest struct {
	infos     []TableInfo
	matchList []matchEntry

	val *Validator

	matchInputs []matchEntry
}

func (v *validatorTest) LoadAll(_ context.Context) ([]TableInfo, error) {
	return v.infos, nil
}

func newValidatorTest(options ...ValidatorOption) *validatorTest {
	s := &validatorTest{}
	s.val = NewValidator(
		s,
		func(schemaType reflect.Type, dataType string) bool {
			// construct the validate set
			set := map[matchEntry]struct{}{}
			for _, entry := range s.matchList {
				set[entry] = struct{}{}
			}

			key := matchEntry{
				schemaType: schemaType,
				dbType:     dataType,
			}
			s.matchInputs = append(s.matchInputs, key)

			_, ok := set[key]
			return ok
		},
		options...,
	)
	return s
}

func TestValidator_ValidateSchemas__Empty_Tables(t *testing.T) {
	s := newValidatorTest()

	s.infos = []TableInfo{
		{Name: "table01"},
		{Name: "table02"},
	}

	err := s.val.ValidateSchemas(
		context.Background(),
		[]dbc.SchemaInterface{
			newFakeSchema("table01"),
			newFakeSchema("table02"),
		},
	)
	assert.Equal(t, nil, err)
}

func TestValidator_ValidateSchemas__Missing_DB_Table(t *testing.T) {
	s := newValidatorTest()

	s.infos = []TableInfo{
		{Name: "table01"},
	}

	err := s.val.ValidateSchemas(
		context.Background(),
		[]dbc.SchemaInterface{
			newFakeSchema("table01"),
			newFakeSchema("table02"),
		},
	)
	assert.Equal(t, errors.New("not found table 'table02' in database"), err)
}

func TestValidator_ValidateSchemas__Missing_Schema(t *testing.T) {
	s := newValidatorTest()

	s.infos = []TableInfo{
		{Name: "table01"},
		{Name: "table02"},
	}

	err := s.val.ValidateSchemas(
		context.Background(),
		[]dbc.SchemaInterface{
			newFakeSchema("table01"),
		},
	)
	assert.Equal(t, errors.New("no schema for table 'table02'"), err)
}

func TestValidator_ValidateSchemas__Columns_Match(t *testing.T) {
	s := newValidatorTest()

	s.infos = []TableInfo{
		{
			Name: "table01",
			Columns: []ColumnInfo{
				{Name: "username", DataType: "VARCHAR", Nullable: false},
			},
		},
	}

	s.matchList = []matchEntry{
		{schemaType: reflect.TypeOf(""), dbType: "varchar"},
	}

	// do validate
	err := s.val.ValidateSchemas(
		context.Background(),
		[]dbc.SchemaInterface{
			newFakeSchema(
				"table01",
				newFieldInfo("username", reflect.TypeOf("")),
			),
		},
	)
	assert.Equal(t, nil, err)

	// check inputs
	assert.Equal(t, []matchEntry{
		{schemaType: reflect.TypeOf(""), dbType: "varchar"},
	}, s.matchInputs)
}

func TestValidator_ValidateSchemas__Columns_Missing_In_DB(t *testing.T) {
	s := newValidatorTest()

	s.infos = []TableInfo{
		{
			Name: "table01",
			Columns: []ColumnInfo{
				{Name: "username", DataType: "varchar", Nullable: false},
			},
		},
	}

	// do validate
	err := s.val.ValidateSchemas(
		context.Background(),
		[]dbc.SchemaInterface{
			newFakeSchema(
				"table01",
				newFieldInfo("username", reflect.TypeOf("")),
				newFieldInfo("age", reflect.TypeOf(int64(0))),
			),
		},
	)
	assert.Equal(t, errors.New("not found column 'age' in table 'table01'"), err)
}

func TestValidator_ValidateSchemas__Columns_Missing_In_Schema(t *testing.T) {
	s := newValidatorTest()

	s.infos = []TableInfo{
		{
			Name: "table01",
			Columns: []ColumnInfo{
				{Name: "username", DataType: "varchar", Nullable: false},
				{Name: "age", DataType: "int", Nullable: false},
			},
		},
	}

	// do validate
	err := s.val.ValidateSchemas(
		context.Background(),
		[]dbc.SchemaInterface{
			newFakeSchema(
				"table01",
				newFieldInfo("username", reflect.TypeOf("")),
			),
		},
	)
	assert.Equal(t, errors.New("not found column 'age' in schema 'table01_type'"), err)
}

func TestValidator_ValidateSchemas__Columns_Match__Mismatch_Type(t *testing.T) {
	s := newValidatorTest()

	s.infos = []TableInfo{
		{
			Name: "table01",
			Columns: []ColumnInfo{
				{Name: "username", DataType: "varchar", Nullable: false},
				{Name: "age", DataType: "int", Nullable: false},
			},
		},
	}

	s.matchList = []matchEntry{
		{schemaType: reflect.TypeOf(""), dbType: "varchar"},
	}

	// do validate
	err := s.val.ValidateSchemas(
		context.Background(),
		[]dbc.SchemaInterface{
			newFakeSchema(
				"table01",
				newFieldInfo("username", reflect.TypeOf("")),
				newFieldInfo("age", reflect.TypeOf(int64(0))),
			),
		},
	)
	assert.Equal(t, errors.New("column 'table01.age int' is incompatible with type 'int64'"), err)
}

func TestValidator_ValidateSchemas__Columns_Match__Null_String(t *testing.T) {
	s := newValidatorTest()

	s.infos = []TableInfo{
		{
			Name: "table01",
			Columns: []ColumnInfo{
				{Name: "username", DataType: "varchar", Nullable: true},
			},
		},
	}

	// string to varchar
	s.matchList = []matchEntry{
		{schemaType: reflect.TypeOf(""), dbType: "varchar"},
	}

	// do validate
	var empty null.Null[string]
	err := s.val.ValidateSchemas(
		context.Background(),
		[]dbc.SchemaInterface{
			newFakeSchema(
				"table01",
				newFieldInfo("username", reflect.TypeOf(empty)),
			),
		},
	)
	assert.Equal(t, nil, err)

	// check inputs
	assert.Equal(t, []matchEntry{
		{schemaType: reflect.TypeOf(""), dbType: "varchar"},
	}, s.matchInputs)
}

func TestValidator_ValidateSchemas__Columns_Match__Null_String__DB_Non_Nullable__Error(t *testing.T) {
	s := newValidatorTest()

	s.infos = []TableInfo{
		{
			Name: "table01",
			Columns: []ColumnInfo{
				{Name: "username", DataType: "varchar", Nullable: false}, // non nullable
			},
		},
	}

	// string to varchar
	s.matchList = []matchEntry{
		{schemaType: reflect.TypeOf(""), dbType: "varchar"},
	}

	// do validate
	var empty null.Null[string]
	err := s.val.ValidateSchemas(
		context.Background(),
		[]dbc.SchemaInterface{
			newFakeSchema(
				"table01",
				newFieldInfo("username", reflect.TypeOf(empty)),
			),
		},
	)
	assert.Equal(t, errors.New("column 'table01.username' must be nullable"), err)
}

func TestValidator_ValidateSchemas__Pointer_Column__Error(t *testing.T) {
	s := newValidatorTest()

	s.infos = []TableInfo{
		{
			Name: "table01",
			Columns: []ColumnInfo{
				{Name: "username", DataType: "varchar", Nullable: false},
			},
		},
	}

	s.matchList = []matchEntry{
		{schemaType: reflect.TypeOf(""), dbType: "varchar"},
	}

	// do validate
	var empty string
	err := s.val.ValidateSchemas(
		context.Background(),
		[]dbc.SchemaInterface{
			newFakeSchema(
				"table01",
				newFieldInfo("username", reflect.TypeOf(&empty)),
			),
		},
	)
	assert.Equal(t, errors.New("invalid type '*string' of column 'table01.username'"), err)
}

func TestValidator_ValidateSchemas__Custom_Validator(t *testing.T) {
	var actions []string
	s := newValidatorTest(
		WithCustomTableValidateFunc(func(schema dbc.SchemaInterface, table TableInfo) error {
			actions = append(actions, "validate-fn")
			return errors.New("test validate err")
		}),
	)

	s.infos = []TableInfo{
		{
			Name: "table01",
			Columns: []ColumnInfo{
				{Name: "username", DataType: "VARCHAR", Nullable: false},
			},
		},
	}

	s.matchList = []matchEntry{
		{schemaType: reflect.TypeOf(""), dbType: "varchar"},
	}

	// do validate
	err := s.val.ValidateSchemas(
		context.Background(),
		[]dbc.SchemaInterface{
			newFakeSchema(
				"table01",
				newFieldInfo("username", reflect.TypeOf("")),
			),
		},
	)
	assert.Equal(t, errors.New("test validate err"), err)
	assert.Equal(t, []string{"validate-fn"}, actions)

	// check inputs
	assert.Equal(t, []matchEntry{
		{schemaType: reflect.TypeOf(""), dbType: "varchar"},
	}, s.matchInputs)
}

func TestValidator_ValidateSchemas__Unique_Key_Missing_In_DB(t *testing.T) {
	s := newValidatorTest(
		WithValidateUniqueKey(true),
	)

	s.infos = []TableInfo{
		{
			Name: "table01",
			Columns: []ColumnInfo{
				{Name: "username", DataType: "VARCHAR", Nullable: false},
			},
		},
	}

	schema := newFakeSchema(
		"table01",
		newFieldInfo("username", reflect.TypeOf("")),
	)
	schema.uniqueKeys = []dbc.UniqueKeyInfo{
		{Columns: []string{"role_id", "age"}},
	}

	s.matchList = []matchEntry{
		{schemaType: reflect.TypeOf(""), dbType: "varchar"},
	}

	// do validate
	err := s.val.ValidateSchemas(
		context.Background(),
		[]dbc.SchemaInterface{schema},
	)
	assert.Equal(t, errors.New("not found unique key (role_id, age) in table 'table01'"), err)
}

func TestValidator_ValidateSchemas__Unique_Key_Missing_In_Schema(t *testing.T) {
	s := newValidatorTest(
		WithValidateUniqueKey(true),
	)

	s.infos = []TableInfo{
		{
			Name: "table01",
			Columns: []ColumnInfo{
				{Name: "username", DataType: "VARCHAR", Nullable: false},
			},
			UniqueKeys: []UniqueKeyInfo{
				{Columns: []string{"age", "role_id"}},
			},
		},
	}

	schema := newFakeSchema(
		"table01",
		newFieldInfo("username", reflect.TypeOf("")),
	)

	s.matchList = []matchEntry{
		{schemaType: reflect.TypeOf(""), dbType: "varchar"},
	}

	// do validate
	err := s.val.ValidateSchemas(
		context.Background(),
		[]dbc.SchemaInterface{schema},
	)
	assert.Equal(t, errors.New("not found unique key (age, role_id) in schema 'table01_type'"), err)
}

func TestValidator_ValidateSchemas__Unique_Key__Success(t *testing.T) {
	s := newValidatorTest(
		WithValidateUniqueKey(true),
	)

	s.infos = []TableInfo{
		{
			Name: "table01",
			Columns: []ColumnInfo{
				{Name: "username", DataType: "VARCHAR", Nullable: false},
			},
			UniqueKeys: []UniqueKeyInfo{
				{Columns: []string{"age", "role_id"}},
			},
		},
	}

	schema := newFakeSchema(
		"table01",
		newFieldInfo("username", reflect.TypeOf("")),
	)
	schema.uniqueKeys = []dbc.UniqueKeyInfo{
		{Columns: []string{"age", "role_id"}},
	}

	s.matchList = []matchEntry{
		{schemaType: reflect.TypeOf(""), dbType: "varchar"},
	}

	// do validate
	err := s.val.ValidateSchemas(
		context.Background(),
		[]dbc.SchemaInterface{schema},
	)
	assert.Equal(t, nil, err)
}

func TestValidator_ValidateSchemas__Unique_Key_Missing_In_Schema__Not_Enabled(t *testing.T) {
	s := newValidatorTest()

	s.infos = []TableInfo{
		{
			Name: "table01",
			Columns: []ColumnInfo{
				{Name: "username", DataType: "VARCHAR", Nullable: false},
			},
			UniqueKeys: []UniqueKeyInfo{
				{Columns: []string{"age", "role_id"}},
			},
		},
	}

	schema := newFakeSchema(
		"table01",
		newFieldInfo("username", reflect.TypeOf("")),
	)

	s.matchList = []matchEntry{
		{schemaType: reflect.TypeOf(""), dbType: "varchar"},
	}

	// do validate
	err := s.val.ValidateSchemas(
		context.Background(),
		[]dbc.SchemaInterface{schema},
	)
	assert.Equal(t, nil, err)
}

func TestValidator_ValidateSchemas__Primary_Key_Missing_In_DB(t *testing.T) {
	s := newValidatorTest(
		WithValidatePrimaryKey(true),
	)

	s.infos = []TableInfo{
		{
			Name: "table01",
			Columns: []ColumnInfo{
				{Name: "username", DataType: "VARCHAR", Nullable: false},
			},
		},
	}

	schema := newFakeSchema(
		"table01",
		newFieldInfo("username", reflect.TypeOf("")),
	)
	schema.primaryKeys = []string{"id", "age"}

	s.matchList = []matchEntry{
		{schemaType: reflect.TypeOf(""), dbType: "varchar"},
	}

	// do validate
	err := s.val.ValidateSchemas(
		context.Background(),
		[]dbc.SchemaInterface{schema},
	)
	assert.Equal(
		t,
		errors.New("mismatch schema primary key (id, age) with database primary key () in table 'table01'"),
		err,
	)
}

func TestValidator_ValidateSchemas__Primary_Key_Missing_In_Schema(t *testing.T) {
	s := newValidatorTest(
		WithValidatePrimaryKey(true),
	)

	s.infos = []TableInfo{
		{
			Name: "table01",
			Columns: []ColumnInfo{
				{Name: "username", DataType: "VARCHAR", Nullable: false},
			},
			PrimaryKeys: []string{"age", "role_id"},
		},
	}

	schema := newFakeSchema(
		"table01",
		newFieldInfo("username", reflect.TypeOf("")),
	)

	s.matchList = []matchEntry{
		{schemaType: reflect.TypeOf(""), dbType: "varchar"},
	}

	// do validate
	err := s.val.ValidateSchemas(
		context.Background(),
		[]dbc.SchemaInterface{schema},
	)
	assert.Equal(
		t,
		errors.New("mismatch schema primary key () with database primary key (age, role_id) in table 'table01'"),
		err,
	)
}

func TestValidator_ValidateSchemas__Primary_Key_Match(t *testing.T) {
	s := newValidatorTest(
		WithValidatePrimaryKey(true),
	)

	s.infos = []TableInfo{
		{
			Name: "table01",
			Columns: []ColumnInfo{
				{Name: "username", DataType: "VARCHAR", Nullable: false},
			},
			PrimaryKeys: []string{"age", "role_id"},
		},
	}

	schema := newFakeSchema(
		"table01",
		newFieldInfo("username", reflect.TypeOf("")),
	)
	schema.primaryKeys = []string{"age", "role_id"}

	s.matchList = []matchEntry{
		{schemaType: reflect.TypeOf(""), dbType: "varchar"},
	}

	// do validate
	err := s.val.ValidateSchemas(
		context.Background(),
		[]dbc.SchemaInterface{schema},
	)
	assert.Equal(t, nil, err)
}

func TestValidator_ValidateSchemas__Primary_Key_Missing_In_Schema__Not_Enabled(t *testing.T) {
	s := newValidatorTest()

	s.infos = []TableInfo{
		{
			Name: "table01",
			Columns: []ColumnInfo{
				{Name: "username", DataType: "VARCHAR", Nullable: false},
			},
			PrimaryKeys: []string{"age", "role_id"},
		},
	}

	schema := newFakeSchema(
		"table01",
		newFieldInfo("username", reflect.TypeOf("")),
	)

	s.matchList = []matchEntry{
		{schemaType: reflect.TypeOf(""), dbType: "varchar"},
	}

	// do validate
	err := s.val.ValidateSchemas(
		context.Background(),
		[]dbc.SchemaInterface{schema},
	)
	assert.Equal(t, nil, err)
}
