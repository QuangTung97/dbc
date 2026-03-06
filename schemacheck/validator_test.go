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
)

type fakeSchema struct {
	dbc.SchemaInterface

	tableName string
	fields    []dbc.FieldTraverseInfo
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

type matchEntry struct {
	schemaType reflect.Type
	dataType   string
}

type validatorTest struct {
	infos     []TableInfo
	matchList []matchEntry

	val *Validator

	matchInputs []matchEntry
}

func (v *validatorTest) LoadAll(ctx context.Context) ([]TableInfo, error) {
	return v.infos, nil
}

func newValidatorTest() *validatorTest {
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
				dataType:   dataType,
			}
			s.matchInputs = append(s.matchInputs, key)

			_, ok := set[key]
			return ok
		},
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
				{Name: "username", DataType: "varchar", Nullable: false},
			},
		},
	}

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
}
