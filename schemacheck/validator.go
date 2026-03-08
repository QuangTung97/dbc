package schemacheck

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/QuangTung97/dbc"
	"github.com/QuangTung97/dbc/null"
)

type ColumnTypeMatchFunc func(schemaType reflect.Type, dataType string) bool

type Validator struct {
	conf            validatorConfig
	loader          TableLoader
	columnMatchFunc ColumnTypeMatchFunc
}

func NewValidator(
	loader TableLoader,
	columnMatchFunc ColumnTypeMatchFunc,
	options ...ValidatorOption,
) *Validator {
	return &Validator{
		conf:            newValidatorConfig(options),
		loader:          loader,
		columnMatchFunc: columnMatchFunc,
	}
}

func (v *Validator) ValidateSchemas(
	ctx context.Context, schemaList []dbc.SchemaInterface,
) error {
	schemaMap := map[string]dbc.SchemaInterface{}
	for _, schema := range schemaList {
		schemaMap[schema.GetTableName()] = schema
	}

	dbTables, err := v.loader.LoadAll(ctx)
	if err != nil {
		return err
	}
	dbTableMap := map[string]TableInfo{}
	for _, table := range dbTables {
		dbTableMap[table.Name] = table

		if _, ok := schemaMap[table.Name]; !ok {
			return fmt.Errorf("no schema for table '%s'", table.Name)
		}
	}

	for _, schema := range schemaList {
		if _, ok := dbTableMap[schema.GetTableName()]; !ok {
			return fmt.Errorf(
				"not found table '%s' in database",
				schema.GetTableName(),
			)
		}
	}

	for _, schema := range schemaList {
		tableName := schema.GetTableName()
		if err := v.validateSingleSchema(schema, dbTableMap[tableName]); err != nil {
			return err
		}
	}

	return nil
}

func (v *Validator) validateSingleSchema(
	schema dbc.SchemaInterface, table TableInfo,
) error {
	schemaColumMap := map[string]dbc.FieldTraverseInfo{}
	var fieldColList []string
	for field := range schema.TraverseFields() {
		schemaColumMap[field.DBName] = field
		fieldColList = append(fieldColList, field.DBName)
	}

	tableColumnMap := map[string]ColumnInfo{}
	for _, col := range table.Columns {
		tableColumnMap[col.Name] = col
		if _, ok := schemaColumMap[col.Name]; !ok {
			return fmt.Errorf(
				"not found column '%s' in schema '%s'",
				col.Name, schema.GetTypeString(),
			)
		}
	}

	for _, col := range fieldColList {
		if _, ok := tableColumnMap[col]; !ok {
			return fmt.Errorf(
				"not found column '%s' in table '%s'",
				col, table.Name,
			)
		}
	}

	for _, col := range fieldColList {
		tableCol := tableColumnMap[col]
		fieldInfo := schemaColumMap[col]

		fieldType := fieldInfo.Type
		if fieldType.Kind() == reflect.Ptr {
			return fmt.Errorf(
				"invalid type '%s' of column '%s.%s'",
				fieldType.String(), table.Name, col,
			)
		}

		innerType, ok := null.IsReflectNullType(fieldType)
		if ok {
			fieldType = innerType
			if !tableCol.Nullable {
				return fmt.Errorf("column '%s.%s' must be nullable", table.Name, col)
			}
		}

		tableCol.DataType = strings.ToLower(tableCol.DataType)
		if !v.columnMatchFunc(fieldType, tableCol.DataType) {
			return fmt.Errorf(
				"column '%s.%s %s' is incompatible with type '%s'",
				table.Name, col, tableCol.DataType,
				fieldInfo.Type.String(),
			)
		}
	}

	if v.conf.enableValidatePrimaryKey {
		if !slices.Equal(schema.GetPrimaryKeys(), table.PrimaryKeys) {
			return fmt.Errorf(
				"mismatch schema primary key (%s) with database primary key (%s) in table '%s'",
				strings.Join(schema.GetPrimaryKeys(), ", "),
				strings.Join(table.PrimaryKeys, ", "),
				table.Name,
			)
		}
	}

	if v.conf.enableValidateUniqueKeys {
		if err := v.validateUniqueKeys(schema, table); err != nil {
			return err
		}
	}

	if v.conf.enableValidateIndexes {
		if err := v.validateIndexes(schema, table); err != nil {
			return err
		}
	}

	return v.conf.tableValidateFunc(schema, table)
}

func (v *Validator) validateUniqueKeys(schema dbc.SchemaInterface, table TableInfo) error {
	schemaSet := map[string]struct{}{}
	for _, info := range schema.GetUniqueKeys() {
		key := strings.Join(info.Columns, "|")
		schemaSet[key] = struct{}{}
	}

	tableSet := map[string]struct{}{}
	for _, info := range table.UniqueKeys {
		key := strings.Join(info.Columns, "|")
		tableSet[key] = struct{}{}
		if _, ok := schemaSet[key]; !ok {
			return fmt.Errorf(
				"not found unique key (%s) in schema '%s'",
				strings.Join(info.Columns, ", "), schema.GetTypeString(),
			)
		}
	}

	for _, info := range schema.GetUniqueKeys() {
		key := strings.Join(info.Columns, "|")
		if _, ok := tableSet[key]; !ok {
			return fmt.Errorf(
				"not found unique key (%s) in table '%s'",
				strings.Join(info.Columns, ", "), table.Name,
			)
		}
	}

	return nil
}

func (v *Validator) validateIndexes(schema dbc.SchemaInterface, table TableInfo) error {
	type indexColumns struct {
		cols []string
	}

	schemaIndexMap := map[string]indexColumns{}
	for _, info := range schema.GetIndexes() {
		schemaIndexMap[info.Name] = indexColumns{
			cols: info.Columns,
		}
	}

	tableIndexMap := map[string]indexColumns{}
	for _, info := range table.Indexes {
		tableIndexMap[info.Name] = indexColumns{
			cols: info.Columns,
		}

		_, ok := schemaIndexMap[info.Name]
		if !ok {
			return fmt.Errorf(
				"missing index '%s' in schema '%s'",
				info.Name, schema.GetTypeString(),
			)
		}
	}

	for _, info := range schema.GetIndexes() {
		tableCols, ok := tableIndexMap[info.Name]
		if !ok {
			return fmt.Errorf(
				"missing index '%s' in table '%s'",
				info.Name, table.Name,
			)
		}

		schemaCols := schemaIndexMap[info.Name]
		if !slices.Equal(schemaCols.cols, tableCols.cols) {
			return fmt.Errorf(
				"mismatch column list of index '%s' in table '%s'",
				info.Name, table.Name,
			)
		}
	}

	return nil
}
