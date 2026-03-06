package mysqlcheck

import (
	"reflect"

	"github.com/QuangTung97/dbc/schemacheck"
)

// TODO add loader

func DefaultMatchColumnType(schemaType reflect.Type, dataType string) bool {
	if schemaType.Kind() == reflect.String {
		switch dataType {
		case "varchar", "char":
			return true
		case "tinytext", "text", "mediumtext", "longtext":
			return true
		}
	}

	if schemacheck.KindIsInt(schemaType.Kind()) {
		switch dataType {
		case "int", "tinyint", "smallint", "mediumint", "bigint":
			return true
		}
	}

	if schemaType.Kind() == reflect.Bool {
		return dataType == "boolean"
	}

	return false
}

var _ schemacheck.ColumnTypeMatchFunc = DefaultMatchColumnType
