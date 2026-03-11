package postgrescheck

import (
	"reflect"
	"time"

	"github.com/QuangTung97/dbc/schemacheck"
)

func DefaultMatchColumnType(schemaType reflect.Type, dataType string) bool {
	if schemaType.Kind() == reflect.String {
		switch dataType {
		case "varchar", "char", "text":
			return true
		}
	}

	if schemacheck.KindIsInt(schemaType.Kind()) {
		switch dataType {
		case "int4", "int8":
			return true
		}
	}

	if schemaType.Kind() == reflect.Bool {
		return dataType == "bool"
	}

	if schemaType.Kind() == reflect.Struct {
		var empty time.Time
		if schemaType.ConvertibleTo(reflect.TypeOf(empty)) {
			return allowTimeType(dataType)
		}
	}

	return false
}

var _ schemacheck.ColumnTypeMatchFunc = DefaultMatchColumnType

func allowTimeType(dataType string) bool {
	switch dataType {
	case "timestamptz":
		return true
	}
	return false
}
