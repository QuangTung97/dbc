package sqlitecheck

import (
	"reflect"
	"time"

	"github.com/QuangTung97/dbc/schemacheck"
)

func DefaultMatchColumnType(schemaType reflect.Type, dataType string) bool {
	if schemaType.Kind() == reflect.String {
		switch dataType {
		case "text":
			return true
		}
	}

	if schemacheck.KindIsInt(schemaType.Kind()) {
		switch dataType {
		case "integer":
			return true
		}
	}

	if schemaType.Kind() == reflect.Bool {
		return dataType == "integer"
	}

	if schemaType.Kind() == reflect.Struct {
		var empty time.Time
		if schemaType.ConvertibleTo(reflect.TypeOf(empty)) {
			return dataType == "integer"
		}
	}

	return false
}

var _ schemacheck.ColumnTypeMatchFunc = DefaultMatchColumnType
