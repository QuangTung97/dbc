package mysqlcheck

import (
	"reflect"

	"github.com/QuangTung97/dbc/schemacheck"
)

// TODO add validate schema mapping to MySQL Database

func DefaultMatchColumnType(schemaType reflect.Type, dataType string) bool {
	return true
}

var _ schemacheck.ColumnTypeMatchFunc = DefaultMatchColumnType
