package dbc

import (
	"iter"
	"reflect"
	"slices"
	"sync"
)

type FieldTraverseInfo struct {
	Name   string
	DBName string
	Type   reflect.Type
}

type SchemaInterface interface {
	GetTypeName() string
	GetTypeString() string
	GetTableName() string
	GetPackagePath() string
	TraverseFields() iter.Seq[FieldTraverseInfo]
}

var globalRegistryMut sync.Mutex
var globalSchemaList []SchemaInterface

func addToGlobalSchema(schema SchemaInterface) {
	globalRegistryMut.Lock()
	globalSchemaList = append(globalSchemaList, schema)
	globalRegistryMut.Unlock()
}

func GetAllSchemas() []SchemaInterface {
	globalRegistryMut.Lock()
	defer globalRegistryMut.Unlock()
	return slices.Clone(globalSchemaList)
}

// clearAllSchemas for testing only
func clearAllSchemas() {
	globalRegistryMut.Lock()
	defer globalRegistryMut.Unlock()
	globalSchemaList = nil
}
