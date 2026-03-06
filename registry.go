package dbc

import (
	"iter"
	"reflect"
	"slices"
	"sync"
)

type FieldTraverseInfo struct {
	Name      string
	DBName    string
	Type      reflect.Type
	IsIgnored bool
}

type UniqueKeyInfo struct {
	Columns []string
}

type IndexInfo struct {
	Name    string
	Columns []string
}

type SchemaInterface interface {
	GetTypeName() string
	GetTypeString() string
	GetReflectType() reflect.Type
	GetTableName() string
	GetPackagePath() string
	TraverseFields() iter.Seq[FieldTraverseInfo]

	GetUniqueKeys() []UniqueKeyInfo
	GetIndexes() []IndexInfo
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
