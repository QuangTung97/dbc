package schemacheck

import (
	"context"
)

type TableLoader interface {
	LoadAll(ctx context.Context) ([]TableInfo, error)
}

type ColumnInfo struct {
	Name     string
	DataType string
	Nullable bool
}

type UniqueKeyInfo struct {
	Columns []string
}

type IndexInfo struct {
	Name    string
	Columns []string
}

type TableInfo struct {
	Name       string
	Columns    []ColumnInfo
	UniqueKeys []UniqueKeyInfo
	Indexes    []IndexInfo
}
