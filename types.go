package dbc

type TableNamer interface {
	TableName() string
}
