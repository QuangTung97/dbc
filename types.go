package dbc

const DBTag = "db"

type TableNamer interface {
	TableName() string
}
