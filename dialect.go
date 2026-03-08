package dbc

import "strings"

type DatabaseDialect int

const (
	DialectMySQL DatabaseDialect = iota + 1
	DialectMySQL5x
	DialectPostgres
	DialectSQLite3
)

func (d DatabaseDialect) withReturningSyntax() bool {
	switch d {
	case DialectPostgres, DialectSQLite3:
		return true
	default:
		return false
	}
}

func (d DatabaseDialect) withOnConflictSyntax() bool {
	switch d {
	case DialectPostgres, DialectSQLite3:
		return true
	default:
		return false
	}
}

func (c *commonBuilder[T]) quoteIdent(name string) string {
	dotIndex := strings.Index(name, ".")
	if dotIndex >= 0 {
		return c.quoteIdent(name[:dotIndex]) + "." + c.quoteIdent(name[dotIndex+1:])
	}

	switch c.dialect {
	case DialectMySQL, DialectMySQL5x:
		return "`" + name + "`"
	case DialectPostgres, DialectSQLite3:
		return `"` + name + `"`
	default:
		return name
	}
}

func (c *commonBuilder[T]) computeUpdateMultiNewColumn(col string) string {
	switch c.dialect {
	case DialectMySQL:
		return upsertMultiNewValues + "." + col
	case DialectMySQL5x:
		return "VALUES(" + col + ")"
	case DialectPostgres, DialectSQLite3:
		return upsertMultiPostgresExcluded + "." + col
	default:
		return col
	}
}

func (c *commonBuilder[T]) computeUpdateMultiOldColumn(col string) string {
	switch c.dialect {
	case DialectMySQL, DialectMySQL5x:
		return c.quoteIdent(c.schema.tableName) + "." + col
	case DialectPostgres, DialectSQLite3:
		return c.quoteIdent(c.schema.tableName) + "." + col
	default:
		return col
	}
}
