package dbc

import (
	"context"
	"reflect"
	"strings"

	"github.com/QuangTung97/dbc/null"
)

type DatabaseDialect int

const (
	DialectMysql DatabaseDialect = iota + 1
	DialectPostgres
)

type Executor[T TableNamer] struct {
	dialect DatabaseDialect
	schema  *Schema[T]
}

func NewExecutor[T TableNamer](
	dialect DatabaseDialect, schema *Schema[T],
) (*Executor[T], error) {
	return &Executor[T]{
		dialect: dialect,
		schema:  schema,
	}, nil
}

func (e *Executor[T]) Insert(ctx context.Context, entity *T) error {
	tx := GetTx(ctx)

	var buf strings.Builder
	buf.WriteString("INSERT INTO ")

	var empty T
	buf.WriteString(e.quoteIdent(empty.TableName()))
	buf.WriteString(" (")

	entityVal := reflect.ValueOf(entity).Elem()
	fieldCount := 0
	var args []any
	var placeholder strings.Builder

	var autoIncField null.Null[reflect.Value]

	for index := range entityVal.NumField() {
		offset := e.schema.allFields[index]
		info := e.schema.fieldInfos[offset]
		if !info.specType.isVisible() {
			continue
		}
		if info.isAutoInc {
			autoIncField = null.New(entityVal.Field(index))
			continue
		}

		fieldCount++
		if fieldCount > 1 {
			buf.WriteString(", ")
			placeholder.WriteString(", ")
		}
		buf.WriteString(e.quoteIdent(info.dbName))
		placeholder.WriteString("?")

		val := entityVal.Field(index).Interface()
		args = append(args, val)
	}

	buf.WriteString(") VALUES (")
	buf.WriteString(placeholder.String())
	buf.WriteString(")")

	result, err := tx.ExecContext(ctx, buf.String(), args...)
	if err != nil {
		return err
	}

	if autoIncField.Valid {
		val := autoIncField.Data
		insertID, err := result.LastInsertId()
		if err != nil {
			return err
		}
		val.SetInt(insertID)
	}

	return err
}

func (e *Executor[T]) quoteIdent(name string) string {
	return "`" + name + "`"
}
