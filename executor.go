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

func (e *Executor[T]) GetByID(ctx context.Context, id T) (null.Null[T], error) {
	var buf strings.Builder
	buf.WriteString("SELECT ")

	entityVal := reflect.ValueOf(id)
	fieldCount := 0
	var primaryKeys []primaryKeyInfo
	for index := range entityVal.NumField() {
		offset := e.schema.allFields[index]
		info := e.schema.fieldInfos[offset]

		if info.isPrimaryKey {
			primaryKeys = append(primaryKeys, primaryKeyInfo{
				info: info,
				val:  entityVal.Field(index).Interface(),
			})
		}

		if !info.specType.isVisible() {
			continue
		}

		fieldCount++
		if fieldCount > 1 {
			buf.WriteString(", ")
		}
		buf.WriteString(e.quoteIdent(info.dbName))
	}

	buf.WriteString(" FROM ")
	buf.WriteString(e.quoteIdent(id.TableName()))
	buf.WriteString(" WHERE ")

	var args []any
	for index, primaryKey := range primaryKeys {
		if index > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(e.quoteIdent(primaryKey.info.dbName))
		buf.WriteString(" = ?")
		args = append(args, primaryKey.val)
	}

	return NullGet[T](ctx, buf.String(), args...)
}

func (e *Executor[T]) Insert(ctx context.Context, entity *T) error {
	var buf strings.Builder
	buf.WriteString("INSERT INTO ")

	buf.WriteString(e.quoteIdent((*entity).TableName()))
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

	tx := GetTx(ctx)
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

// TODO insert multi

type primaryKeyInfo struct {
	info fieldInfo
	val  any
}

func (e *Executor[T]) Update(ctx context.Context, entity T) error {
	var buf strings.Builder
	buf.WriteString("UPDATE ")
	buf.WriteString(e.quoteIdent(entity.TableName()))
	buf.WriteString(" SET ")

	entityVal := reflect.ValueOf(entity)
	fieldCount := 0
	var args []any
	var primaryKeys []primaryKeyInfo
	for index := range entityVal.NumField() {
		offset := e.schema.allFields[index]
		info := e.schema.fieldInfos[offset]

		if info.isPrimaryKey {
			primaryKeys = append(primaryKeys, primaryKeyInfo{
				info: info,
				val:  entityVal.Field(index).Interface(),
			})
		}

		if info.specType != fieldSpecEditable {
			continue
		}

		fieldCount++
		if fieldCount > 1 {
			buf.WriteString(", ")
		}
		buf.WriteString(e.quoteIdent(info.dbName))
		buf.WriteString(" = ?")
		args = append(args, entityVal.Field(index).Interface())
	}

	buf.WriteString(" WHERE ")
	for index, primaryKey := range primaryKeys {
		if index > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(e.quoteIdent(primaryKey.info.dbName))
		buf.WriteString(" = ?")
		args = append(args, primaryKey.val)
	}

	tx := GetTx(ctx)
	_, err := tx.ExecContext(ctx, buf.String(), args...)
	return err
}

// TODO update multi

func (e *Executor[T]) Delete(ctx context.Context, entity T) error {
	var buf strings.Builder
	buf.WriteString("DELETE FROM ")
	buf.WriteString(e.quoteIdent(entity.TableName()))

	entityVal := reflect.ValueOf(entity)
	var primaryKeys []primaryKeyInfo
	for index := range entityVal.NumField() {
		offset := e.schema.allFields[index]
		info := e.schema.fieldInfos[offset]

		if info.isPrimaryKey {
			primaryKeys = append(primaryKeys, primaryKeyInfo{
				info: info,
				val:  entityVal.Field(index).Interface(),
			})
		}
	}

	buf.WriteString(" WHERE ")
	var args []any
	for index, primaryKey := range primaryKeys {
		if index > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(e.quoteIdent(primaryKey.info.dbName))
		buf.WriteString(" = ?")
		args = append(args, primaryKey.val)
	}

	tx := GetTx(ctx)
	_, err := tx.ExecContext(ctx, buf.String(), args...)
	return err
}

func (e *Executor[T]) quoteIdent(name string) string {
	return "`" + name + "`"
}
