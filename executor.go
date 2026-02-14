package dbc

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/QuangTung97/dbc/null"
)

type DatabaseDialect int

const (
	DialectMysql    DatabaseDialect = iota + 1
	DialectPostgres                 // TODO add test
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

func (e *Executor[T]) getValuesOfEntity(
	offsetList []fieldOffsetType,
) func(entityVal reflect.Value) []any {
	var empty T
	typ := reflect.TypeOf(empty)

	// build map from offset => field index
	indexMap := map[fieldOffsetType]int{}
	for index := range typ.NumField() {
		field := typ.Field(index)
		offset := fieldOffsetType(field.Offset)
		indexMap[offset] = index
	}

	return func(entityVal reflect.Value) []any {
		result := make([]any, 0, len(offsetList))
		for _, offset := range offsetList {
			index := indexMap[offset]
			val := entityVal.Field(index).Interface()
			result = append(result, val)
		}
		return result
	}
}

func (e *Executor[T]) GetByID(ctx context.Context, id T) (null.Null[T], error) {
	var buf strings.Builder
	primaryKeys, primaryOffsets := e.buildSelectQuery(&buf, true)

	e.buildPrimaryEqualMatchSingle(&buf, primaryKeys)
	args := e.getValuesOfEntity(primaryOffsets)(reflect.ValueOf(id))

	return NullGet[T](ctx, buf.String(), args...)
}

func (e *Executor[T]) GetWithLock(ctx context.Context, id T) (null.Null[T], error) {
	var buf strings.Builder
	primaryKeys, primaryOffsets := e.buildSelectQuery(&buf, true)

	e.buildPrimaryEqualMatchSingle(&buf, primaryKeys)
	args := e.getValuesOfEntity(primaryOffsets)(reflect.ValueOf(id))
	buf.WriteString(" FOR UPDATE")

	return NullGet[T](ctx, buf.String(), args...)
}

func (e *Executor[T]) GetMulti(ctx context.Context, idList []T) ([]T, error) {
	if len(idList) == 0 {
		return nil, nil
	}

	var buf strings.Builder
	primaryKeys, primaryOffsets := e.buildSelectQuery(&buf, true)
	args := e.buildPrimaryEqualMatchMulti(&buf, primaryKeys, primaryOffsets, idList)

	// execute
	tx := GetReadonly(ctx)
	var result []T
	err := tx.SelectContext(ctx, &result, buf.String(), args...)
	return result, err
}

func (e *Executor[T]) buildWhereCondFromCond(buf *strings.Builder, cond CondBuilderFunc[T]) ([]any, bool) {
	builder, table := NewCondBuilder[T](e.dialect)
	cond(builder, table)
	if builder.IsEmpty() {
		return nil, true
	}

	whereCond, args := builder.GetWhereCond()
	buf.WriteString(" WHERE ")
	buf.WriteString(whereCond)
	return args, false
}

func (e *Executor[T]) GetCond(ctx context.Context, cond CondBuilderFunc[T]) (null.Null[T], error) {
	var buf strings.Builder
	e.buildSelectQuery(&buf, false)
	args, _ := e.buildWhereCondFromCond(&buf, cond)
	return NullGet[T](ctx, buf.String(), args...)
}

func (e *Executor[T]) SelectCond(ctx context.Context, cond CondBuilderFunc[T]) ([]T, error) {
	var buf strings.Builder
	e.buildSelectQuery(&buf, false)
	args, _ := e.buildWhereCondFromCond(&buf, cond)

	var result []T
	err := GetReadonly(ctx).SelectContext(ctx, &result, buf.String(), args...)
	return result, err
}

func (e *Executor[T]) buildPrimaryEqualMatchSingle(buf *strings.Builder, primaryKeys []string) {
	for index, keyCol := range primaryKeys {
		if index > 0 {
			buf.WriteString(" AND ")
		}
		buf.WriteString(e.quoteIdent(keyCol))
		buf.WriteString(" = ?")
	}
}

func (e *Executor[T]) buildPrimaryEqualMatchMulti(
	buf *strings.Builder, primaryKeys []string, primaryOffsets []fieldOffsetType, idList []T,
) []any {
	if len(primaryKeys) > 1 {
		e.buildWhereInMultiCols(buf, primaryKeys)
		buf.WriteString(" IN ")
		e.buildPlaceholderTwoLevels(buf, len(primaryKeys), len(idList))
	} else {
		buf.WriteString(e.quoteIdent(primaryKeys[0]))
		buf.WriteString(" IN ")
		e.buildPlaceholderLen(buf, len(idList))
	}

	// build args
	getFunc := e.getValuesOfEntity(primaryOffsets)
	args := make([]any, 0, len(primaryOffsets)*len(idList))
	for _, id := range idList {
		args = append(args, getFunc(reflect.ValueOf(id))...)
	}
	return args
}

func (e *Executor[T]) buildSelectQuery(
	buf *strings.Builder, withWhere bool,
) ([]string, []fieldOffsetType) {
	buf.WriteString("SELECT ")

	var primaryKeys []string
	var primaryOffsets []fieldOffsetType
	fieldCount := 0

	for _, offset := range e.schema.allFields {
		info := e.schema.fieldInfos[offset]
		if info.isPrimaryKey {
			primaryKeys = append(primaryKeys, info.dbName)
			primaryOffsets = append(primaryOffsets, offset)
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
	var emptyValue T
	buf.WriteString(e.quoteIdent(emptyValue.TableName()))
	if withWhere {
		buf.WriteString(" WHERE ")
	}

	return primaryKeys, primaryOffsets
}

func (e *Executor[T]) buildPlaceholderLen(buf *strings.Builder, size int) {
	buf.WriteString("(")
	for index := range size {
		if index > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString("?")
	}
	buf.WriteString(")")
}

func (e *Executor[T]) buildPlaceholderTwoLevels(
	buf *strings.Builder, numInner int, numOuter int,
) {
	buf.WriteString("(")
	for y := range numOuter {
		if y > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString("(")
		for x := range numInner {
			if x > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString("?")
		}
		buf.WriteString(")")
	}
	buf.WriteString(")")
}

func (e *Executor[T]) buildWhereInMultiCols(buf *strings.Builder, cols []string) {
	buf.WriteString("(")
	for index, col := range cols {
		if index > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(e.quoteIdent(col))
	}
	buf.WriteString(")")
}

func (e *Executor[T]) Insert(ctx context.Context, entity *T) error {
	var buf strings.Builder
	buf.WriteString("INSERT INTO ")

	buf.WriteString(e.quoteIdent((*entity).TableName()))
	buf.WriteString(" (")

	entityVal := reflect.ValueOf(entity).Elem()
	fieldCount := 0
	var args []any

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
		}
		buf.WriteString(e.quoteIdent(info.dbName))

		val := entityVal.Field(index).Interface()
		args = append(args, val)
	}

	buf.WriteString(") VALUES ")
	e.buildPlaceholderLen(&buf, fieldCount)

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

func (e *Executor[T]) Update(ctx context.Context, entity T) error {
	var buf strings.Builder
	buf.WriteString("UPDATE ")
	buf.WriteString(e.quoteIdent(entity.TableName()))
	buf.WriteString(" SET ")

	entityVal := reflect.ValueOf(entity)
	fieldCount := 0
	var args []any

	var primaryKeys []string
	var primaryOffsets []fieldOffsetType

	for index := range entityVal.NumField() {
		offset := e.schema.allFields[index]
		info := e.schema.fieldInfos[offset]

		if info.isPrimaryKey {
			primaryKeys = append(primaryKeys, info.dbName)
			primaryOffsets = append(primaryOffsets, offset)
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
	e.buildPrimaryEqualMatchSingle(&buf, primaryKeys)
	args = append(args, e.getValuesOfEntity(primaryOffsets)(entityVal)...)

	tx := GetTx(ctx)
	_, err := tx.ExecContext(ctx, buf.String(), args...)
	return err
}

// TODO update multi
// TODO update with condition
// TODO add insert or update multi

func (e *Executor[T]) Delete(ctx context.Context, entity T) error {
	var buf strings.Builder
	primaryKeys, primaryOffsets := e.buildDeleteQuery(&buf)

	e.buildPrimaryEqualMatchSingle(&buf, primaryKeys)
	args := e.getValuesOfEntity(primaryOffsets)(reflect.ValueOf(entity))

	tx := GetTx(ctx)
	_, err := tx.ExecContext(ctx, buf.String(), args...)
	return err
}

func (e *Executor[T]) DeleteMulti(ctx context.Context, idList []T) error {
	var buf strings.Builder
	primaryKeys, primaryOffsets := e.buildDeleteQuery(&buf)
	args := e.buildPrimaryEqualMatchMulti(&buf, primaryKeys, primaryOffsets, idList)

	tx := GetTx(ctx)
	_, err := tx.ExecContext(ctx, buf.String(), args...)
	return err
}

func (e *Executor[T]) DeleteCond(ctx context.Context, cond CondBuilderFunc[T]) error {
	var buf strings.Builder
	buf.WriteString("DELETE FROM ")
	var empty T
	buf.WriteString(e.quoteIdent(empty.TableName()))

	args, isEmpty := e.buildWhereCondFromCond(&buf, cond)
	if isEmpty {
		return fmt.Errorf("delete where condition must not be empty")
	}

	tx := GetTx(ctx)
	_, err := tx.ExecContext(ctx, buf.String(), args...)
	return err
}

func (e *Executor[T]) buildDeleteQuery(buf *strings.Builder) ([]string, []fieldOffsetType) {
	buf.WriteString("DELETE FROM ")
	var empty T
	buf.WriteString(e.quoteIdent(empty.TableName()))

	var primaryKeys []string
	var primaryOffsets []fieldOffsetType
	for _, offset := range e.schema.allFields {
		info := e.schema.fieldInfos[offset]
		if info.isPrimaryKey {
			primaryKeys = append(primaryKeys, info.dbName)
			primaryOffsets = append(primaryOffsets, offset)
		}
	}

	buf.WriteString(" WHERE ")
	return primaryKeys, primaryOffsets
}

func (e *Executor[T]) quoteIdent(name string) string {
	return quoteIdentWithDialect(e.dialect, name)
}
