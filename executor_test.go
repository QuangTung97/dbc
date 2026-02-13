package dbc

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
)

type executorTest struct {
	Transaction
	ctx context.Context

	schema *Schema[tableTest03]

	execQueries []string
	execArgs    [][]any

	currentIncID int64
}

func newExecTest(_ *testing.T) *executorTest {
	e := &executorTest{}
	e.currentIncID = 60

	e.ctx = context.Background()
	e.ctx = setToContext(e.ctx, &contextValueType{
		isReadonly: false,
		tx:         e,
	})

	e.schema = RegisterSchema(func(s *Schema[tableTest03], table *tableTest03) {
		SchemaIDAutoInc(s, &table.ID)
		SchemaConst(s, &table.RoleID)

		SchemaEditable(s, &table.Username)
		SchemaEditable(s, &table.Age)

		SchemaIgnore(s, &table.CreatedAt)
		SchemaIgnore(s, &table.UpdatedAt)
	})

	return e
}

func (e *executorTest) newExec() *Executor[tableTest03] {
	exec, err := NewExecutor(DialectMysql, e.schema)
	if err != nil {
		panic(err)
	}
	return exec
}

type fakeResult struct {
	sql.Result
	insertID int64
}

func (r *fakeResult) LastInsertId() (int64, error) {
	return r.insertID, nil
}

func (e *executorTest) ExecContext(
	_ context.Context, query string, args ...any,
) (sql.Result, error) {
	e.execQueries = append(e.execQueries, query)
	e.execArgs = append(e.execArgs, args)
	e.currentIncID++
	return &fakeResult{
		insertID: e.currentIncID,
	}, nil
}

func TestExecutor_MySQL__Insert(t *testing.T) {
	e := newExecTest(t)
	exec := e.newExec()

	entity := tableTest03{
		RoleID:   21,
		Username: "user01",
		Age:      31,
	}

	// do insert
	err := exec.Insert(e.ctx, &entity)
	assert.Equal(t, nil, err)

	// check query
	assert.Equal(t, 1, len(e.execQueries))
	assert.Equal(
		t,
		joinString(
			"INSERT INTO `table_test03` (`role_id`, `username`, `age`)",
			"VALUES (?, ?, ?)",
		),
		e.execQueries[0],
	)

	// check args
	assert.Equal(t, 1, len(e.execArgs))
	assert.Equal(t, []any{
		entity.RoleID, entity.Username, entity.Age,
	}, e.execArgs[0])

	// check insert id
	assert.Equal(t, int64(61), entity.ID)
}

func TestExecutor_MySQL__Insert__ID_Not_Auto_Inc(t *testing.T) {
	e := newExecTest(t)
	e.schema = RegisterSchema(func(s *Schema[tableTest03], table *tableTest03) {
		SchemaIDInt64(s, &table.ID)
		SchemaConst(s, &table.RoleID)

		SchemaEditable(s, &table.Username)
		SchemaEditable(s, &table.Age)

		SchemaIgnore(s, &table.CreatedAt)
		SchemaIgnore(s, &table.UpdatedAt)
	})
	exec := e.newExec()

	entity := tableTest03{
		ID:       11,
		RoleID:   21,
		Username: "user01",
		Age:      31,
	}

	// do insert
	err := exec.Insert(e.ctx, &entity)
	assert.Equal(t, nil, err)

	// check query
	assert.Equal(t, 1, len(e.execQueries))
	assert.Equal(
		t,
		joinString(
			"INSERT INTO `table_test03` (`id`, `role_id`, `username`, `age`)",
			"VALUES (?, ?, ?, ?)",
		),
		e.execQueries[0],
	)

	// check args
	assert.Equal(t, 1, len(e.execArgs))
	assert.Equal(t, []any{
		entity.ID, entity.RoleID, entity.Username, entity.Age,
	}, e.execArgs[0])

	// check insert id
	assert.Equal(t, int64(11), entity.ID)
}

func TestExecutor_MySQL__Update(t *testing.T) {
	e := newExecTest(t)
	exec := e.newExec()

	entity := tableTest03{
		ID:       11,
		RoleID:   21,
		Username: "user01",
		Age:      31,
	}

	// do insert
	err := exec.Update(e.ctx, entity)
	assert.Equal(t, nil, err)

	// check query
	assert.Equal(t, 1, len(e.execQueries))
	assert.Equal(
		t,
		joinString(
			"UPDATE `table_test03`",
			"SET `username` = ?, `age` = ?",
			"WHERE `id` = ?",
		),
		e.execQueries[0],
	)

	// check args
	assert.Equal(t, 1, len(e.execArgs))
	assert.Equal(t, []any{
		entity.Username, entity.Age,
		entity.ID,
	}, e.execArgs[0])
}

func TestExecutor_MySQL__Delete(t *testing.T) {
	e := newExecTest(t)
	exec := e.newExec()

	entity := tableTest03{
		ID: 11,
	}

	// do insert
	err := exec.Delete(e.ctx, entity)
	assert.Equal(t, nil, err)

	// check query
	assert.Equal(t, 1, len(e.execQueries))
	assert.Equal(
		t,
		joinString(
			"DELETE FROM `table_test03`",
			"WHERE `id` = ?",
		),
		e.execQueries[0],
	)

	// check args
	assert.Equal(t, 1, len(e.execArgs))
	assert.Equal(t, []any{
		entity.ID,
	}, e.execArgs[0])
}
