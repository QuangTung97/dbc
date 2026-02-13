package dbc

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

type executorTest struct {
	Transaction
	ctx context.Context

	schema *Schema[tableTest03]

	insertQueries []string
	insertArgs    [][]any

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
	e.insertQueries = append(e.insertQueries, query)
	e.insertArgs = append(e.insertArgs, args)
	e.currentIncID++
	return &fakeResult{
		insertID: e.currentIncID,
	}, nil
}

func TestExecutor_MySQL__Insert(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
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
		assert.Equal(t, 1, len(e.insertQueries))
		assert.Equal(
			t,
			joinString(
				"INSERT INTO `table_test03` (`role_id`, `username`, `age`)",
				"VALUES (?, ?, ?)",
			),
			e.insertQueries[0],
		)

		// check args
		assert.Equal(t, 1, len(e.insertArgs))
		assert.Equal(t, []any{
			entity.RoleID, entity.Username, entity.Age,
		}, e.insertArgs[0])

		// check insert id
		assert.Equal(t, int64(61), entity.ID)
	})

	t.Run("id not auto inc", func(t *testing.T) {
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
		assert.Equal(t, 1, len(e.insertQueries))
		assert.Equal(
			t,
			joinString(
				"INSERT INTO `table_test03` (`id`, `role_id`, `username`, `age`)",
				"VALUES (?, ?, ?, ?)",
			),
			e.insertQueries[0],
		)

		// check args
		assert.Equal(t, 1, len(e.insertArgs))
		assert.Equal(t, []any{
			entity.ID, entity.RoleID, entity.Username, entity.Age,
		}, e.insertArgs[0])

		// check insert id
		assert.Equal(t, int64(11), entity.ID)
	})
}

func joinString(values ...string) string {
	return strings.Join(values, " ")
}
