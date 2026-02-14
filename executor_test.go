package dbc

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/QuangTung97/dbc/null"
)

type executorTest struct {
	Transaction
	ctx context.Context

	schema       *Schema[tableTest03]
	schemaTable4 *Schema[tableTest04]

	execQueries []string
	execArgs    [][]any

	currentIncID int64

	getQueries []string
	getArgs    [][]any
	getResult  tableTest03

	selectQueries []string
	selectArgs    [][]any
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

	e.schemaTable4 = RegisterSchema(func(s *Schema[tableTest04], table *tableTest04) {
		SchemaCompositePrimaryKey(s, &table.RoleID)
		SchemaCompositePrimaryKey(s, &table.Username)

		SchemaEditable(s, &table.Age)
		SchemaEditable(s, &table.Desc)

		SchemaIgnore(s, &table.CreatedAt)
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

func (e *executorTest) newExecTable04() *Executor[tableTest04] {
	exec, err := NewExecutor(DialectMysql, e.schemaTable4)
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

func (e *executorTest) GetContext(
	_ context.Context, dest any, query string, args ...any,
) error {
	e.getQueries = append(e.getQueries, query)
	e.getArgs = append(e.getArgs, args)

	// set result
	val, ok := dest.(*tableTest03)
	if ok {
		*val = e.getResult
	}

	return nil
}

func (e *executorTest) SelectContext(
	_ context.Context, _ any, query string, args ...any,
) error {
	e.selectQueries = append(e.selectQueries, query)
	e.selectArgs = append(e.selectArgs, args)
	return nil
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

	// do update
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

	// do delete
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

func TestExecutor_MySQL__DeleteMulti(t *testing.T) {
	e := newExecTest(t)
	exec := e.newExec()

	entity1 := tableTest03{ID: 11}
	entity2 := tableTest03{ID: 12}
	entity3 := tableTest03{ID: 13}

	// do delete
	err := exec.DeleteMulti(e.ctx, []tableTest03{entity1, entity2, entity3})
	assert.Equal(t, nil, err)

	// check query
	assert.Equal(t, 1, len(e.execQueries))
	assert.Equal(
		t,
		joinString(
			"DELETE FROM `table_test03`",
			"WHERE `id` IN (?, ?, ?)",
		),
		e.execQueries[0],
	)

	// check args
	assert.Equal(t, 1, len(e.execArgs))
	assert.Equal(t, []any{entity1.ID, entity2.ID, entity3.ID}, e.execArgs[0])
}

func TestExecutor_MySQL__GetByID(t *testing.T) {
	e := newExecTest(t)
	exec := e.newExec()

	entity := tableTest03{
		ID: 11,
	}
	e.getResult = tableTest03{
		ID:       11,
		Username: "user01",
	}

	// do get
	nullUser, err := exec.GetByID(e.ctx, entity)
	assert.Equal(t, nil, err)
	assert.Equal(t, null.New(e.getResult), nullUser)

	// check query
	assert.Equal(t, 1, len(e.getQueries))
	assert.Equal(
		t,
		joinString(
			"SELECT `id`, `role_id`, `username`, `age`",
			"FROM `table_test03`",
			"WHERE `id` = ?",
		),
		e.getQueries[0],
	)

	// check args
	assert.Equal(t, 1, len(e.getArgs))
	assert.Equal(t, []any{
		entity.ID,
	}, e.getArgs[0])
}

func TestExecutor_MySQL__GetWithLock(t *testing.T) {
	e := newExecTest(t)
	exec := e.newExec()

	entity := tableTest03{
		ID: 11,
	}

	// do get
	_, err := exec.GetWithLock(e.ctx, entity)
	assert.Equal(t, nil, err)

	// check query
	assert.Equal(t, 1, len(e.getQueries))
	assert.Equal(
		t,
		joinString(
			"SELECT `id`, `role_id`, `username`, `age`",
			"FROM `table_test03`",
			"WHERE `id` = ? FOR UPDATE",
		),
		e.getQueries[0],
	)

	// check args
	assert.Equal(t, 1, len(e.getArgs))
	assert.Equal(t, []any{
		entity.ID,
	}, e.getArgs[0])
}

func TestExecutor_MySQL__GetMulti(t *testing.T) {
	e := newExecTest(t)
	exec := e.newExec()

	entity1 := tableTest03{ID: 11}
	entity2 := tableTest03{ID: 12}
	entity3 := tableTest03{ID: 13}

	// do get multi
	userList, err := exec.GetMulti(e.ctx, []tableTest03{entity1, entity2, entity3})
	assert.Equal(t, nil, err)
	assert.Equal(t, []tableTest03(nil), userList)

	// check query
	assert.Equal(t, 1, len(e.selectQueries))
	assert.Equal(
		t,
		joinString(
			"SELECT `id`, `role_id`, `username`, `age`",
			"FROM `table_test03`",
			"WHERE `id` IN (?, ?, ?)",
		),
		e.selectQueries[0],
	)

	// check args
	assert.Equal(t, 1, len(e.selectArgs))
	assert.Equal(t, []any{entity1.ID, entity2.ID, entity3.ID}, e.selectArgs[0])
}

func TestExecutor_MySQL__Empty(t *testing.T) {
	e := newExecTest(t)
	exec := e.newExec()

	// do get multi
	userList, err := exec.GetMulti(e.ctx, nil)
	assert.Equal(t, nil, err)
	assert.Equal(t, []tableTest03(nil), userList)

	// check query
	assert.Equal(t, 0, len(e.selectQueries))
	// check args
	assert.Equal(t, 0, len(e.selectArgs))
}

func TestExecutor_MySQL__GetByID__Composite_Key(t *testing.T) {
	e := newExecTest(t)
	exec := e.newExecTable04()

	entity := tableTest04{
		RoleID:   21,
		Username: "user01",
	}

	// do get
	_, err := exec.GetByID(e.ctx, entity)
	assert.Equal(t, nil, err)

	// check query
	assert.Equal(t, 1, len(e.getQueries))
	assert.Equal(
		t,
		joinString(
			"SELECT `role_id`, `username`, `age`, `desc`",
			"FROM `table_test04`",
			"WHERE `role_id` = ? AND `username` = ?",
		),
		e.getQueries[0],
	)

	// check args
	assert.Equal(t, 1, len(e.getArgs))
	assert.Equal(t, []any{entity.RoleID, entity.Username}, e.getArgs[0])
}

func TestExecutor_MySQL__GetMulti__Composite_Key(t *testing.T) {
	e := newExecTest(t)
	exec := e.newExecTable04()

	entity1 := tableTest04{RoleID: 21, Username: "user01"}
	entity2 := tableTest04{RoleID: 22, Username: "user02"}
	entity3 := tableTest04{RoleID: 23, Username: "user03"}

	// do get multi
	userList, err := exec.GetMulti(e.ctx, []tableTest04{entity1, entity2, entity3})
	assert.Equal(t, nil, err)
	assert.Equal(t, []tableTest04(nil), userList)

	// check query
	assert.Equal(t, 1, len(e.selectQueries))
	assert.Equal(
		t,
		joinString(
			"SELECT `role_id`, `username`, `age`, `desc`",
			"FROM `table_test04`",
			"WHERE (`role_id`, `username`) IN ((?, ?), (?, ?), (?, ?))",
		),
		e.selectQueries[0],
	)

	// check args
	assert.Equal(t, 1, len(e.selectArgs))
	assert.Equal(t, []any{
		entity1.RoleID, entity1.Username,
		entity2.RoleID, entity2.Username,
		entity3.RoleID, entity3.Username,
	}, e.selectArgs[0])
}

func TestExecutor_MySQL__GetCond(t *testing.T) {
	e := newExecTest(t)
	exec := e.newExec()

	// do get by condition
	_, err := exec.GetCond(e.ctx, func(b *CondBuilder[tableTest03], table *tableTest03) {
		CondEqual(b, &table.RoleID, testRoleID(31))
	})
	assert.Equal(t, nil, err)

	// check query
	assert.Equal(t, 1, len(e.getQueries))
	assert.Equal(
		t,
		joinString(
			"SELECT `id`, `role_id`, `username`, `age`",
			"FROM `table_test03`",
			"WHERE `role_id` = ?",
		),
		e.getQueries[0],
	)

	// check args
	assert.Equal(t, 1, len(e.getArgs))
	assert.Equal(t, []any{testRoleID(31)}, e.getArgs[0])
}

func TestExecutor_MySQL__SelectCond(t *testing.T) {
	e := newExecTest(t)
	exec := e.newExec()

	// do select by cond
	_, err := exec.SelectCond(e.ctx, func(b *CondBuilder[tableTest03], table *tableTest03) {
		CondEqual(b, &table.Username, "user02")
	})
	assert.Equal(t, nil, err)

	// check query
	assert.Equal(t, 1, len(e.selectQueries))
	assert.Equal(
		t,
		joinString(
			"SELECT `id`, `role_id`, `username`, `age`",
			"FROM `table_test03`",
			"WHERE `username` = ?",
		),
		e.selectQueries[0],
	)

	// check args
	assert.Equal(t, 1, len(e.selectArgs))
	assert.Equal(t, []any{"user02"}, e.selectArgs[0])
}
