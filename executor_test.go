package dbc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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
	selectHandler func(dest any)
}

func newExecTest(_ *testing.T) *executorTest {
	e := &executorTest{}
	e.currentIncID = 60
	e.selectHandler = func(dest any) {
	}

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
		SchemaPrimaryKey(s, &table.RoleID)
		SchemaPrimaryKey(s, &table.Username)

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

func (e *executorTest) newExecWithSchema(schema *Schema[tableTest03]) *Executor[tableTest03] {
	exec, err := NewExecutor(DialectMysql, schema)
	if err != nil {
		panic(err)
	}
	return exec
}

func (e *executorTest) newExecPostgres() *Executor[tableTest03] {
	exec, err := NewExecutor(DialectPostgres, e.schema)
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
	_ context.Context, dest any, query string, args ...any,
) error {
	e.selectQueries = append(e.selectQueries, query)
	e.selectArgs = append(e.selectArgs, args)
	e.selectHandler(dest)
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

func TestExecutor_MySQL__Insert__Validate_Error(t *testing.T) {
	e := newExecTest(t)
	exec := e.newExec()

	entity := tableTest03{
		RoleID:   21,
		Username: "user01",
		Age:      0,
	}

	// do insert
	err := exec.Insert(e.ctx, &entity)
	assert.Equal(t, errors.New("field 'Age' of struct 'dbc.tableTest03' must not be zero"), err)

	// check query
	assert.Equal(t, 0, len(e.execQueries))
}

func newSchemaTable03WithValidate() *Schema[tableTest03] {
	return RegisterSchema(func(s *Schema[tableTest03], table *tableTest03) {
		SchemaIDAutoInc(s, &table.ID)
		SchemaConst(s, &table.RoleID)

		SchemaEditable(s, &table.Username)
		SchemaEditable(s, &table.Age)
		ValidateFunc(s, &table.Age, func(value int) error {
			if value < 40 {
				return errors.New("age must >= 40")
			}
			return nil
		})

		SchemaIgnore(s, &table.CreatedAt)
		SchemaIgnore(s, &table.UpdatedAt)
	})
}

func TestExecutor_MySQL__Insert__Custom_Validator(t *testing.T) {
	e := newExecTest(t)

	// init schema and executor
	schema := newSchemaTable03WithValidate()
	exec := e.newExecWithSchema(schema)

	entity := tableTest03{
		RoleID:   21,
		Username: "user01",
		Age:      31,
	}

	// do insert
	err := exec.Insert(e.ctx, &entity)
	assert.Equal(t, errors.New("age must >= 40"), err)

	// check query
	assert.Equal(t, 0, len(e.execQueries))
}

func TestExecutor_MySQL__Insert__ID_Not_Zero__Error(t *testing.T) {
	e := newExecTest(t)
	exec := e.newExec()

	entity := tableTest03{
		ID:       41,
		RoleID:   21,
		Username: "user01",
		Age:      31,
	}

	// do insert
	err := exec.Insert(e.ctx, &entity)
	assert.Equal(t, errors.New("field 'ID' of struct 'dbc.tableTest03' must be zero"), err)

	// check query
	assert.Equal(t, 0, len(e.execQueries))
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

func TestExecutor_MySQL__Insert_Multi(t *testing.T) {
	e := newExecTest(t)
	exec := e.newExec()

	entity1 := tableTest03{
		RoleID:   21,
		Username: "user01",
		Age:      31,
	}
	entity2 := tableTest03{
		RoleID:   22,
		Username: "user02",
		Age:      32,
	}

	// do insert
	err := exec.InsertMulti(e.ctx, []*tableTest03{&entity1, &entity2})
	assert.Equal(t, nil, err)

	// check query
	assert.Equal(t, 1, len(e.execQueries))
	assert.Equal(
		t,
		joinString(
			"INSERT INTO `table_test03` (`role_id`, `username`, `age`)",
			"VALUES (?, ?, ?), (?, ?, ?)",
		),
		e.execQueries[0],
	)

	// check args
	assert.Equal(t, 1, len(e.execArgs))
	assert.Equal(t, []any{
		entity1.RoleID, entity1.Username, entity1.Age,
		entity2.RoleID, entity2.Username, entity2.Age,
	}, e.execArgs[0])

	// check insert id
	assert.Equal(t, int64(61), entity1.ID)
	assert.Equal(t, int64(62), entity2.ID)
}

func TestExecutor_PostgresL__Insert_Multi(t *testing.T) {
	e := newExecTest(t)
	exec := e.newExecPostgres()

	entity1 := tableTest03{
		RoleID:   21,
		Username: "user01",
		Age:      31,
	}
	entity2 := tableTest03{
		RoleID:   22,
		Username: "user02",
		Age:      32,
	}

	e.selectHandler = func(dest any) {
		idList := dest.(*[]int64)
		*idList = []int64{65, 66}
	}

	// do insert
	err := exec.InsertMulti(e.ctx, []*tableTest03{&entity1, &entity2})
	assert.Equal(t, nil, err)

	// check query
	assert.Equal(t, 1, len(e.selectQueries))
	assert.Equal(
		t,
		joinString(
			`INSERT INTO "table_test03" ("role_id", "username", "age")`,
			`VALUES (?, ?, ?), (?, ?, ?)`,
			`RETURNING id`,
		),
		e.selectQueries[0],
	)

	// check args
	assert.Equal(t, 1, len(e.selectArgs))
	assert.Equal(t, []any{
		entity1.RoleID, entity1.Username, entity1.Age,
		entity2.RoleID, entity2.Username, entity2.Age,
	}, e.selectArgs[0])

	// check insert id
	assert.Equal(t, int64(65), entity1.ID)
	assert.Equal(t, int64(66), entity2.ID)
}

func TestExecutor_MySQL__Insert_Multi__With_Validator_Failed(t *testing.T) {
	e := newExecTest(t)
	exec := e.newExecWithSchema(
		newSchemaTable03WithValidate(),
	)

	entity1 := tableTest03{
		RoleID:   21,
		Username: "user01",
		Age:      31,
	}
	entity2 := tableTest03{
		RoleID:   22,
		Username: "user02",
		Age:      32,
	}

	// do insert
	err := exec.InsertMulti(e.ctx, []*tableTest03{&entity1, &entity2})
	assert.Equal(t, errors.New("age must >= 40"), err)

	// check query
	assert.Equal(t, 0, len(e.execQueries))
}

func TestExecutor_MySQL__Insert__With_Nested(t *testing.T) {
	e := newExecTest(t)
	exec, _ := NewExecutor(DialectMysql, tableTest06Schema)

	entity := tableTest06{
		RoleID: null.New(testRoleID(21)),
		subSchemaStruct01: subSchemaStruct01{
			Username: "user01",
			Age:      31,
		},
	}

	// do insert
	err := exec.Insert(e.ctx, &entity)
	assert.Equal(t, nil, err)

	// check query
	assert.Equal(t, 1, len(e.execQueries))
	assert.Equal(
		t,
		joinString(
			"INSERT INTO `table_test06` (`role_id`, `username`, `age`)",
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

func TestExecutor_MySQL__Update__With_Optional_Age(t *testing.T) {
	e := newExecTest(t)

	schema := RegisterSchema(func(s *Schema[tableTest03], table *tableTest03) {
		SchemaIDAutoInc(s, &table.ID)
		SchemaConst(s, &table.RoleID)

		SchemaEditable(s, &table.Username)
		SchemaEditable(s, &table.Age)
		ValidateOptional(s, &table.Age)

		SchemaIgnore(s, &table.CreatedAt)
		SchemaIgnore(s, &table.UpdatedAt)
	})
	exec := e.newExecWithSchema(schema)

	entity := tableTest03{
		ID:       11,
		RoleID:   21,
		Username: "user01",
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

func TestExecutor_MySQL__Update_Multi(t *testing.T) {
	e := newExecTest(t)
	exec := e.newExec()

	entity1 := tableTest03{
		ID:       11,
		RoleID:   21,
		Username: "user01",
		Age:      31,
	}
	entity2 := tableTest03{
		ID:       12,
		RoleID:   22,
		Username: "user02",
		Age:      32,
	}

	// do update
	err := exec.UpdateMulti(
		e.ctx, []tableTest03{entity1, entity2},
		func(b *UpdateMultiBuilder[tableTest03], table *tableTest03) {
			UpdateMultiAssign(b, &table.Username)
			UpdateMultiColumnExpr(b, &table.Age, func(oldCol string, newCol string) string {
				return oldCol + " + 1"
			})
			UpdateMultiColumnExpr(b, &table.RoleID, func(oldCol string, newCol string) string {
				return newCol + " + 10"
			})
		},
	)
	assert.Equal(t, nil, err)

	// check query
	assert.Equal(t, 1, len(e.execQueries))
	assert.Equal(
		t,
		joinString(
			"INSERT INTO `table_test03`",
			"(`id`, `role_id`, `username`, `age`)",
			"VALUES",
			"(?, ?, ?, ?), (?, ?, ?, ?)",
			"AS new_values",
			"ON DUPLICATE KEY UPDATE",
			"`username` = new_values.`username`,",
			"`age` = `age` + 1,",
			"`role_id` = new_values.`role_id` + 10",
		),
		e.execQueries[0],
	)

	// check args
	assert.Equal(t, 1, len(e.execArgs))
	assert.Equal(t, []any{
		entity1.ID, entity1.RoleID, entity1.Username, entity1.Age,
		entity2.ID, entity2.RoleID, entity2.Username, entity2.Age,
	}, e.execArgs[0])
}

func TestExecutor_MySQL__Update_Cond(t *testing.T) {
	e := newExecTest(t)
	exec := e.newExec()

	// do update cond
	_, err := exec.UpdateCond(
		e.ctx,
		func(b *UpdateBuilder[tableTest03], table *tableTest03) {
			UpdateAssign(b, &table.Age, 41)
			UpdateAssign(b, &table.Username, "user02")
		},
		func(b *CondBuilder[tableTest03], table *tableTest03) {
			CondEqual(b, &table.RoleID, testRoleID(11))
		},
	)
	assert.Equal(t, nil, err)

	// check query
	assert.Equal(t, 1, len(e.execQueries))
	assert.Equal(
		t,
		joinString(
			"UPDATE `table_test03`",
			"SET `age` = ?, `username` = ?",
			"WHERE `role_id` = ?",
		),
		e.execQueries[0],
	)

	// check args
	assert.Equal(t, 1, len(e.execArgs))
	assert.Equal(t, []any{
		41, "user02", testRoleID(11),
	}, e.execArgs[0])
}

func TestExecutor_MySQL__Update_Cond__Username_Empty__Error(t *testing.T) {
	e := newExecTest(t)
	exec := e.newExecWithSchema(
		newSchemaTable03WithValidate(),
	)

	// do update cond
	_, err := exec.UpdateCond(
		e.ctx,
		func(b *UpdateBuilder[tableTest03], table *tableTest03) {
			UpdateAssign(b, &table.Username, "")
		},
		func(b *CondBuilder[tableTest03], table *tableTest03) {
			CondEqual(b, &table.RoleID, testRoleID(11))
		},
	)
	assert.Equal(t, errors.New("field 'Username' of struct 'dbc.tableTest03' must not be zero"), err)

	// check query
	assert.Equal(t, 0, len(e.execQueries))
}

func TestExecutor_MySQL__Update_Cond__Age_Less_Than_40(t *testing.T) {
	e := newExecTest(t)
	exec := e.newExecWithSchema(
		newSchemaTable03WithValidate(),
	)

	// do update cond
	_, err := exec.UpdateCond(
		e.ctx,
		func(b *UpdateBuilder[tableTest03], table *tableTest03) {
			UpdateAssign(b, &table.Age, 31)
		},
		func(b *CondBuilder[tableTest03], table *tableTest03) {
			CondEqual(b, &table.RoleID, testRoleID(11))
		},
	)
	assert.Equal(t, errors.New("age must >= 40"), err)

	// check query
	assert.Equal(t, 0, len(e.execQueries))
}

func TestExecutor_MySQL__Update_Cond__With_Generic_Func(t *testing.T) {
	e := newExecTest(t)
	exec := e.newExec()

	// do update cond
	_, err := exec.UpdateCond(
		e.ctx,
		func(b *UpdateBuilder[tableTest03], table *tableTest03) {
			UpdateAssign(b, &table.Age, 41)
			UpdateColumnExpr(b, &table.Username, func(col string) string {
				return fmt.Sprintf("%s + ?", col)
			}, "another")
		},
		func(b *CondBuilder[tableTest03], table *tableTest03) {
			CondEqual(b, &table.RoleID, testRoleID(11))
		},
	)
	assert.Equal(t, nil, err)

	// check query
	assert.Equal(t, 1, len(e.execQueries))
	assert.Equal(
		t,
		joinString(
			"UPDATE `table_test03`",
			"SET `age` = ?, `username` = `username` + ?",
			"WHERE `role_id` = ?",
		),
		e.execQueries[0],
	)

	// check args
	assert.Equal(t, 1, len(e.execArgs))
	assert.Equal(t, []any{
		41, "another", testRoleID(11),
	}, e.execArgs[0])
}

func TestExecutor_MySQL__Update_Cond__Empty_Where(t *testing.T) {
	e := newExecTest(t)
	exec := e.newExec()

	// do update cond
	_, err := exec.UpdateCond(
		e.ctx,
		func(b *UpdateBuilder[tableTest03], table *tableTest03) {
			UpdateAssign(b, &table.Age, 41)
			UpdateAssign(b, &table.Username, "user02")
		},
		func(b *CondBuilder[tableTest03], table *tableTest03) {
		},
	)
	assert.Equal(t, errors.New("not allow empty where condition"), err)

	// check query
	assert.Equal(t, 0, len(e.execQueries))
}

func TestExecutor_MySQL__Update_Cond__Empty_Update(t *testing.T) {
	e := newExecTest(t)
	exec := e.newExec()

	// do update cond
	_, err := exec.UpdateCond(
		e.ctx,
		func(b *UpdateBuilder[tableTest03], table *tableTest03) {
		},
		func(b *CondBuilder[tableTest03], table *tableTest03) {
			CondEqual(b, &table.RoleID, testRoleID(11))
		},
	)
	assert.Equal(t, errors.New("not allow empty update expression"), err)

	// check query
	assert.Equal(t, 0, len(e.execQueries))
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

func TestExecutor_MySQL__DeleteCond(t *testing.T) {
	e := newExecTest(t)
	exec := e.newExec()

	// do delete
	err := exec.DeleteCond(e.ctx, func(b *CondBuilder[tableTest03], table *tableTest03) {
		CondEqual(b, &table.RoleID, testRoleID(32))
	})
	assert.Equal(t, nil, err)

	// check query
	assert.Equal(t, 1, len(e.execQueries))
	assert.Equal(
		t,
		joinString(
			"DELETE FROM `table_test03`",
			"WHERE `role_id` = ?",
		),
		e.execQueries[0],
	)

	// check args
	assert.Equal(t, 1, len(e.execArgs))
	assert.Equal(t, []any{testRoleID(32)}, e.execArgs[0])
}

func TestExecutor_MySQL__DeleteCond__No_Cond(t *testing.T) {
	e := newExecTest(t)
	exec := e.newExec()

	// do delete
	err := exec.DeleteCond(e.ctx, func(b *CondBuilder[tableTest03], table *tableTest03) {})
	assert.Equal(t, errors.New("delete where condition must not be empty"), err)

	// check query
	assert.Equal(t, 0, len(e.execQueries))
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
	_, err := exec.SelectCond(e.ctx, func(cond *CondBuilder[tableTest03], table *tableTest03) {
		CondEqual(cond, &table.Username, "user02")
		CondOrderBy(cond, func(b *OrderByBuilder[tableTest03]) {
			OrderByAsc(b, &table.Age)
			OrderByDesc(b, &table.RoleID)
		})
		CondLimit(cond, 30)
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
			"ORDER BY `age` ASC, `role_id` DESC",
			"LIMIT ?",
		),
		e.selectQueries[0],
	)

	// check args
	assert.Equal(t, 1, len(e.selectArgs))
	assert.Equal(t, []any{"user02", 30}, e.selectArgs[0])
}
