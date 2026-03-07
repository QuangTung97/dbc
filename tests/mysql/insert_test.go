package mysql_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/QuangTung97/dbc"
	"github.com/QuangTung97/dbc/null"
)

func TestInsertAuthUser__Then_Get(t *testing.T) {
	tc := newTestCase(t)
	exec := tc.userExec

	user1 := AuthUser{Username: "user01", Age: 21}

	// insert first
	err := exec.Insert(tc.ctx, &user1)
	assert.Equal(t, nil, err)
	assert.Equal(t, UserID(1), user1.ID)

	// insert multi
	user2 := AuthUser{Username: "user02", Age: 22}
	user3 := AuthUser{Username: "user03", Age: 23}
	err = exec.InsertMulti(tc.ctx, []*AuthUser{&user2, &user3})
	assert.Equal(t, nil, err)
	assert.Equal(t, UserID(2), user2.ID)
	assert.Equal(t, UserID(3), user3.ID)

	// get by id
	nullUser, err := exec.GetByID(tc.ctx, AuthUser{ID: 2})
	assert.Equal(t, nil, err)
	assert.Equal(t, null.New(user2), nullUser)

	// get by id not found
	nullUser, err = exec.GetByID(tc.ctx, AuthUser{ID: 4})
	assert.Equal(t, nil, err)
	assert.Equal(t, null.Null[AuthUser]{}, nullUser)

	// select all
	users, err := exec.SelectCond(tc.ctx, func(cond *dbc.CondBuilder[AuthUser], table *AuthUser) {
		dbc.CondOrderBy(cond, func(b *dbc.OrderByBuilder[AuthUser]) {
			dbc.OrderByDesc(b, &table.ID)
		})
	})
	assert.Equal(t, nil, err)
	assert.Equal(t, []AuthUser{user3, user2, user1}, users)

	// select with limit
	users, err = exec.SelectCond(tc.ctx, func(cond *dbc.CondBuilder[AuthUser], table *AuthUser) {
		dbc.CondOrderBy(cond, func(b *dbc.OrderByBuilder[AuthUser]) {
			dbc.OrderByDesc(b, &table.ID)
		})
		dbc.CondLimit(cond, 2)
	})
	assert.Equal(t, nil, err)
	assert.Equal(t, []AuthUser{user3, user2}, users)

	// select with filter
	users, err = exec.SelectCond(tc.ctx, func(cond *dbc.CondBuilder[AuthUser], table *AuthUser) {
		dbc.CondEqual(cond, &table.Age, user2.Age)
	})
	assert.Equal(t, nil, err)
	assert.Equal(t, []AuthUser{user2}, users)

	// get with cond
	nullUser, err = exec.GetCond(tc.ctx, func(cond *dbc.CondBuilder[AuthUser], table *AuthUser) {
		dbc.CondEqual(cond, &table.Age, user3.Age)
	})
	assert.Equal(t, nil, err)
	assert.Equal(t, null.New(user3), nullUser)

	// get multi
	users, err = exec.GetMulti(tc.ctx, []AuthUser{{ID: user1.ID}, {ID: user3.ID}})
	assert.Equal(t, nil, err)
	assert.Equal(t, []AuthUser{user1, user3}, users)

	// select with where in
	users, err = exec.SelectCond(tc.ctx, func(cond *dbc.CondBuilder[AuthUser], table *AuthUser) {
		dbc.CondWhereIn(cond,
			[]AuthUser{{Age: 21}, {Age: 22}},
			func(g *dbc.ColumnGetter[AuthUser], table *AuthUser) {
				dbc.ReturnColumn(g, &table.Age)
			},
		)
		dbc.CondOrderBy(cond, func(b *dbc.OrderByBuilder[AuthUser]) {
			dbc.OrderByDesc(b, &table.ID)
		})
	})
	assert.Equal(t, nil, err)
	assert.Equal(t, []AuthUser{user2, user1}, users)
}

func TestInsertAuthUser__Then_Update(t *testing.T) {
	tc := newTestCase(t)
	exec := tc.userExec

	// insert multi
	user1 := AuthUser{Username: "user01", Age: 21}
	user2 := AuthUser{Username: "user02", Age: 22}
	user3 := AuthUser{Username: "user03", Age: 23}
	err := exec.InsertMulti(tc.ctx, []*AuthUser{&user1, &user2, &user3})
	assert.Equal(t, nil, err)

	// update user2
	user2.Age += 10
	err = exec.Update(tc.ctx, user2)
	assert.Equal(t, nil, err)

	// select all
	users, err := exec.SelectCond(tc.ctx, func(cond *dbc.CondBuilder[AuthUser], table *AuthUser) {
	})
	assert.Equal(t, nil, err)
	assert.Equal(t, []AuthUser{user1, user2, user3}, users)

	// update by cond
	affected, err := exec.UpdateCond(
		tc.ctx,
		func(b *dbc.UpdateBuilder[AuthUser], table *AuthUser) {
			dbc.UpdateAssign(b, &table.Age, 40)
		},
		func(cond *dbc.CondBuilder[AuthUser], table *AuthUser) {
			dbc.CondLess(cond, &table.ID, user3.ID)
		},
	)
	assert.Equal(t, nil, err)
	assert.Equal(t, int64(2), affected)

	// select all again
	users, err = exec.SelectCond(tc.ctx, func(cond *dbc.CondBuilder[AuthUser], table *AuthUser) {
	})
	assert.Equal(t, nil, err)
	user1.Age = 40
	user2.Age = 40
	assert.Equal(t, []AuthUser{user1, user2, user3}, users)
}
