package tests

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/QuangTung97/dbc"
	"github.com/QuangTung97/dbc/null"
)

func RunTestInsertAuthUserThenGet(t *testing.T, conf TestConfig) {
	tc := NewTestCase(t, conf)
	exec := tc.UserExec

	user1 := AuthUser{Username: "user01", Age: 21}

	// insert first
	err := exec.Insert(tc.Ctx, &user1)
	assert.Equal(t, nil, err)
	assert.Equal(t, UserID(1), user1.ID)

	// insert multi
	user2 := AuthUser{Username: "user02", Age: 22}
	user3 := AuthUser{Username: "user03", Age: 23}
	err = exec.InsertMulti(tc.Ctx, []*AuthUser{&user2, &user3})
	assert.Equal(t, nil, err)
	assert.Equal(t, UserID(2), user2.ID)
	assert.Equal(t, UserID(3), user3.ID)

	// get by id
	nullUser, err := exec.GetByID(tc.Ctx, AuthUser{ID: 2})
	assert.Equal(t, nil, err)
	assert.Equal(t, null.New(user2), nullUser)

	// get by id not found
	nullUser, err = exec.GetByID(tc.Ctx, AuthUser{ID: 4})
	assert.Equal(t, nil, err)
	assert.Equal(t, null.Null[AuthUser]{}, nullUser)

	// select all
	users, err := exec.SelectCond(tc.Ctx, func(cond *dbc.CondBuilder[AuthUser], table *AuthUser) {
		dbc.CondOrderBy(cond, func(b *dbc.OrderByBuilder[AuthUser]) {
			dbc.OrderByDesc(b, &table.ID)
		})
	})
	assert.Equal(t, nil, err)
	assert.Equal(t, []AuthUser{user3, user2, user1}, users)

	// select with limit
	users, err = exec.SelectCond(tc.Ctx, func(cond *dbc.CondBuilder[AuthUser], table *AuthUser) {
		dbc.CondOrderBy(cond, func(b *dbc.OrderByBuilder[AuthUser]) {
			dbc.OrderByDesc(b, &table.ID)
		})
		dbc.CondLimit(cond, 2)
	})
	assert.Equal(t, nil, err)
	assert.Equal(t, []AuthUser{user3, user2}, users)

	// select with filter
	users, err = exec.SelectCond(tc.Ctx, func(cond *dbc.CondBuilder[AuthUser], table *AuthUser) {
		dbc.CondEqual(cond, &table.Age, user2.Age)
	})
	assert.Equal(t, nil, err)
	assert.Equal(t, []AuthUser{user2}, users)

	// get with cond
	nullUser, err = exec.GetCond(tc.Ctx, func(cond *dbc.CondBuilder[AuthUser], table *AuthUser) {
		dbc.CondEqual(cond, &table.Age, user3.Age)
	})
	assert.Equal(t, nil, err)
	assert.Equal(t, null.New(user3), nullUser)

	// get multi
	users, err = exec.GetMulti(tc.Ctx, []AuthUser{{ID: user1.ID}, {ID: user3.ID}})
	assert.Equal(t, nil, err)
	assert.Equal(t, []AuthUser{user1, user3}, users)

	// select with where in
	users, err = exec.SelectCond(tc.Ctx, func(cond *dbc.CondBuilder[AuthUser], table *AuthUser) {
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

func RunTestInsertAuthUserThenUpdate(t *testing.T, conf TestConfig) {
	tc := NewTestCase(t, conf)
	exec := tc.UserExec

	// insert multi
	user1 := AuthUser{Username: "user01", Age: 21}
	user2 := AuthUser{Username: "user02", Age: 22}
	user3 := AuthUser{Username: "user03", Age: 23}
	err := exec.InsertMulti(tc.Ctx, []*AuthUser{&user1, &user2, &user3})
	assert.Equal(t, nil, err)

	// update user2
	user2.Age += 10
	err = exec.Update(tc.Ctx, user2)
	assert.Equal(t, nil, err)

	// select all
	users, err := exec.SelectCond(tc.Ctx, func(cond *dbc.CondBuilder[AuthUser], table *AuthUser) {
		dbc.CondOrderBy(cond, func(b *dbc.OrderByBuilder[AuthUser]) {
			dbc.OrderByAsc(b, &table.ID)
		})
	})
	assert.Equal(t, nil, err)
	assert.Equal(t, []AuthUser{user1, user2, user3}, users)

	// update by cond
	affected, err := exec.UpdateCond(
		tc.Ctx,
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
	users, err = exec.SelectCond(tc.Ctx, func(cond *dbc.CondBuilder[AuthUser], table *AuthUser) {
		dbc.CondOrderBy(cond, func(b *dbc.OrderByBuilder[AuthUser]) {
			dbc.OrderByAsc(b, &table.ID)
		})
	})
	assert.Equal(t, nil, err)
	user1.Age = 40
	user2.Age = 40
	assert.Equal(t, []AuthUser{user1, user2, user3}, users)
}

func RunTestInsertAuthUserThenUpdateMulti(t *testing.T, conf TestConfig) {
	tc := NewTestCase(t, conf)
	exec := tc.UserExec

	// insert multi
	user1 := AuthUser{Username: "user01", Age: 21}
	user2 := AuthUser{Username: "user02", Age: 22}
	user3 := AuthUser{Username: "user03", Age: 23}
	err := exec.InsertMulti(tc.Ctx, []*AuthUser{&user1, &user2, &user3})
	assert.Equal(t, nil, err)

	// update multi
	user1.Username = "user11"
	user2.Username = "user12"
	user3.Username = "user13"
	err = exec.InsertOrUpdateMulti(
		tc.Ctx,
		[]AuthUser{user1, user2, user3},
		func(b *dbc.UpdateMultiBuilder[AuthUser], table *AuthUser) {
			dbc.UpdateMultiAssign(b, &table.Username)
			dbc.UpdateMultiColumnExpr(b, &table.Age, func(oldCol string, newCol string) string {
				return oldCol + " + 5"
			})
		},
	)
	assert.Equal(t, nil, err)

	// select all
	users, err := exec.SelectCond(tc.Ctx, func(cond *dbc.CondBuilder[AuthUser], table *AuthUser) {
		dbc.CondOrderBy(cond, func(b *dbc.OrderByBuilder[AuthUser]) {
			dbc.OrderByAsc(b, &table.ID)
		})
	})
	assert.Equal(t, nil, err)

	user1.Age += 5
	user2.Age += 5
	user3.Age += 5
	assert.Equal(t, []AuthUser{user1, user2, user3}, users)
}

func RunTestInsertAuthUserThenDelete(t *testing.T, conf TestConfig) {
	tc := NewTestCase(t, conf)
	exec := tc.UserExec

	// insert multi
	user1 := AuthUser{Username: "user01", Age: 21}
	user2 := AuthUser{Username: "user02", Age: 22}
	user3 := AuthUser{Username: "user03", Age: 23}
	err := exec.InsertMulti(tc.Ctx, []*AuthUser{&user1, &user2, &user3})
	assert.Equal(t, nil, err)

	// delete
	err = exec.Delete(tc.Ctx, user2)
	assert.Equal(t, nil, err)

	// select all
	users, err := exec.SelectCond(tc.Ctx, func(cond *dbc.CondBuilder[AuthUser], table *AuthUser) {
		dbc.CondOrderBy(cond, func(b *dbc.OrderByBuilder[AuthUser]) {
			dbc.OrderByAsc(b, &table.ID)
		})
	})
	assert.Equal(t, nil, err)
	assert.Equal(t, []AuthUser{user1, user3}, users)
}

func RunTestInsertAuthUserThenDeleteMulti(t *testing.T, conf TestConfig) {
	tc := NewTestCase(t, conf)
	exec := tc.UserExec

	// insert multi
	user1 := AuthUser{Username: "user01", Age: 21}
	user2 := AuthUser{Username: "user02", Age: 22}
	user3 := AuthUser{Username: "user03", Age: 23}
	err := exec.InsertMulti(tc.Ctx, []*AuthUser{&user1, &user2, &user3})
	assert.Equal(t, nil, err)

	// delete
	err = exec.DeleteMulti(tc.Ctx, []AuthUser{user1, user2})
	assert.Equal(t, nil, err)

	// select all
	users, err := exec.SelectCond(tc.Ctx, func(cond *dbc.CondBuilder[AuthUser], table *AuthUser) {
		dbc.CondOrderBy(cond, func(b *dbc.OrderByBuilder[AuthUser]) {
			dbc.OrderByAsc(b, &table.ID)
		})
	})
	assert.Equal(t, nil, err)
	assert.Equal(t, []AuthUser{user3}, users)
}

func RunTestInsertAuthUserThenDeleteByCond(t *testing.T, conf TestConfig) {
	tc := NewTestCase(t, conf)
	exec := tc.UserExec

	// insert multi
	user1 := AuthUser{Username: "user01", Age: 21}
	user2 := AuthUser{Username: "user02", Age: 22}
	user3 := AuthUser{Username: "user03", Age: 23}
	err := exec.InsertMulti(tc.Ctx, []*AuthUser{&user1, &user2, &user3})
	assert.Equal(t, nil, err)

	// delete
	err = exec.DeleteCond(tc.Ctx, func(b *dbc.CondBuilder[AuthUser], table *AuthUser) {
		dbc.CondEqual(b, &table.Age, user2.Age)
	})
	assert.Equal(t, nil, err)

	// select all
	users, err := exec.SelectCond(tc.Ctx, func(cond *dbc.CondBuilder[AuthUser], table *AuthUser) {
		dbc.CondOrderBy(cond, func(b *dbc.OrderByBuilder[AuthUser]) {
			dbc.OrderByAsc(b, &table.ID)
		})
	})
	assert.Equal(t, nil, err)
	assert.Equal(t, []AuthUser{user1, user3}, users)
}

func RunTestInsertUserPermissionThenGet(t *testing.T, conf TestConfig) {
	tc := NewTestCase(t, conf)
	exec := tc.PermExec

	perm1 := UserPermission{
		UserID: 21,
		Perm:   "perm01",
	}
	perm2 := UserPermission{
		UserID: 22,
		Perm:   "perm02",
		Desc:   null.New("desc 02"),
	}
	perm3 := UserPermission{
		UserID: 23,
		Perm:   "perm03",
		Desc:   null.New("desc 03"),
	}
	err := exec.Insert(tc.Ctx, &perm1)
	assert.Equal(t, nil, err)

	// insert multi
	err = exec.InsertMulti(tc.Ctx, []*UserPermission{&perm2, &perm3})
	assert.Equal(t, nil, err)

	// get all
	getAllFunc := func() []UserPermission {
		perms, err := exec.SelectCond(tc.Ctx, func(cond *dbc.CondBuilder[UserPermission], table *UserPermission) {
			dbc.CondOrderBy(cond, func(b *dbc.OrderByBuilder[UserPermission]) {
				dbc.OrderByDesc(b, &table.UserID)
				dbc.OrderByDesc(b, &table.Perm)
			})
		})
		assert.Equal(t, nil, err)
		return perms
	}
	perms := getAllFunc()
	assert.Equal(t, []UserPermission{perm3, perm2, perm1}, perms)

	// update multi
	perm2.Desc = null.New("new desc 12")
	perm3.Desc = null.New("new desc 13")
	err = exec.InsertOrUpdateMulti(
		tc.Ctx,
		[]UserPermission{perm2, perm3},
		func(b *dbc.UpdateMultiBuilder[UserPermission], table *UserPermission) {
			dbc.UpdateMultiAssign(b, &table.Desc)
		},
	)
	assert.Equal(t, nil, err)

	// get all again
	perms = getAllFunc()
	assert.Equal(t, []UserPermission{perm3, perm2, perm1}, perms)
}

func RunAllAuthUserTests(t *testing.T, conf TestConfig) {
	RunTestInsertAuthUserThenGet(t, conf)
	RunTestInsertAuthUserThenUpdate(t, conf)
	RunTestInsertAuthUserThenUpdateMulti(t, conf)
	RunTestInsertAuthUserThenDelete(t, conf)
	RunTestInsertAuthUserThenDeleteMulti(t, conf)
	RunTestInsertAuthUserThenDeleteByCond(t, conf)

	RunTestInsertUserPermissionThenGet(t, conf)
	// TODO add tests for postgres & sqlite3
}
