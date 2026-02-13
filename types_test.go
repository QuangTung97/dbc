package dbc

import "time"

// ------------------------------

type tableTest01 struct {
}

func (tableTest01) TableName() string {
	return "table_test01"
}

// ------------------------------

type tableTest02 struct {
	ID       int64 `db:"id"`
	Username string
}

func (tableTest02) TableName() string {
	return "table_test02"
}

// ------------------------------

type testRoleID int64

type tableTest03 struct {
	ID        int64      `db:"id"`
	RoleID    testRoleID `db:"role_id"`
	Username  string     `db:"username"`
	Age       int        `db:"age"`
	CreatedAt time.Time  `db:"created_at"`
	UpdatedAt time.Time  `db:"updated_at"`
}

func (tableTest03) TableName() string {
	return "table_test03"
}

// ------------------------------

type tableTest04 struct {
	RoleID    testRoleID `db:"role_id"`
	Username  string     `db:"username"`
	Age       int        `db:"age"`
	Desc      string     `db:"desc"`
	CreatedAt time.Time  `db:"created_at"`
}

func (tableTest04) TableName() string {
	return "table_test04"
}
