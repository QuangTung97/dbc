package mysql_test

import (
	"time"

	"github.com/QuangTung97/dbc"
)

type UserID int64

type AuthUser struct {
	ID       UserID `db:"id"`
	Username string `db:"username"`
	Age      int32  `db:"age"`

	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`
}

func (AuthUser) TableName() string {
	return "auth_user"
}

var AuthUserSchema = dbc.RegisterSchema(func(s *dbc.Schema[AuthUser], table *AuthUser) {
	dbc.SchemaIDAutoInc(s, &table.ID)
	dbc.SchemaConst(s, &table.Username)
	dbc.SchemaEditable(s, &table.Age)

	dbc.SchemaIgnore(s, &table.CreatedAt)
	dbc.SchemaIgnore(s, &table.UpdatedAt)
})
