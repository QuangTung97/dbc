package mysql

import "time"

type UserID int64

type AuthUser struct {
	ID       UserID `db:"id"`
	Username string `db:"username"`
	Age      int32  `db:"age"`

	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`
}
