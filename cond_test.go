package dbc

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCondBuilder_Equal(t *testing.T) {
	c, table := NewCondBuilder[tableTest03](tableTest03Schema, DialectMySQL)
	CondEqual(c, &table.RoleID, testRoleID(21))

	whereCond, args := c.GetWhereCond()
	assert.Equal(t, "`role_id` = ?", whereCond)
	assert.Equal(t, []any{testRoleID(21)}, args)
}

func TestCondBuilder_ColumnExpr(t *testing.T) {
	c, table := NewCondBuilder[tableTest03](tableTest03Schema, DialectMySQL)
	CondColumnExpr(c, &table.RoleID, func(col string) string {
		return fmt.Sprintf("LOWER(%s) = ?", col)
	}, "hello")

	whereCond, args := c.GetWhereCond()
	assert.Equal(t, "LOWER(`role_id`) = ?", whereCond)
	assert.Equal(t, []any{"hello"}, args)
}

func TestCondBuilder_IsNull(t *testing.T) {
	c, table := NewCondBuilder[tableTest05](tableTest05Schema, DialectMySQL)
	CondIsNull(c, &table.RoleID)

	whereCond, args := c.GetWhereCond()
	assert.Equal(t, "`role_id` IS NULL", whereCond)
	assert.Equal(t, []any(nil), args)
}

func TestCondBuilder_IsNotNull(t *testing.T) {
	c, table := NewCondBuilder[tableTest05](tableTest05Schema, DialectMySQL)
	CondIsNotNull(c, &table.RoleID)

	whereCond, args := c.GetWhereCond()
	assert.Equal(t, "`role_id` IS NOT NULL", whereCond)
	assert.Equal(t, []any(nil), args)
}

func TestCondBuilder_With_Limit(t *testing.T) {
	c, table := NewCondBuilder[tableTest05](tableTest05Schema, DialectMySQL)
	CondEqual(c, &table.Username, "user01")
	CondLimit(c, 20)

	whereCond, args := c.GetWhereCond()
	assert.Equal(t, "`username` = ? LIMIT ?", whereCond)
	assert.Equal(t, []any{"user01", 20}, args)
}

func TestCondBuilder_With_Order_By(t *testing.T) {
	c, table := NewCondBuilder[tableTest05](tableTest05Schema, DialectMySQL)
	CondEqual(c, &table.Username, "user01")
	CondLimit(c, 20)
	CondOrderBy(c, func(b *OrderByBuilder[tableTest05]) {
		OrderByAsc(b, &table.RoleID)
		OrderByDesc(b, &table.Age)
	})

	whereCond, args := c.GetWhereCond()
	assert.Equal(t, "`username` = ? ORDER BY `role_id` ASC, `age` DESC LIMIT ?", whereCond)
	assert.Equal(t, []any{"user01", 20}, args)
}
