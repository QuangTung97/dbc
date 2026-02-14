package dbc

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCondBuilder_Equal(t *testing.T) {
	c, table := NewCondBuilder[tableTest03](DialectMysql)
	CondEqual(c, &table.RoleID, testRoleID(21))

	whereCond, args := c.GetWhereCond()
	assert.Equal(t, "`role_id` = ?", whereCond)
	assert.Equal(t, []any{testRoleID(21)}, args)
}

func TestCondBuilder_ColumnExpr(t *testing.T) {
	c, table := NewCondBuilder[tableTest03](DialectMysql)
	CondColumnExpr(c, &table.RoleID, func(col string) string {
		return fmt.Sprintf("LOWER(%s) = ?", col)
	}, "hello")

	whereCond, args := c.GetWhereCond()
	assert.Equal(t, "LOWER(`role_id`) = ?", whereCond)
	assert.Equal(t, []any{"hello"}, args)
}

func TestCondBuilder_IsNull(t *testing.T) {
	c, table := NewCondBuilder[tableTest05](DialectMysql)
	CondIsNull(c, &table.RoleID)

	whereCond, args := c.GetWhereCond()
	assert.Equal(t, "`role_id` IS NULL", whereCond)
	assert.Equal(t, []any(nil), args)
}

func TestCondBuilder_IsNotNull(t *testing.T) {
	c, table := NewCondBuilder[tableTest05](DialectMysql)
	CondIsNotNull(c, &table.RoleID)

	whereCond, args := c.GetWhereCond()
	assert.Equal(t, "`role_id` IS NOT NULL", whereCond)
	assert.Equal(t, []any(nil), args)
}
