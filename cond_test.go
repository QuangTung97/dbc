package dbc

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCondBuilder_Equal(t *testing.T) {
	c, table := NewCondBuilder[tableTest03]()
	CondEqual(c, &table.RoleID, testRoleID(21))

	whereCond, args := c.GetWhereCond()
	assert.Equal(t, "`role_id` = ?", whereCond)
	assert.Equal(t, []any{testRoleID(21)}, args)
}
