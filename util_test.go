package dbc

import "strings"

func joinString(values ...string) string {
	return strings.Join(values, " ")
}
