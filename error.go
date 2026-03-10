package dbc

import "fmt"

const (
	errorCodeUpdateBlockEmpty = iota + 1
	errorCodeValidateConvert
	errorCodePanic
	errorCodeInvalidDialect
	errorCodeFieldMustNonZero
	errorCodeFieldMustZero
	errorCodeEmptyWhere
	errorCodeEmptyUpdate
)

type Error struct {
	Code int
	Msg  string
}

func (e *Error) Error() string {
	return fmt.Sprintf("dbc [E%02d]: %s", e.Code, e.Msg)
}

func NewError(code int, msg string) error {
	return &Error{Code: code, Msg: msg}
}

func Errorf(code int, format string, args ...any) error {
	return &Error{
		Code: code,
		Msg:  fmt.Sprintf(format, args...),
	}
}
