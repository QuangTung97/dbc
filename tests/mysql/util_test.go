package mysql_test

import "reflect"

type simpleType int

func getCurrentPackage() string {
	return reflect.TypeOf(simpleType(0)).PkgPath()
}
