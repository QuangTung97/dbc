package schemacheck

import (
	"context"
	"reflect"

	"github.com/QuangTung97/dbc"
)

type ColumnTypeMatchFunc func(schemaType reflect.Type, dataType string) bool

type Validator struct {
	loader          TableLoader
	columnMatchFunc ColumnTypeMatchFunc
}

func NewValidator(
	loader TableLoader,
	columnMatchFunc ColumnTypeMatchFunc,
) *Validator {
	return &Validator{
		loader:          loader,
		columnMatchFunc: columnMatchFunc,
	}
}

func (v *Validator) ValidateSchemas(
	ctx context.Context, schemaList []dbc.SchemaInterface,
) error {
	_, err := v.loader.LoadAll(ctx)
	if err != nil {
		return err
	}

	return nil
}
