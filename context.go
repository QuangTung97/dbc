package dbc

import "context"

var ctxKey = new(int)

type contextValueType struct {
	isReadonly bool
	isTransact bool // TODO remove
	tx         Transaction

	hookMap          map[*txHookKey]any
	beforeCommitList []func(ctx context.Context) error
	afterCommitList  []func(ctx context.Context)
}

func newContextValue(tx transactionWithRebind, isReadonly bool, isTransact bool) *contextValueType {
	return &contextValueType{
		isReadonly: isReadonly,
		isTransact: isTransact,
		tx: &autoRebindTransaction{
			base: tx,
		},
	}
}

func getFromContext(ctx context.Context) (*contextValueType, bool) {
	val, ok := ctx.Value(ctxKey).(*contextValueType)
	return val, ok
}

func setToContext(ctx context.Context, val *contextValueType) context.Context {
	return context.WithValue(ctx, ctxKey, val)
}

func (v *contextValueType) executeBeforeCommit(ctx context.Context) error {
	fnList := v.beforeCommitList
	v.beforeCommitList = nil

	// TODO idempotent

	for _, fn := range fnList {
		if err := fn(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (v *contextValueType) executeAfterCommit(ctx context.Context) {
	fnList := v.afterCommitList
	v.afterCommitList = nil

	// TODO idempotent

	for _, fn := range fnList {
		fn(ctx)
	}
}
