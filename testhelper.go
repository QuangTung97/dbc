package dbc

import (
	"context"
	"testing"
)

func NewFakeProvider() Provider {
	return &providerFake{}
}

type providerFake struct {
}

func (p *providerFake) Transact(inputCtx context.Context, fn func(ctx context.Context) error) error {
	val := newContextValue(nil, false)
	val.isTx = true
	ctx := setToContext(inputCtx, val)

	if err := fn(ctx); err != nil {
		return err
	}

	if err := val.executeBeforeCommit(ctx); err != nil {
		return err
	}

	val.executeAfterCommit(inputCtx)

	return nil
}

func (p *providerFake) Readonly(ctx context.Context) context.Context {
	return setToContext(ctx, newContextValue(nil, true))
}

func (p *providerFake) Autocommit(ctx context.Context) context.Context {
	val := newContextValue(nil, false)
	return setToContext(ctx, val)
}

func AssertIsTx(t *testing.T, ctx context.Context) {
	t.Helper()
	val, ok := getFromContext(ctx)
	if !ok || !val.isTx {
		t.Errorf("Missing provider.Transact() call")
	}
}

func AssertIsAutocommit(t *testing.T, ctx context.Context) {
	t.Helper()
	val, ok := getFromContext(ctx)
	if !ok || val.isReadonly {
		t.Errorf("Missing provider.Autocommit() call")
	}
}

func AssertIsReadonly(t *testing.T, ctx context.Context) {
	t.Helper()
	val, ok := getFromContext(ctx)
	if !ok || !val.isReadonly {
		t.Errorf("Missing provider.Readonly() call")
	}
}
