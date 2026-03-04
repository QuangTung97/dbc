package dbc

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFakeProvider_Transact(t *testing.T) {
	p := NewFakeProvider()

	var actions []string
	err := p.Transact(context.Background(), func(ctx context.Context) error {
		actions = append(actions, "transact")
		GetTx(ctx)
		return nil
	})
	assert.Equal(t, nil, err)
	assert.Equal(t, []string{"transact"}, actions)
}

func TestFakeProvider_Transact__With_Hook(t *testing.T) {
	p := NewFakeProvider()
	var actions []string

	hook := newTestHookWithInsertList(&actions)

	err := p.Transact(context.Background(), func(ctx context.Context) error {
		actions = append(actions, "transact")
		state := hook.Get(ctx)
		state.list = append(state.list, "insert01")
		return nil
	})
	assert.Equal(t, nil, err)
	assert.Equal(t, []string{
		"transact",
		"init",
		"before-commit:insert01",
		"after-commit:insert01",
	}, actions)
}

func TestFakeProvider_Transact__With_Hook__With_Error(t *testing.T) {
	p := NewFakeProvider()
	var actions []string

	hook := newTestHookWithInsertList(&actions)

	err := p.Transact(context.Background(), func(ctx context.Context) error {
		AssertIsTx(t, ctx)

		actions = append(actions, "transact")
		state := hook.Get(ctx)
		state.list = append(state.list, "insert01")
		return errors.New("test err")
	})
	assert.Equal(t, errors.New("test err"), err)
	assert.Equal(t, []string{
		"transact",
		"init",
	}, actions)
}

func TestFakeProvider_Readonly(t *testing.T) {
	p := NewFakeProvider()

	ctx := p.Readonly(context.Background())
	GetReadonly(ctx)

	AssertIsReadonly(t, ctx)

	val, ok := getFromContext(ctx)
	assert.Equal(t, true, ok)
	assert.Equal(t, true, val.isReadonly)
}

func TestFakeProvider_Autocommit(t *testing.T) {
	p := NewFakeProvider()

	ctx := p.Autocommit(context.Background())
	GetReadonly(ctx)
	GetTx(ctx)

	AssertIsAutocommit(t, ctx)

	val, ok := getFromContext(ctx)
	assert.Equal(t, true, ok)
	assert.Equal(t, false, val.isReadonly)
}
