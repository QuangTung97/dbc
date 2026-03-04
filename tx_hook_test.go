package dbc

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

type hookStateTest struct {
	list []string
}

func newTestHookWithInsertList(actions *[]string) *TxHook[hookStateTest] {
	return NewTxHook[hookStateTest](
		func() *hookStateTest {
			*actions = append(*actions, "init")
			return &hookStateTest{}
		},
		func(ctx context.Context, state *hookStateTest) error {
			*actions = append(*actions, "before-commit:"+strings.Join(state.list, ","))
			return nil
		},
		func(ctx context.Context, state *hookStateTest) {
			*actions = append(*actions, "after-commit:"+strings.Join(state.list, ","))
		},
	)
}

func TestTxHook_Normal(t *testing.T) {
	var actions []string
	hook := newTestHookWithInsertList(&actions)

	ctx := setToContext(
		context.Background(),
		newContextValue(nil, false),
	)
	state := hook.Get(ctx)
	state.list = append(state.list, "insert01")
	state.list = append(state.list, "insert02")

	// check get again
	state2 := hook.Get(ctx)
	assert.Same(t, state, state2)

	// execute before commit
	err := ExecuteBeforeCommitHooks(ctx)
	assert.Equal(t, nil, err)

	// check actions
	assert.Equal(t, []string{
		"init",
		"before-commit:insert01,insert02",
	}, actions)

	// execute again
	err = ExecuteBeforeCommitHooks(ctx)
	assert.Equal(t, nil, err)

	// execute after commit
	ExecuteAfterCommitHooks(ctx)
	ExecuteAfterCommitHooks(ctx)

	// check actions
	assert.Equal(t, []string{
		"init",
		"before-commit:insert01,insert02",
		"after-commit:insert01,insert02",
	}, actions)
}
