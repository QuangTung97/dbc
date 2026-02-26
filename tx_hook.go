package dbc

import "context"

type txHookKey struct{}

type TxHook[S any] struct {
	initFunc         func() *S
	beforeCommitFunc func(ctx context.Context, state *S) error
	afterCommitFunc  func(ctx context.Context, state *S)

	key *txHookKey
}

func NewTxHook[S any](
	initFunc func() *S,
	beforeCommit func(ctx context.Context, state *S) error,
	afterCommit func(ctx context.Context, state *S),
) *TxHook[S] {
	return &TxHook[S]{
		initFunc:         initFunc,
		beforeCommitFunc: beforeCommit,
		afterCommitFunc:  afterCommit,

		key: &txHookKey{},
	}
}

func (h *TxHook[S]) Get(ctx context.Context) *S {
	val, ok := getFromContext(ctx)
	if !ok {
		panic("TxHook.Get() must be run inside a transaction")
	}
	if val.isReadonly {
		panic("TxHook.Get() must be run inside a transaction")
	}

	// init on demand
	if val.hookMap == nil {
		val.hookMap = map[*txHookKey]any{}
	}

	prevState, ok := val.hookMap[h.key]
	if ok {
		return prevState.(*S)
	}

	state := h.initFunc()
	val.hookMap[h.key] = state
	val.beforeCommitList = append(val.beforeCommitList, func(ctx context.Context) error {
		return h.beforeCommitFunc(ctx, state)
	})
	val.afterCommitList = append(val.afterCommitList, func(ctx context.Context) {
		h.afterCommitFunc(ctx, state)
	})
	return state
}
