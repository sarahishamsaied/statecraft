package model

import (
	"context"
	"statecraft/core"
)

// InvokeFn[C] starts an async side-effect when a state is entered.
//
// The ctx is cancelled automatically when the state is exited or the service
// is stopped — watch ctx.Done() to clean up. machineCtx is a snapshot of the
// machine context at entry time. send delivers events back to the machine's
// mailbox (goroutine-safe for the runtime; synchronous-only for Harness).
//
// InvokeFn must return promptly. Any blocking work must run in a goroutine:
//
//	s.Invoke(func(ctx context.Context, c MyCtx, ev core.Event, send func(core.Event)) {
//	    go func() {
//	        result, err := fetchData(ctx, c.ID)
//	        if err != nil {
//	            send(core.E("FETCH_ERROR"))
//	            return
//	        }
//	        send(ResultEvent{Data: result})
//	    }()
//	})
type InvokeFn[C any] func(ctx context.Context, machineCtx C, ev core.Event, send func(core.Event))
