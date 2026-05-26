// Package model defines the machine definition layer: pure data structures with
// no goroutines, no channels, and no side effects. A compiled Machine may be
// shared safely across any number of concurrent Service instances.
package model

import (
	"fmt"
	"statecraft/core"
)

// ActionFn is the function signature for all state machine actions.
//
// It receives the current context and the triggering event, executes any
// work, and returns the updated context. To request side effects that
// the interpreter must handle (e.g. raising an internal event), write
// to ac. The ac parameter may be nil — check before use only if you
// need the ActionContext; the built-in helpers below handle this safely.
type ActionFn[C any] func(ctx C, event core.Event, ac *ActionContext) C

// ActionContext is the side-effect bus passed to every action execution.
// Actions write to it; the interpreter reads from it after the action
// sequence completes and processes the requests.
type ActionContext struct {
	raised []core.Event
}

// Raise schedules an internal event to be injected into the interpreter's
// internal queue after the current action sequence.
func (ac *ActionContext) Raise(ev core.Event) {
	ac.raised = append(ac.raised, ev)
}

// Drain returns all raised events and resets the slice.
// Called by the interpreter after each action sequence.
func (ac *ActionContext) Drain() []core.Event {
	r := ac.raised
	ac.raised = nil
	return r
}

// Assign creates a pure context-mutation action from a simple function.
// The function receives the current context and event, and returns the
// next context. It must not produce side effects.
//
//	Assign(func(ctx MyCtx, e core.Event) MyCtx {
//	    ctx.Count++
//	    return ctx
//	})
func Assign[C any](fn func(C, core.Event) C) ActionFn[C] {
	return func(ctx C, event core.Event, _ *ActionContext) C {
		return fn(ctx, event)
	}
}

// Log creates a side-effect-only action that prints a line to stdout.
// The context is returned unchanged. Intended for debugging; replace with
// a structured logger in production by writing a custom ActionFn.
func Log[C any](fn func(C, core.Event) string) ActionFn[C] {
	return func(ctx C, event core.Event, _ *ActionContext) C {
		fmt.Println(fn(ctx, event))
		return ctx
	}
}

// Raise creates an action that injects an internal event immediately after
// the current action sequence. The raised event is processed before the
// next external event from the mailbox.
func Raise[C any](fn func(C, core.Event) core.Event) ActionFn[C] {
	return func(ctx C, event core.Event, ac *ActionContext) C {
		if ac != nil {
			ac.Raise(fn(ctx, event))
		}
		return ctx
	}
}
