// Package statecraft is a concurrency-first statechart runtime for Go.
//
// Phase 1 provides a flat FSM runtime with typed context, guards, actions,
// state-scoped timers, and a correct internal-event-priority event loop.
//
// Quick start:
//
//	type TrafficCtx struct{ Cycles int }
//
//	m := statecraft.New[TrafficCtx]("traffic-light").
//	    Initial("red").
//	    State("red",   func(s *statecraft.StateBuilder[TrafficCtx]) { s.On("TIMER", "green") }).
//	    State("green", func(s *statecraft.StateBuilder[TrafficCtx]) { s.On("TIMER", "yellow") }).
//	    State("yellow",func(s *statecraft.StateBuilder[TrafficCtx]) { s.On("TIMER", "red") }).
//	    MustBuild()
//
//	svc := statecraft.Start(m)
//	defer svc.Stop()
//
//	svc.Send(statecraft.E("TIMER"))
//	fmt.Println(svc.State()) // "green"
package statecraft

import (
	"statecraft/core"
	"statecraft/model"
	"statecraft/runtime"
)

// ─── Re-exported types ────────────────────────────────────────────────────────

// Event is the core interface for all events sent to a state machine.
type Event = core.Event

// EventType is the string discriminant carried by every Event.
type EventType = core.EventType

// StateID is a state identifier.
type StateID = core.StateID

// Snapshot is an immutable point-in-time view of a running service.
type Snapshot[C any] = runtime.Snapshot[C]

// UnsubscribeFn cancels a subscription when called.
type UnsubscribeFn = runtime.UnsubscribeFn

// ActionFn is the function signature for state machine actions.
type ActionFn[C any] = model.ActionFn[C]

// ActionContext is passed to actions for requesting side effects.
type ActionContext = model.ActionContext

// GuardFn is a pure predicate evaluated during transition selection.
type GuardFn[C any] = model.GuardFn[C]

// Builder constructs a machine definition.
type Builder[C any] = model.Builder[C]

// StateBuilder configures a single state inside Builder.State().
type StateBuilder[C any] = model.StateBuilder[C]

// TransitionOption modifies a transition at declaration time.
type TransitionOption[C any] = model.TransitionOption[C]

// Machine is a compiled, immutable machine definition.
type Machine[C any] = model.Machine[C]

// Service is a running instance of a Machine.
type Service[C any] = runtime.Service[C]

// ─── Constructor functions ────────────────────────────────────────────────────

// New creates a machine Builder for context type C.
//
//	m := statecraft.New[MyCtx]("my-machine").
//	    Context(MyCtx{}).
//	    Initial("idle").
//	    State("idle", func(s *statecraft.StateBuilder[MyCtx]) { ... }).
//	    MustBuild()
func New[C any](id string) *model.Builder[C] {
	return model.New[C](id)
}

// Start creates a running Service from a compiled Machine.
// The interpreter goroutine begins immediately; the service is ready to
// receive events before Start() returns.
func Start[C any](m *model.Machine[C], opts ...func(*runtime.ServiceOptions)) *runtime.Service[C] {
	return runtime.Start(m, opts...)
}

// E creates a simple event with the given type name and no payload.
// For events with payloads, define a concrete struct that implements Event.
func E(t string) core.Event { return core.E(t) }

// ─── Action creators ──────────────────────────────────────────────────────────

// Assign creates a pure context-mutation action.
func Assign[C any](fn func(C, core.Event) C) model.ActionFn[C] {
	return model.Assign(fn)
}

// Log creates a stdout-logging action (context unchanged).
func Log[C any](fn func(C, core.Event) string) model.ActionFn[C] {
	return model.Log(fn)
}

// Raise creates an action that injects an internal event.
func Raise[C any](fn func(C, core.Event) core.Event) model.ActionFn[C] {
	return model.Raise(fn)
}

// ─── Guard combinators ────────────────────────────────────────────────────────

// When attaches a guard predicate to a transition.
func When[C any](guard model.GuardFn[C]) model.TransitionOption[C] {
	return model.When(guard)
}

// Do attaches actions to a transition.
func Do[C any](actions ...model.ActionFn[C]) model.TransitionOption[C] {
	return model.Do(actions...)
}

// And returns a guard that passes only when all provided guards pass.
func And[C any](guards ...model.GuardFn[C]) model.GuardFn[C] {
	return model.And(guards...)
}

// Or returns a guard that passes when any provided guard passes.
func Or[C any](guards ...model.GuardFn[C]) model.GuardFn[C] {
	return model.Or(guards...)
}

// Not inverts a guard.
func Not[C any](guard model.GuardFn[C]) model.GuardFn[C] {
	return model.Not(guard)
}

// ─── Runtime options ──────────────────────────────────────────────────────────

// WithMailboxSize sets the mailbox buffer size (default: 256).
func WithMailboxSize(n int) func(*runtime.ServiceOptions) {
	return runtime.WithMailboxSize(n)
}

// WithClock injects a custom Clock for deterministic timer testing.
func WithClock(c core.Clock) func(*runtime.ServiceOptions) {
	return runtime.WithClock(c)
}
