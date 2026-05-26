package model

import (
	"fmt"
	"time"
)

// Builder constructs a flat machine definition using a fluent API.
// Invoke Build() or MustBuild() to compile and validate.
//
//	m := model.New[MyCtx]("my-machine").
//	    Context(MyCtx{}).
//	    Initial("idle").
//	    State("idle", func(s *model.StateBuilder[MyCtx]) {
//	        s.On("START", "running")
//	    }).
//	    State("running", func(s *model.StateBuilder[MyCtx]) {
//	        s.On("STOP", "idle")
//	    }).
//	    MustBuild()
type Builder[C any] struct {
	cfg *machineConfig[C]
}

// New creates a new Builder for a machine with the given ID.
// C is the context type; set its initial value with .Context().
func New[C any](id string) *Builder[C] {
	return &Builder[C]{cfg: &machineConfig[C]{id: id}}
}

// Context sets the initial context value for every new Service started from
// this machine. The value is copied by value — use a pointer type if you
// need reference semantics (but be aware of concurrent access).
func (b *Builder[C]) Context(initial C) *Builder[C] {
	b.cfg.context = initial
	return b
}

// Initial declares which state is active when the machine starts.
func (b *Builder[C]) Initial(stateID string) *Builder[C] {
	b.cfg.initial = stateID
	return b
}

// State declares a state. The optional configure callback receives a
// StateBuilder to add transitions, actions, and timers.
func (b *Builder[C]) State(id string, configure ...func(*StateBuilder[C])) *Builder[C] {
	sc := &stateConfig[C]{id: id}
	if len(configure) > 0 {
		configure[0](&StateBuilder[C]{cfg: sc})
	}
	b.cfg.states = append(b.cfg.states, sc)
	return b
}

// Build compiles and validates the machine. Returns an error if the
// definition is invalid (unknown targets, missing initial state, etc.).
func (b *Builder[C]) Build() (*Machine[C], error) {
	return compile(b.cfg)
}

// MustBuild compiles the machine and panics on any validation error.
// Suitable for package-level variable initialisation where a bad definition
// is a programming error, not a runtime condition.
func (b *Builder[C]) MustBuild() *Machine[C] {
	m, err := b.Build()
	if err != nil {
		panic(fmt.Sprintf("statecraft: machine %q build failed: %v", b.cfg.id, err))
	}
	return m
}

// ─── StateBuilder ────────────────────────────────────────────────────────────

// StateBuilder configures a single state inside the Builder.State() callback.
type StateBuilder[C any] struct {
	cfg *stateConfig[C]
}

// On adds a transition triggered by the named event type.
// If multiple On calls share the same event type, they form a guarded chain:
// the interpreter tries each in definition order and takes the first whose
// guard passes. Without a guard, a transition always matches.
//
//	s.On("SUBMIT", "reviewing")
//	s.On("SUBMIT", "error", model.When[C](isInvalid))
func (s *StateBuilder[C]) On(event, target string, opts ...TransitionOption[C]) *StateBuilder[C] {
	tc := transitionConfig[C]{event: event, target: target}
	for _, o := range opts {
		o(&tc)
	}
	s.cfg.transitions = append(s.cfg.transitions, tc)
	return s
}

// After adds a delayed transition that fires after duration d if the machine
// is still in this state. Timers are scoped to the state: exiting the state
// cancels the timer automatically, preventing stale timer events.
func (s *StateBuilder[C]) After(d time.Duration, target string, opts ...TransitionOption[C]) *StateBuilder[C] {
	ac := afterStateConfig[C]{delay: d, target: target}
	// Options are borrowed from the transition option set.
	for _, o := range opts {
		tc := transitionConfig[C]{}
		o(&tc)
		ac.guard = tc.guard
		ac.actions = append(ac.actions, tc.actions...)
	}
	s.cfg.afterConfs = append(s.cfg.afterConfs, ac)
	return s
}

// Entry registers one or more actions to run on state entry.
// Actions execute in the order they are registered.
func (s *StateBuilder[C]) Entry(actions ...ActionFn[C]) *StateBuilder[C] {
	s.cfg.onEntry = append(s.cfg.onEntry, actions...)
	return s
}

// Exit registers one or more actions to run on state exit.
func (s *StateBuilder[C]) Exit(actions ...ActionFn[C]) *StateBuilder[C] {
	s.cfg.onExit = append(s.cfg.onExit, actions...)
	return s
}

// Final marks this state as terminal. When the machine enters a final state
// it keeps running (can still receive events) but exposes IsFinal() == true
// on its snapshot. Phase 3 will propagate a done event to parent actors.
func (s *StateBuilder[C]) Final() *StateBuilder[C] {
	s.cfg.final = true
	return s
}

// ─── TransitionOption ────────────────────────────────────────────────────────

// TransitionOption modifies a transition during its declaration.
type TransitionOption[C any] func(*transitionConfig[C])

// When attaches a guard predicate to the transition.
// The transition is only selected when the guard returns true.
func When[C any](guard GuardFn[C]) TransitionOption[C] {
	return func(tc *transitionConfig[C]) { tc.guard = guard }
}

// Do attaches actions to the transition. They run after exit actions and
// before entry actions, in the order provided.
func Do[C any](actions ...ActionFn[C]) TransitionOption[C] {
	return func(tc *transitionConfig[C]) {
		tc.actions = append(tc.actions, actions...)
	}
}
