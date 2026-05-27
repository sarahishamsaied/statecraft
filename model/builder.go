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

// Parallel declares a parallel (AND) state. All direct children are regions
// that run concurrently: every region is entered on entry and each region
// processes events independently. The machine stays in the parallel state
// until a transition exits it, or until all regions reach a final state.
func (b *Builder[C]) Parallel(id string, configure ...func(*StateBuilder[C])) *Builder[C] {
	sc := &stateConfig[C]{id: id, parallel: true}
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

// Always adds an automatic (null) transition that is evaluated after every
// event step — including initial state entry. The first Always whose guard
// passes fires immediately, without waiting for any external event.
//
// Always transitions are checked in definition order. A guardless Always
// must target a different state (compile-time enforced) to prevent an
// infinite loop. A guarded Always may target the same state only if the
// guard will eventually become false.
//
// Common pattern: routing states that redirect based on context.
//
//	s.Always("paid",    model.When[C](func(c C, _ core.Event) bool { return c.Balance >= c.Price }))
//	s.Always("overdue", model.When[C](func(c C, _ core.Event) bool { return c.DaysPastDue > 30 }))
//	s.Always("pending") // fallback — always fires if none above matched
func (s *StateBuilder[C]) Always(target string, opts ...TransitionOption[C]) *StateBuilder[C] {
	tc := transitionConfig[C]{target: target}
	for _, o := range opts {
		o(&tc)
	}
	s.cfg.always = append(s.cfg.always, tc)
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
// on its snapshot.
func (s *StateBuilder[C]) Final() *StateBuilder[C] {
	s.cfg.final = true
	return s
}

// Invoke registers a function to be called when this state is entered.
// The function runs immediately on entry and should spawn a goroutine for
// any blocking work. The context it receives is cancelled when the state is
// exited or the service is stopped, so the goroutine can clean up via ctx.Done().
// Multiple Invoke calls on the same state are all started on entry.
func (s *StateBuilder[C]) Invoke(fn InvokeFn[C]) *StateBuilder[C] {
	s.cfg.invokes = append(s.cfg.invokes, fn)
	return s
}

// State declares a child state inside this compound state.
// The first child declared is the default initial child unless Initial() is
// called to override it.
//
//	s.State("active", func(s *model.StateBuilder[C]) {
//	    s.Initial("idle")
//	    s.State("idle",    func(s *model.StateBuilder[C]) { s.On("START", "running") })
//	    s.State("running", func(s *model.StateBuilder[C]) { s.On("STOP", "idle") })
//	    s.On("CANCEL", "cancelled")  // parent handles events not handled by children
//	})
func (s *StateBuilder[C]) State(id string, configure ...func(*StateBuilder[C])) *StateBuilder[C] {
	child := &stateConfig[C]{id: id}
	if len(configure) > 0 {
		configure[0](&StateBuilder[C]{cfg: child})
	}
	s.cfg.children = append(s.cfg.children, child)
	return s
}

// Parallel declares a parallel (AND) child state inside this state.
// All direct children of the parallel state are concurrent regions.
func (s *StateBuilder[C]) Parallel(id string, configure ...func(*StateBuilder[C])) *StateBuilder[C] {
	child := &stateConfig[C]{id: id, parallel: true}
	if len(configure) > 0 {
		configure[0](&StateBuilder[C]{cfg: child})
	}
	s.cfg.children = append(s.cfg.children, child)
	return s
}

// Initial sets the initial child state for this compound state.
// If not called, the first child declared with State() is the initial child.
func (s *StateBuilder[C]) Initial(id string) *StateBuilder[C] {
	s.cfg.initialChild = id
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
