package model

import (
	"statecraft/core"
	"time"
)

// Machine is a compiled, immutable machine definition.
// It carries zero runtime state; create as many concurrent Service instances
// from it as you like.
type Machine[C any] struct {
	id      string
	initial core.StateID
	context C
	states  map[core.StateID]*compiledState[C]
	order   []core.StateID // definition order — used for deterministic iteration
}

// ID returns the machine's identifier.
func (m *Machine[C]) ID() string { return m.id }

// InitialState returns the ID of the state the machine starts in.
func (m *Machine[C]) InitialState() core.StateID { return m.initial }

// InitialContext returns a copy of the initial context value.
func (m *Machine[C]) InitialContext() C { return m.context }

// StateIDs returns all declared state IDs in definition order.
func (m *Machine[C]) StateIDs() []core.StateID {
	out := make([]core.StateID, len(m.order))
	copy(out, m.order)
	return out
}

// Has reports whether the given state ID exists in the machine.
func (m *Machine[C]) Has(id core.StateID) bool {
	_, ok := m.states[id]
	return ok
}

// IsFinal reports whether the given state is marked as terminal.
func (m *Machine[C]) IsFinal(id core.StateID) bool {
	s, ok := m.states[id]
	return ok && s.final
}

// ResolveTransition finds the first enabled transition for the current state
// and event. Guards are evaluated in definition order; the first passing
// transition is returned.
//
// Returns (target, actions, true) on success, ("", nil, false) if no
// transition matches (the event is silently ignored by the interpreter).
func (m *Machine[C]) ResolveTransition(
	stateID core.StateID,
	ctx C,
	ev core.Event,
) (target core.StateID, actions []ActionFn[C], ok bool) {
	s, exists := m.states[stateID]
	if !exists {
		return "", nil, false
	}
	for _, ct := range s.transitions[ev.Type()] {
		if ct.guard == nil || ct.guard(ctx, ev) {
			return ct.target, ct.actions, true
		}
	}
	return "", nil, false
}

// EntryActions returns the entry actions registered for a state.
func (m *Machine[C]) EntryActions(id core.StateID) []ActionFn[C] {
	if s, ok := m.states[id]; ok {
		return s.onEntry
	}
	return nil
}

// ExitActions returns the exit actions registered for a state.
func (m *Machine[C]) ExitActions(id core.StateID) []ActionFn[C] {
	if s, ok := m.states[id]; ok {
		return s.onExit
	}
	return nil
}

// ─── Inspection API (used by viz package) ────────────────────────────────────

// TransitionInfo describes a single transition for visualization purposes.
type TransitionInfo struct {
	From     string
	To       string
	Event    string        // "" for after/always transitions
	Delay    time.Duration // non-zero for after-transitions
	IsAfter  bool
	IsAlways bool
	HasGuard bool
}

// Transitions returns all transitions across all states in definition order.
// Useful for generating diagrams and documentation.
func (m *Machine[C]) Transitions() []TransitionInfo {
	var out []TransitionInfo
	for _, id := range m.order {
		s := m.states[id]
		// Regular transitions, grouped by event type.
		// We collect in a stable order: iterate over all event types found.
		seen := make(map[core.EventType]bool)
		for evType, cts := range s.transitions {
			if seen[evType] {
				continue
			}
			seen[evType] = true
			// Skip synthetic after-event types.
			if isAfterEvent(evType) {
				continue
			}
			for _, ct := range cts {
				out = append(out, TransitionInfo{
					From:     string(id),
					To:       string(ct.target),
					Event:    string(evType),
					HasGuard: ct.guard != nil,
				})
			}
		}
		// After-transitions.
		for _, ac := range s.afterConfs {
			out = append(out, TransitionInfo{
				From:     string(id),
				To:       string(ac.transition.target),
				Delay:    ac.delay,
				IsAfter:  true,
				HasGuard: ac.transition.guard != nil,
			})
		}
		// Always transitions.
		for _, ct := range s.always {
			out = append(out, TransitionInfo{
				From:     string(id),
				To:       string(ct.target),
				IsAlways: true,
				HasGuard: ct.guard != nil,
			})
		}
	}
	return out
}

func isAfterEvent(et core.EventType) bool {
	return len(et) > 16 && et[:16] == "statecraft.after"
}

// AfterConf describes a single timer-based transition on a state.
type AfterConf[C any] struct {
	// TimerID is a unique identifier scoped to the state, used to start and
	// cancel the underlying timer.
	TimerID string
	// Delay is the duration after state entry before the event fires.
	Delay time.Duration
	// EventType is the synthetic event type generated when the timer fires.
	// It is pre-registered as a transition in the compiled state.
	EventType core.EventType
}

// AfterConfs returns all timer configurations registered for a state.
func (m *Machine[C]) AfterConfs(id core.StateID) []AfterConf[C] {
	s, ok := m.states[id]
	if !ok {
		return nil
	}
	out := make([]AfterConf[C], len(s.afterConfs))
	for i, ac := range s.afterConfs {
		out[i] = AfterConf[C]{
			TimerID:   ac.timerID,
			Delay:     ac.delay,
			EventType: afterEventType(ac.timerID),
		}
	}
	return out
}

// ResolveAlways finds the first enabled automatic transition for the current
// state. Always transitions are evaluated after every step, in definition order.
// Returns (target, actions, true) when one matches, ("", nil, false) otherwise.
func (m *Machine[C]) ResolveAlways(
	stateID core.StateID,
	ctx C,
) (target core.StateID, actions []ActionFn[C], ok bool) {
	s, exists := m.states[stateID]
	if !exists {
		return "", nil, false
	}
	for _, ct := range s.always {
		if ct.guard == nil || ct.guard(ctx, AlwaysEvent{}) {
			return ct.target, ct.actions, true
		}
	}
	return "", nil, false
}

// AlwaysEvent is the synthetic event passed to guards and actions in
// always transitions. It carries no user-visible payload.
// Guards and actions can detect it via type assertion when needed.
type AlwaysEvent struct{}

func (AlwaysEvent) Type() core.EventType { return core.EventTypeAlways }

// ─── Internal compiled types ─────────────────────────────────────────────────

type compiledState[C any] struct {
	id          core.StateID
	transitions map[core.EventType][]*compiledTransition[C]
	afterConfs  []*compiledAfter[C]
	always      []*compiledTransition[C] // null/automatic transitions
	onEntry     []ActionFn[C]
	onExit      []ActionFn[C]
	final       bool
}

type compiledTransition[C any] struct {
	target  core.StateID
	guard   GuardFn[C]
	actions []ActionFn[C]
}

type compiledAfter[C any] struct {
	delay      time.Duration
	timerID    string
	transition *compiledTransition[C]
}
