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
// For compound initial states, call LeafTarget(InitialState()) to get the
// deepest initial child.
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

// IsCompound reports whether the state has child states.
func (m *Machine[C]) IsCompound(id core.StateID) bool {
	s, ok := m.states[id]
	return ok && len(s.children) > 0
}

// Parent returns the parent state ID. Returns ("", false) for top-level states.
func (m *Machine[C]) Parent(id core.StateID) (core.StateID, bool) {
	s, ok := m.states[id]
	if !ok || s.parent == nil {
		return "", false
	}
	return s.parent.id, true
}

// ─── Transition resolution ────────────────────────────────────────────────────

// ResolveTransition finds the first enabled transition for the given event,
// walking up the ancestor chain if the leaf state has no matching handler
// (event bubbling). Guards are evaluated in definition order.
//
// Returns (target, actions, true) on success, ("", nil, false) if no
// transition matches anywhere in the ancestor chain.
func (m *Machine[C]) ResolveTransition(
	stateID core.StateID,
	ctx C,
	ev core.Event,
) (target core.StateID, actions []ActionFn[C], ok bool) {
	for s := m.states[stateID]; s != nil; s = s.parent {
		for _, ct := range s.transitions[ev.Type()] {
			if ct.guard == nil || ct.guard(ctx, ev) {
				return ct.target, ct.actions, true
			}
		}
	}
	return "", nil, false
}

// ResolveAlways finds the first enabled automatic (null) transition, walking
// up the ancestor chain. Evaluated after every step and on initial entry.
func (m *Machine[C]) ResolveAlways(
	stateID core.StateID,
	ctx C,
) (target core.StateID, actions []ActionFn[C], ok bool) {
	for s := m.states[stateID]; s != nil; s = s.parent {
		for _, ct := range s.always {
			if ct.guard == nil || ct.guard(ctx, AlwaysEvent{}) {
				return ct.target, ct.actions, true
			}
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

// ─── Hierarchical navigation ──────────────────────────────────────────────────

// LeafTarget follows initial children from id until reaching an atomic state.
// For atomic states, returns id unchanged. Used to find the active leaf when
// entering a compound state.
func (m *Machine[C]) LeafTarget(id core.StateID) core.StateID {
	for {
		s, ok := m.states[id]
		if !ok || len(s.children) == 0 {
			return id
		}
		id = s.initial
	}
}

// LCCA returns the Least Common Compound Ancestor of s1 and s2 — the deepest
// compound state that is a proper ancestor of both. Returns "" if no compound
// ancestor exists (both states are in different top-level branches).
func (m *Machine[C]) LCCA(s1, s2 core.StateID) core.StateID {
	// Collect proper ancestors of s1 (not including s1 itself).
	anc1 := make(map[core.StateID]bool)
	for s := m.states[s1]; s != nil && s.parent != nil; s = s.parent {
		anc1[s.parent.id] = true
	}
	// Walk up from s2 to find the deepest compound state that is also in anc1.
	for s := m.states[s2]; s != nil && s.parent != nil; s = s.parent {
		if anc1[s.parent.id] && len(m.states[s.parent.id].children) > 0 {
			return s.parent.id
		}
	}
	return "" // no compound common ancestor; use implicit root
}

// ExitPath returns the states to exit when leaving currentLeaf toward lcca,
// in innermost-first order (leaf first). Includes currentLeaf; excludes lcca.
// Pass lcca="" to exit all the way to the top level.
func (m *Machine[C]) ExitPath(currentLeaf, lcca core.StateID) []core.StateID {
	var path []core.StateID
	for id := currentLeaf; id != lcca; {
		path = append(path, id)
		s := m.states[id]
		if s.parent == nil {
			break
		}
		id = s.parent.id
	}
	return path
}

// EntryPath returns the states to enter when going from lcca down to target,
// in outermost-first order. Excludes lcca; follows initial children if target
// is compound, stopping at the resulting atomic leaf.
// Pass lcca="" to enter from the top level.
func (m *Machine[C]) EntryPath(lcca, target core.StateID) []core.StateID {
	leaf := m.LeafTarget(target)
	var path []core.StateID
	for id := leaf; id != lcca; {
		path = append(path, id)
		s := m.states[id]
		if s.parent == nil {
			break
		}
		id = s.parent.id
	}
	// Reverse for outermost-first order.
	for i, j := 0, len(path)-1; i < j; i, j = i+1, j-1 {
		path[i], path[j] = path[j], path[i]
	}
	return path
}

// Configuration returns the active configuration for the given leaf state:
// all states from the outermost ancestor down to the leaf, in
// outermost-first order. For flat machines this is always [leaf].
func (m *Machine[C]) Configuration(leaf core.StateID) []core.StateID {
	var path []core.StateID
	for s := m.states[leaf]; s != nil; s = s.parent {
		path = append(path, s.id)
	}
	for i, j := 0, len(path)-1; i < j; i, j = i+1, j-1 {
		path[i], path[j] = path[j], path[i]
	}
	return path
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
		seen := make(map[core.EventType]bool)
		for evType, cts := range s.transitions {
			if seen[evType] {
				continue
			}
			seen[evType] = true
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
		for _, ac := range s.afterConfs {
			out = append(out, TransitionInfo{
				From:     string(id),
				To:       string(ac.transition.target),
				Delay:    ac.delay,
				IsAfter:  true,
				HasGuard: ac.transition.guard != nil,
			})
		}
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
	TimerID   string
	Delay     time.Duration
	EventType core.EventType
}

// InvokeFns returns the invoke functions registered for a state.
func (m *Machine[C]) InvokeFns(id core.StateID) []InvokeFn[C] {
	if s, ok := m.states[id]; ok {
		return s.invokes
	}
	return nil
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

// AlwaysEvent is the synthetic event passed to guards and actions in
// always transitions.
type AlwaysEvent struct{}

func (AlwaysEvent) Type() core.EventType { return core.EventTypeAlways }

// ─── Internal compiled types ─────────────────────────────────────────────────

type compiledState[C any] struct {
	id          core.StateID
	parent      *compiledState[C]    // nil for top-level states
	children    []*compiledState[C]  // in definition order; empty for atomic states
	initial     core.StateID         // initial child (only set when len(children)>0)
	transitions map[core.EventType][]*compiledTransition[C]
	afterConfs  []*compiledAfter[C]
	always      []*compiledTransition[C]
	onEntry     []ActionFn[C]
	onExit      []ActionFn[C]
	invokes     []InvokeFn[C]
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
