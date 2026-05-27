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

// IsCompound reports whether the state has child states (OR or AND).
func (m *Machine[C]) IsCompound(id core.StateID) bool {
	s, ok := m.states[id]
	return ok && len(s.children) > 0
}

// IsParallel reports whether the state is a parallel (AND) state.
func (m *Machine[C]) IsParallel(id core.StateID) bool {
	s, ok := m.states[id]
	return ok && s.parallel
}

// Depth returns the nesting depth of a state: 0 for top-level states,
// 1 for their direct children, etc. Used to sort exit sets innermost-first.
func (m *Machine[C]) Depth(id core.StateID) int {
	depth := 0
	for s := m.states[id]; s != nil && s.parent != nil; s = s.parent {
		depth++
	}
	return depth
}

// Parent returns the parent state ID. Returns ("", false) for top-level states.
func (m *Machine[C]) Parent(id core.StateID) (core.StateID, bool) {
	s, ok := m.states[id]
	if !ok || s.parent == nil {
		return "", false
	}
	return s.parent.id, true
}

// Children returns the direct child state IDs in definition order.
// Returns nil for atomic states.
func (m *Machine[C]) Children(id core.StateID) []core.StateID {
	s, ok := m.states[id]
	if !ok || len(s.children) == 0 {
		return nil
	}
	out := make([]core.StateID, len(s.children))
	for i, c := range s.children {
		out[i] = c.id
	}
	return out
}

// InitialChild returns the initial child of a compound OR state.
// Returns ("", false) for atomic states and parallel states.
func (m *Machine[C]) InitialChild(id core.StateID) (core.StateID, bool) {
	s, ok := m.states[id]
	if !ok || len(s.children) == 0 || s.parallel {
		return "", false
	}
	return s.initial, true
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
// For atomic states, returns id unchanged. For compound OR states, follows
// the initial child chain. For parallel states, follows the first region's
// initial child (use LeafTargets for all regions).
func (m *Machine[C]) LeafTarget(id core.StateID) core.StateID {
	for {
		s, ok := m.states[id]
		if !ok || len(s.children) == 0 {
			return id
		}
		if s.parallel {
			// Follow first region for single-leaf compat; callers that need
			// all regions must use LeafTargets.
			id = s.children[0].id
		} else {
			id = s.initial
		}
	}
}

// LeafTargets returns all initial leaf states reachable from id.
// For atomic states: [id].
// For compound OR states: the single leaf via the initial child chain.
// For parallel states: one leaf per region (in definition order).
func (m *Machine[C]) LeafTargets(id core.StateID) []core.StateID {
	s, ok := m.states[id]
	if !ok || len(s.children) == 0 {
		return []core.StateID{id}
	}
	if s.parallel {
		var result []core.StateID
		for _, child := range s.children {
			result = append(result, m.LeafTargets(child.id)...)
		}
		return result
	}
	return m.LeafTargets(s.initial)
}

// InitialLeaves returns all active leaf states when the machine first starts.
func (m *Machine[C]) InitialLeaves() []core.StateID {
	return m.LeafTargets(m.initial)
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

// ConfigurationOf returns the active configuration for a set of leaf states
// (used when parallel regions are active). All ancestors of every leaf are
// included, deduplicated, and returned in definition order (outermost first).
func (m *Machine[C]) ConfigurationOf(leaves []core.StateID) []core.StateID {
	if len(leaves) == 1 {
		return m.Configuration(leaves[0])
	}
	seen := make(map[core.StateID]bool)
	for _, leaf := range leaves {
		for s := m.states[leaf]; s != nil; s = s.parent {
			seen[s.id] = true
		}
	}
	var result []core.StateID
	for _, id := range m.order {
		if seen[id] {
			result = append(result, id)
		}
	}
	return result
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
	initial     core.StateID         // initial child for OR compound states
	parallel    bool                 // AND-node: all children active simultaneously
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
