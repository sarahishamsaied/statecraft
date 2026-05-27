package model

import (
	"fmt"
	"statecraft/core"
)

// compile transforms a raw machineConfig into an immutable, validated Machine.
// All transition targets are resolved and all timer event types pre-registered.
// Any invalid reference fails fast here — never at runtime.
func compile[C any](cfg *machineConfig[C]) (*Machine[C], error) {
	if cfg.id == "" {
		return nil, fmt.Errorf("%w: machine ID must not be empty", core.ErrInvalidMachine)
	}
	if len(cfg.states) == 0 {
		return nil, fmt.Errorf("%w: at least one state must be declared", core.ErrInvalidMachine)
	}
	if cfg.initial == "" {
		return nil, fmt.Errorf("%w: call .Initial(stateID) before Build()", core.ErrNoInitialState)
	}

	// ── Pass 1: collect ALL state IDs (including nested) and check duplicates ──
	knownIDs := make(map[string]struct{})
	if err := collectStateIDs(cfg.states, knownIDs); err != nil {
		return nil, err
	}

	// Validate initial state reference.
	if _, ok := knownIDs[cfg.initial]; !ok {
		return nil, fmt.Errorf("%w: initial state %q not declared",
			core.ErrUnknownState, cfg.initial)
	}

	// ── Pass 2: compile each state tree (DFS) ─────────────────────────────────
	compiled := make(map[core.StateID]*compiledState[C], len(knownIDs))
	order := make([]core.StateID, 0, len(knownIDs))

	for _, sc := range cfg.states {
		if err := compileTree(sc, nil, knownIDs, compiled, &order); err != nil {
			return nil, err
		}
	}

	return &Machine[C]{
		id:      cfg.id,
		initial: core.StateID(cfg.initial),
		context: cfg.context,
		states:  compiled,
		order:   order,
	}, nil
}

// collectStateIDs recursively collects all state IDs and checks for duplicates.
func collectStateIDs[C any](states []*stateConfig[C], known map[string]struct{}) error {
	for _, sc := range states {
		if sc.id == "" {
			return fmt.Errorf("%w: state ID must not be empty", core.ErrInvalidMachine)
		}
		if _, dup := known[sc.id]; dup {
			return fmt.Errorf("%w: %q", core.ErrDuplicateState, sc.id)
		}
		known[sc.id] = struct{}{}
		if err := collectStateIDs(sc.children, known); err != nil {
			return err
		}
	}
	return nil
}

// compileTree compiles sc and all its descendants into compiled/order.
// parent is nil for top-level states.
func compileTree[C any](
	sc *stateConfig[C],
	parent *compiledState[C],
	knownIDs map[string]struct{},
	compiled map[core.StateID]*compiledState[C],
	order *[]core.StateID,
) error {
	cs, err := compileNode(sc, parent, knownIDs)
	if err != nil {
		return err
	}
	compiled[cs.id] = cs
	*order = append(*order, cs.id)

	for _, child := range sc.children {
		if err := compileTree(child, cs, knownIDs, compiled, order); err != nil {
			return err
		}
		cs.children = append(cs.children, compiled[core.StateID(child.id)])
	}

	// Set initial child after all children are compiled.
	if len(cs.children) > 0 {
		if sc.initialChild != "" {
			found := false
			for _, c := range cs.children {
				if string(c.id) == sc.initialChild {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("%w: Initial(%q) is not a direct child of state %q",
					core.ErrInvalidMachine, sc.initialChild, sc.id)
			}
			cs.initial = core.StateID(sc.initialChild)
		} else {
			cs.initial = cs.children[0].id // default: first declared child
		}
	}

	return nil
}

// compileNode compiles a single state node (no children) into a compiledState.
func compileNode[C any](
	sc *stateConfig[C],
	parent *compiledState[C],
	knownIDs map[string]struct{},
) (*compiledState[C], error) {
	cs := &compiledState[C]{
		id:          core.StateID(sc.id),
		parent:      parent,
		transitions: make(map[core.EventType][]*compiledTransition[C]),
		onEntry:     sc.onEntry,
		onExit:      sc.onExit,
		invokes:     sc.invokes,
		final:       sc.final,
	}

	// ── Regular transitions ─────────────────────────────────────────────────
	for i, tc := range sc.transitions {
		if tc.target == "" {
			return nil, fmt.Errorf("%w: transition %d in state %q has no target",
				core.ErrInvalidMachine, i, sc.id)
		}
		if _, ok := knownIDs[tc.target]; !ok {
			return nil, fmt.Errorf("%w: state %q transition %d → %q (undefined)",
				core.ErrUnknownTarget, sc.id, i, tc.target)
		}
		ct := &compiledTransition[C]{
			target:  core.StateID(tc.target),
			guard:   tc.guard,
			actions: tc.actions,
		}
		evType := core.EventType(tc.event)
		cs.transitions[evType] = append(cs.transitions[evType], ct)
	}

	// ── After-transitions ───────────────────────────────────────────────────
	for i, asc := range sc.afterConfs {
		if asc.delay <= 0 {
			return nil, fmt.Errorf("%w: after-transition %d in state %q must have positive delay",
				core.ErrInvalidMachine, i, sc.id)
		}
		if asc.target == "" {
			return nil, fmt.Errorf("%w: after-transition %d in state %q has no target",
				core.ErrInvalidMachine, i, sc.id)
		}
		if _, ok := knownIDs[asc.target]; !ok {
			return nil, fmt.Errorf("%w: state %q after-transition %d → %q (undefined)",
				core.ErrUnknownTarget, sc.id, i, asc.target)
		}

		timerID := fmt.Sprintf("%s.after[%d]", sc.id, i)
		evType := afterEventType(timerID)

		ct := &compiledTransition[C]{
			target:  core.StateID(asc.target),
			guard:   asc.guard,
			actions: asc.actions,
		}
		after := &compiledAfter[C]{
			delay:      asc.delay,
			timerID:    timerID,
			transition: ct,
		}
		cs.afterConfs = append(cs.afterConfs, after)
		cs.transitions[evType] = append(cs.transitions[evType], ct)
	}

	// ── Always transitions ──────────────────────────────────────────────────
	for i, tc := range sc.always {
		if tc.target == "" {
			return nil, fmt.Errorf("%w: always-transition %d in state %q has no target",
				core.ErrInvalidMachine, i, sc.id)
		}
		if _, ok := knownIDs[tc.target]; !ok {
			return nil, fmt.Errorf("%w: state %q always-transition %d → %q (undefined)",
				core.ErrUnknownTarget, sc.id, i, tc.target)
		}
		if tc.target == sc.id && tc.guard == nil {
			return nil, fmt.Errorf(
				"%w: state %q always-transition %d targets itself with no guard — infinite loop",
				core.ErrInvalidMachine, sc.id, i)
		}
		ct := &compiledTransition[C]{
			target:  core.StateID(tc.target),
			guard:   tc.guard,
			actions: tc.actions,
		}
		cs.always = append(cs.always, ct)
	}

	return cs, nil
}

// afterEventType returns the synthetic EventType for a given timer ID.
func afterEventType(timerID string) core.EventType {
	return core.EventType("statecraft.after." + timerID)
}
