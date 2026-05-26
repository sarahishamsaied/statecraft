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

	// ── Pass 1: collect all declared state IDs and check for duplicates ────
	knownIDs := make(map[string]struct{}, len(cfg.states))
	for _, sc := range cfg.states {
		if sc.id == "" {
			return nil, fmt.Errorf("%w: state ID must not be empty", core.ErrInvalidMachine)
		}
		if _, dup := knownIDs[sc.id]; dup {
			return nil, fmt.Errorf("%w: %q", core.ErrDuplicateState, sc.id)
		}
		knownIDs[sc.id] = struct{}{}
	}

	// ── Validate initial state reference ───────────────────────────────────
	if _, ok := knownIDs[cfg.initial]; !ok {
		return nil, fmt.Errorf("%w: initial state %q not declared",
			core.ErrUnknownState, cfg.initial)
	}

	// ── Pass 2: compile each state ─────────────────────────────────────────
	compiled := make(map[core.StateID]*compiledState[C], len(cfg.states))
	order := make([]core.StateID, 0, len(cfg.states))

	for _, sc := range cfg.states {
		cs, err := compileState(sc, knownIDs)
		if err != nil {
			return nil, err
		}
		id := core.StateID(sc.id)
		compiled[id] = cs
		order = append(order, id)
	}

	return &Machine[C]{
		id:      cfg.id,
		initial: core.StateID(cfg.initial),
		context: cfg.context,
		states:  compiled,
		order:   order,
	}, nil
}

func compileState[C any](
	sc *stateConfig[C],
	knownIDs map[string]struct{},
) (*compiledState[C], error) {

	cs := &compiledState[C]{
		id:          core.StateID(sc.id),
		transitions: make(map[core.EventType][]*compiledTransition[C]),
		onEntry:     sc.onEntry,
		onExit:      sc.onExit,
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
	// Each After(d, target) gets a unique synthetic event type so that the
	// timer callback can trigger it via the normal transition resolution path
	// without any special-casing in the interpreter.
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

		// Register as a normal transition so ResolveTransition finds it.
		cs.transitions[evType] = append(cs.transitions[evType], ct)
	}

	return cs, nil
}

// afterEventType returns the synthetic EventType for a given timer ID.
// The prefix ensures it can never collide with user-defined event names.
func afterEventType(timerID string) core.EventType {
	return core.EventType("statecraft.after." + timerID)
}
