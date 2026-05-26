package core

// StateID is a fully-qualified state identifier.
// In Phase 1 (flat machines) this is just the local name.
// In Phase 4 (hierarchical) it becomes a dot-separated path: "active.connected".
type StateID string

// StateType classifies a node in the statechart tree.
// Phase 1 uses only Atomic. The other values are placeholders for Phase 4.
type StateType uint8

const (
	StateAtomic   StateType = iota // Leaf state — no children
	StateCompound                  // Has children, one active at a time
	StateParallel                  // All regions active simultaneously
	StateFinal                     // Terminal; triggers a done event on parent
	StateHistory                   // Pseudo-state: remembers last substate
)

// HistoryType controls how a history pseudo-state restores configuration.
type HistoryType uint8

const (
	HistoryNone    HistoryType = iota
	HistoryShallow              // Restore direct child only
	HistoryDeep                 // Restore entire subtree configuration
)
