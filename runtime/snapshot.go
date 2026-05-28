package runtime

import (
	"statecraft/core"
	"time"
)

// Snapshot is an immutable point-in-time view of a running service.
// It is safe to read from any goroutine. The generic parameter C mirrors
// the machine's context type.
type Snapshot[C any] struct {
	// State is the first active leaf state ID. For flat and compound machines
	// this is always the single active state. For parallel machines, use
	// Leaves for all active regions.
	State core.StateID

	// Leaves contains all active leaf state IDs — one per parallel region.
	// For flat and compound machines this is always [State]. Suitable for
	// passing directly to persist.Restore to recreate a service.
	Leaves []core.StateID

	// ActiveStates is the full configuration: all states from the outermost
	// ancestor down to all active leaves, in outermost-first definition order.
	// For flat machines this is always [State].
	ActiveStates []core.StateID

	// PreviousState is the leaf state that was active before the last transition.
	// Empty string if the machine has never transitioned.
	PreviousState core.StateID

	// Context is a copy of the machine context at the time of this snapshot.
	Context C

	// Event is the event that triggered this snapshot.
	// nil for the initial snapshot produced when the machine starts.
	Event core.Event

	// Changed is true when a transition fired as a result of Event (including
	// self-transitions where the leaf state stays the same but context updates).
	// False means the event was received but no transition matched.
	Changed bool

	// Final is true when the active state is marked as a terminal state.
	Final bool

	// At is the wall-clock time at which the snapshot was produced.
	At time.Time
}

// Is reports whether the active leaf state equals id.
func (s Snapshot[C]) Is(id string) bool {
	return s.State == core.StateID(id)
}

// In reports whether id appears anywhere in the active configuration —
// i.e., whether the machine is currently inside state id (which may be
// a compound ancestor of the leaf state).
func (s Snapshot[C]) In(id string) bool {
	sid := core.StateID(id)
	for _, a := range s.ActiveStates {
		if a == sid {
			return true
		}
	}
	return false
}
