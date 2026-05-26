package runtime

import (
	"statecraft/core"
	"time"
)

// Snapshot is an immutable point-in-time view of a running service.
// It is safe to read from any goroutine. The generic parameter C mirrors
// the machine's context type.
type Snapshot[C any] struct {
	// State is the currently active state ID. For Phase 1 (flat machines)
	// this is always a single value. Phase 4 will add an ActiveStates slice
	// for hierarchical and parallel configurations.
	State core.StateID

	// PreviousState is the state that was active before the last transition.
	// Empty string if the machine has never transitioned.
	PreviousState core.StateID

	// Context is a copy of the machine context at the time of this snapshot.
	Context C

	// Event is the event that triggered this snapshot.
	// nil for the initial snapshot produced when the machine starts.
	Event core.Event

	// Changed is true when the active state changed as a result of Event.
	// False means the event was received but no transition matched.
	Changed bool

	// Final is true when the active state is marked as a terminal state.
	Final bool

	// At is the wall-clock time at which the snapshot was produced.
	At time.Time
}

// Is reports whether the active state equals id.
// Sugar for: snap.State == core.StateID(id)
func (s Snapshot[C]) Is(id string) bool {
	return s.State == core.StateID(id)
}
