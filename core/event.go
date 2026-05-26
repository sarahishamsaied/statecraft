// Package core defines the primitive types shared across all statecraft packages.
// It has no internal dependencies and may be imported by any layer.
package core

// EventType is the discriminant for an event — a unique string tag.
// All system-reserved event types are prefixed with "statecraft.".
type EventType string

const (
	EventTypeInit   EventType = "statecraft.init"
	EventTypeCancel EventType = "statecraft.cancel"
	EventTypeDone   EventType = "statecraft.done"
	EventTypeError  EventType = "statecraft.error"
	EventTypeAfter  EventType = "statecraft.after"
	EventTypeAlways EventType = "statecraft.always" // synthetic event for always transitions
)

// Event is the core interface. Every message sent to a state machine must
// implement it. Payloads live in concrete types — use a type switch to access them.
type Event interface {
	Type() EventType
}

// E creates a simple event with no payload from a string event type name.
// Useful for ad-hoc events in tests and examples.
func E(t string) Event { return simpleEvent{t: EventType(t)} }

type simpleEvent struct{ t EventType }

func (e simpleEvent) Type() EventType { return e.t }

// InitEvent is the synthetic event passed to entry actions on machine start.
type InitEvent struct{}

func (InitEvent) Type() EventType { return EventTypeInit }

// Init is the singleton init event used for initial state entry.
var Init Event = InitEvent{}

// Envelope wraps an event for transport through the mailbox channel.
// Fields beyond Event will expand in later phases (IDs, tracing, source actor).
type Envelope struct {
	Event Event
}
