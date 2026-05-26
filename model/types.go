package model

import (
	"time"
)

// These unexported types represent the user's raw machine configuration
// as accumulated by the Builder. They are inputs to compile().

type machineConfig[C any] struct {
	id      string
	initial string
	context C
	states  []*stateConfig[C] // ordered — definition order is preserved
}

type stateConfig[C any] struct {
	id          string
	final       bool
	transitions []transitionConfig[C]
	afterConfs  []afterStateConfig[C]
	always      []transitionConfig[C] // null/automatic transitions — no event trigger
	onEntry     []ActionFn[C]
	onExit      []ActionFn[C]
}

type transitionConfig[C any] struct {
	event   string
	target  string
	guard   GuardFn[C]
	actions []ActionFn[C]
}

type afterStateConfig[C any] struct {
	delay   time.Duration
	target  string
	guard   GuardFn[C]
	actions []ActionFn[C]
}
