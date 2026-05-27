package core

import "errors"

var (
	// ErrActorStopped is returned when sending an event to a stopped service.
	ErrActorStopped = errors.New("statecraft: service is stopped")

	// ErrInvalidCheckpoint is returned when a checkpoint is malformed or
	// references states that do not exist in the target machine.
	ErrInvalidCheckpoint = errors.New("statecraft: invalid checkpoint")

	// ErrMailboxFull is returned when the mailbox channel is at capacity
	// and the caller used a non-blocking send variant.
	ErrMailboxFull = errors.New("statecraft: mailbox full")

	// ErrInvalidMachine wraps machine validation failures at Build() time.
	ErrInvalidMachine = errors.New("statecraft: invalid machine definition")

	// ErrDuplicateState signals that a state ID appears more than once.
	ErrDuplicateState = errors.New("statecraft: duplicate state ID")

	// ErrUnknownState signals a reference to a state ID that was never defined.
	ErrUnknownState = errors.New("statecraft: unknown state")

	// ErrUnknownTarget signals a transition that points to an undefined state.
	ErrUnknownTarget = errors.New("statecraft: unknown transition target")

	// ErrNoInitialState signals that no initial state was declared.
	ErrNoInitialState = errors.New("statecraft: no initial state declared")
)
