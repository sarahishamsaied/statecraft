package model

import "statecraft/core"

// GuardFn is a pure predicate evaluated before a transition is selected.
// It MUST be side-effect free. Returning true enables the transition;
// false causes the interpreter to try the next candidate transition.
type GuardFn[C any] func(ctx C, event core.Event) bool

// And returns a guard that passes only when ALL provided guards pass.
// Short-circuits on the first false result.
func And[C any](guards ...GuardFn[C]) GuardFn[C] {
	return func(ctx C, event core.Event) bool {
		for _, g := range guards {
			if !g(ctx, event) {
				return false
			}
		}
		return true
	}
}

// Or returns a guard that passes when ANY of the provided guards passes.
// Short-circuits on the first true result.
func Or[C any](guards ...GuardFn[C]) GuardFn[C] {
	return func(ctx C, event core.Event) bool {
		for _, g := range guards {
			if g(ctx, event) {
				return true
			}
		}
		return false
	}
}

// Not returns a guard that inverts its argument.
func Not[C any](guard GuardFn[C]) GuardFn[C] {
	return func(ctx C, event core.Event) bool {
		return !guard(ctx, event)
	}
}
