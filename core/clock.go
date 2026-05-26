package core

import "time"

// Timer is an opaque handle to a scheduled callback.
// Stop cancels the timer; it returns false if the callback already fired.
type Timer interface {
	Stop() bool
}

// Clock abstracts time — injected so tests can run deterministically
// without real wall-clock delays.
type Clock interface {
	Now() time.Time
	// AfterFunc calls f in a new goroutine after duration d.
	AfterFunc(d time.Duration, f func()) Timer
}

// System is the real-time Clock implementation used in production.
var System Clock = systemClock{}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }
func (systemClock) AfterFunc(d time.Duration, f func()) Timer {
	return time.AfterFunc(d, f)
}
