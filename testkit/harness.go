package testkit

import (
	"context"
	"fmt"
	"statecraft/core"
	"statecraft/model"
	"sort"
	"time"
)

// TB is the subset of testing.TB used by Harness assertions, so the testkit
// package can be imported without pulling in the standard testing package at
// non-test compile time. In tests, pass your *testing.T directly.
type TB interface {
	Helper()
	Errorf(format string, args ...any)
	Fatalf(format string, args ...any)
}

// Step records a single state transition that occurred inside the Harness.
type Step[C any] struct {
	Event   core.Event
	From    core.StateID
	To      core.StateID
	Context C
}

// Harness runs a state machine synchronously — no goroutines, no channels,
// no real-time delays. Every Send and Tick call completes before returning,
// making it ideal for deterministic unit tests.
//
//	h := testkit.NewHarness(myMachine)
//	h.MustTransition(t, core.E("LOGIN"))
//	h.AssertState(t, "authenticating")
//	h.Tick(2 * time.Second)   // fire after-timers
//	h.AssertState(t, "idle")
type Harness[C any] struct {
	machine   *model.Machine[C]
	state     core.StateID
	prevState core.StateID
	ctx       C
	internal  []core.Event
	scheduler *harnessScheduler
	steps     []Step[C]
	invokes   []context.CancelFunc // active invoke cancel funcs
}

// NewHarness creates a Harness from a compiled Machine and immediately
// processes the initial state entry (including always transitions).
func NewHarness[C any](m *model.Machine[C]) *Harness[C] {
	h := &Harness[C]{
		machine:   m,
		state:     m.InitialState(),
		ctx:       m.InitialContext(),
		scheduler: newHarnessScheduler(),
	}
	h.doEntry(h.state, core.Init)
	h.startStateTimers(h.state)
	h.flush()
	return h
}

// ─── Event sending ────────────────────────────────────────────────────────────

// Send delivers an event synchronously. Returns true if the event caused a
// transition; false if no transition matched (event ignored).
func (h *Harness[C]) Send(ev core.Event) bool {
	changed := h.step(ev)
	h.flush()
	return changed
}

// MustTransition calls Send and fails the test if no transition fired.
func (h *Harness[C]) MustTransition(t TB, ev core.Event) {
	t.Helper()
	if !h.Send(ev) {
		t.Fatalf("harness: event %q caused no transition (current state: %q)",
			ev.Type(), h.state)
	}
}

// MustNotTransition calls Send and fails the test if a transition DID fire.
func (h *Harness[C]) MustNotTransition(t TB, ev core.Event) {
	t.Helper()
	if h.Send(ev) {
		t.Fatalf("harness: event %q unexpectedly caused a transition to %q",
			ev.Type(), h.state)
	}
}

// ─── Time advancement ─────────────────────────────────────────────────────────

// Tick advances the harness clock by d and processes any timers that fire,
// in chronological order. Returns the number of transitions that occurred.
func (h *Harness[C]) Tick(d time.Duration) int {
	events := h.scheduler.advance(d)
	count := 0
	for _, ev := range events {
		if h.step(ev) {
			count++
		}
		h.flush()
	}
	return count
}

// ─── Inspection ───────────────────────────────────────────────────────────────

// State returns the current active state ID.
func (h *Harness[C]) State() core.StateID { return h.state }

// PreviousState returns the state active before the last transition.
func (h *Harness[C]) PreviousState() core.StateID { return h.prevState }

// Context returns a copy of the current machine context.
func (h *Harness[C]) Context() C { return h.ctx }

// IsFinal returns true if the current state is marked as a terminal state.
func (h *Harness[C]) IsFinal() bool { return h.machine.IsFinal(h.state) }

// Steps returns all transitions recorded since the Harness was created.
func (h *Harness[C]) Steps() []Step[C] {
	out := make([]Step[C], len(h.steps))
	copy(out, h.steps)
	return out
}

// ─── Assertions ───────────────────────────────────────────────────────────────

// AssertState fails the test if the current state is not id.
func (h *Harness[C]) AssertState(t TB, id string) {
	t.Helper()
	if h.state != core.StateID(id) {
		t.Errorf("state = %q, want %q", h.state, id)
	}
}

// AssertPreviousState fails the test if the previous state is not id.
func (h *Harness[C]) AssertPreviousState(t TB, id string) {
	t.Helper()
	if h.prevState != core.StateID(id) {
		t.Errorf("previousState = %q, want %q", h.prevState, id)
	}
}

// AssertContext fails the test if fn(currentContext) returns false.
// The msg argument is optional and is included in the failure message.
func (h *Harness[C]) AssertContext(t TB, fn func(C) bool, msg ...string) {
	t.Helper()
	if !fn(h.ctx) {
		label := "context assertion failed"
		if len(msg) > 0 {
			label = msg[0]
		}
		t.Errorf("%s (state=%q)", label, h.state)
	}
}

// AssertFinal fails the test if the current state is not a final state.
func (h *Harness[C]) AssertFinal(t TB) {
	t.Helper()
	if !h.IsFinal() {
		t.Errorf("expected final state, got %q", h.state)
	}
}

// AssertNotFinal fails the test if the current state IS a final state.
func (h *Harness[C]) AssertNotFinal(t TB) {
	t.Helper()
	if h.IsFinal() {
		t.Errorf("expected non-final state, got %q", h.state)
	}
}

// AssertSteps fails the test if the recorded transition path doesn't match.
// Pass the expected sequence of "from→to" strings.
func (h *Harness[C]) AssertSteps(t TB, wantPath ...string) {
	t.Helper()
	if len(h.steps) != len(wantPath) {
		t.Errorf("step count = %d, want %d\ngot:  %v\nwant: %v",
			len(h.steps), len(wantPath), h.formatSteps(), wantPath)
		return
	}
	for i, step := range h.steps {
		got := fmt.Sprintf("%s→%s", step.From, step.To)
		if got != wantPath[i] {
			t.Errorf("step[%d] = %q, want %q", i, got, wantPath[i])
		}
	}
}

func (h *Harness[C]) formatSteps() []string {
	out := make([]string, len(h.steps))
	for i, s := range h.steps {
		out[i] = fmt.Sprintf("%s→%s", s.From, s.To)
	}
	return out
}

// ─── Internal engine ──────────────────────────────────────────────────────────

// step resolves one transition for ev. Does NOT flush internal queue or
// check always transitions — that is flush's job.
func (h *Harness[C]) step(ev core.Event) bool {
	target, tActions, ok := h.machine.ResolveTransition(h.state, h.ctx, ev)
	if !ok {
		return false
	}
	h.doTransition(ev, target, tActions)
	return true
}

// stepAlways resolves one always transition if one is enabled.
// Returns true if a transition fired.
func (h *Harness[C]) stepAlways() bool {
	target, tActions, ok := h.machine.ResolveAlways(h.state, h.ctx)
	if !ok {
		return false
	}
	h.doTransition(model.AlwaysEvent{}, target, tActions)
	return true
}

// flush drains the internal queue and evaluates always transitions until
// the machine reaches a stable state (no more internal events, no always
// transition matches).
func (h *Harness[C]) flush() {
	const maxIter = 1000
	for range maxIter {
		if len(h.internal) > 0 {
			ev := h.internal[0]
			h.internal = h.internal[1:]
			h.step(ev)
			continue
		}
		if h.stepAlways() {
			continue
		}
		return // stable
	}
	panic("testkit.Harness: flush exceeded 1000 iterations — always-transition loop in state " +
		string(h.state))
}

func (h *Harness[C]) doTransition(ev core.Event, target core.StateID, tActions []model.ActionFn[C]) {
	from := h.state
	ac := &model.ActionContext{}

	h.doExit(h.state, ev)
	h.stopStateTimers(h.state)
	h.stopInvokes()

	for _, a := range tActions {
		h.ctx = a(h.ctx, ev, ac)
	}

	h.prevState = h.state
	h.state = target

	h.doEntry(h.state, ev)
	h.startStateTimers(h.state)

	h.internal = append(h.internal, ac.Drain()...)
	h.steps = append(h.steps, Step[C]{Event: ev, From: from, To: h.state, Context: h.ctx})
}

func (h *Harness[C]) doEntry(id core.StateID, ev core.Event) {
	actions := h.machine.EntryActions(id)
	ac := &model.ActionContext{}
	for _, a := range actions {
		h.ctx = a(h.ctx, ev, ac)
	}
	h.internal = append(h.internal, ac.Drain()...)
	// Start invocations. The send callback feeds directly into the internal
	// queue — it is NOT goroutine-safe and must only be called synchronously
	// within the InvokeFn body (not from a spawned goroutine).
	for _, fn := range h.machine.InvokeFns(id) {
		invokeCtx, cancel := context.WithCancel(context.Background())
		h.invokes = append(h.invokes, cancel)
		fn(invokeCtx, h.ctx, ev, func(sendEv core.Event) {
			h.internal = append(h.internal, sendEv)
		})
	}
}

func (h *Harness[C]) stopInvokes() {
	for _, cancel := range h.invokes {
		cancel()
	}
	h.invokes = h.invokes[:0]
}

func (h *Harness[C]) doExit(id core.StateID, ev core.Event) {
	actions := h.machine.ExitActions(id)
	ac := &model.ActionContext{}
	for _, a := range actions {
		h.ctx = a(h.ctx, ev, ac)
	}
	h.internal = append(h.internal, ac.Drain()...)
}

func (h *Harness[C]) startStateTimers(id core.StateID) {
	for _, conf := range h.machine.AfterConfs(id) {
		ev := core.E(string(conf.EventType))
		h.scheduler.start(conf.TimerID, conf.Delay, ev)
	}
}

func (h *Harness[C]) stopStateTimers(id core.StateID) {
	for _, conf := range h.machine.AfterConfs(id) {
		h.scheduler.cancel(conf.TimerID)
	}
}

// ─── harnessScheduler ────────────────────────────────────────────────────────

// harnessScheduler is a fully synchronous timer manager used internally by
// Harness. Unlike runtime.Scheduler it does not use goroutines or channels.
type harnessScheduler struct {
	now     time.Time
	pending []harnessTimer
}

type harnessTimer struct {
	id        string
	fireAt    time.Time
	event     core.Event
	cancelled bool
}

func newHarnessScheduler() *harnessScheduler {
	return &harnessScheduler{now: time.Time{}}
}

func (s *harnessScheduler) start(id string, d time.Duration, ev core.Event) {
	s.cancel(id)
	s.pending = append(s.pending, harnessTimer{
		id:     id,
		fireAt: s.now.Add(d),
		event:  ev,
	})
}

func (s *harnessScheduler) cancel(id string) {
	for i := range s.pending {
		if s.pending[i].id == id {
			s.pending[i].cancelled = true
		}
	}
}

// advance moves time forward by d and returns all events whose timers fired,
// in chronological order.
func (s *harnessScheduler) advance(d time.Duration) []core.Event {
	s.now = s.now.Add(d)
	deadline := s.now

	sort.Slice(s.pending, func(i, j int) bool {
		return s.pending[i].fireAt.Before(s.pending[j].fireAt)
	})

	var fired []core.Event
	var remaining []harnessTimer
	for _, t := range s.pending {
		if !t.cancelled && !t.fireAt.After(deadline) {
			fired = append(fired, t.event)
		} else if !t.cancelled {
			remaining = append(remaining, t)
		}
	}
	s.pending = remaining
	return fired
}
