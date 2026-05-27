package testkit

import (
	"context"
	"fmt"
	"sort"
	"statecraft/core"
	"statecraft/model"
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
// For parallel machines, From and To reflect leaves[0] (the first region);
// use Harness.In for compound/parallel membership assertions.
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
// Hierarchical and parallel machines are fully supported: the harness uses
// the same LCCA-based exit/entry algorithm as the runtime.
//
//	h := testkit.NewHarness(myMachine)
//	h.MustTransition(t, core.E("LOGIN"))
//	h.AssertState(t, "authenticating")
//	h.Tick(2 * time.Second)   // fire after-timers
//	h.AssertState(t, "idle")
type Harness[C any] struct {
	machine   *model.Machine[C]
	leaves    []core.StateID            // active leaf states; len=1 for flat/compound
	prevLeaf  core.StateID              // leaves[0] before last transition
	ctx       C
	internal  []core.Event
	scheduler *harnessScheduler
	steps     []Step[C]
	invokes   map[core.StateID][]context.CancelFunc // per-state invoke cancels
}

// NewHarness creates a Harness from a compiled Machine and immediately
// processes the initial state entry (including always transitions).
func NewHarness[C any](m *model.Machine[C]) *Harness[C] {
	h := &Harness[C]{
		machine:   m,
		leaves:    m.InitialLeaves(),
		ctx:       m.InitialContext(),
		scheduler: newHarnessScheduler(),
		invokes:   make(map[core.StateID][]context.CancelFunc),
	}
	// Enter the full initial configuration outermost-first.
	for _, id := range h.entryPaths("", h.leaves) {
		h.doEntry(id, core.Init)
		h.startStateTimers(id)
	}
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
		t.Fatalf("harness: event %q caused no transition (leaves: %v)",
			ev.Type(), h.leaves)
	}
}

// MustNotTransition calls Send and fails the test if a transition DID fire.
func (h *Harness[C]) MustNotTransition(t TB, ev core.Event) {
	t.Helper()
	if h.Send(ev) {
		t.Fatalf("harness: event %q unexpectedly caused a transition (leaves: %v)",
			ev.Type(), h.leaves)
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

// State returns the first active leaf state ID.
// For flat and compound machines this is always the single active state.
func (h *Harness[C]) State() core.StateID { return h.leaves[0] }

// Leaves returns all active leaf state IDs (one per parallel region).
func (h *Harness[C]) Leaves() []core.StateID {
	out := make([]core.StateID, len(h.leaves))
	copy(out, h.leaves)
	return out
}

// ActiveStates returns the full active configuration: all states from the
// outermost ancestor to all active leaves, in definition order.
func (h *Harness[C]) ActiveStates() []core.StateID {
	return h.machine.ConfigurationOf(h.leaves)
}

// In reports whether id is anywhere in the active configuration — i.e.
// whether the machine is currently inside state id (which may be a compound
// or parallel ancestor of a leaf state).
func (h *Harness[C]) In(id string) bool {
	sid := core.StateID(id)
	for _, a := range h.machine.ConfigurationOf(h.leaves) {
		if a == sid {
			return true
		}
	}
	return false
}

// PreviousState returns the first leaf state active before the last transition.
func (h *Harness[C]) PreviousState() core.StateID { return h.prevLeaf }

// Context returns a copy of the current machine context.
func (h *Harness[C]) Context() C { return h.ctx }

// IsFinal returns true when all active leaves are marked as terminal states.
func (h *Harness[C]) IsFinal() bool {
	for _, leaf := range h.leaves {
		if !h.machine.IsFinal(leaf) {
			return false
		}
	}
	return len(h.leaves) > 0
}

// Steps returns all transitions recorded since the Harness was created.
func (h *Harness[C]) Steps() []Step[C] {
	out := make([]Step[C], len(h.steps))
	copy(out, h.steps)
	return out
}

// ─── Assertions ───────────────────────────────────────────────────────────────

// AssertState fails the test if the first active leaf state is not id.
// For flat and compound machines this checks the only active state.
// For parallel machines, prefer AssertIn.
func (h *Harness[C]) AssertState(t TB, id string) {
	t.Helper()
	if h.leaves[0] != core.StateID(id) {
		t.Errorf("state = %q, want %q", h.leaves[0], id)
	}
}

// AssertIn fails the test if id is not in the active configuration.
// Works for atomic leaves, compound ancestors, and parallel parents.
func (h *Harness[C]) AssertIn(t TB, id string) {
	t.Helper()
	if !h.In(id) {
		t.Errorf("In(%q) = false; active = %v", id, h.machine.ConfigurationOf(h.leaves))
	}
}

// AssertNotIn fails the test if id IS in the active configuration.
func (h *Harness[C]) AssertNotIn(t TB, id string) {
	t.Helper()
	if h.In(id) {
		t.Errorf("In(%q) = true (unexpected); active = %v", id, h.machine.ConfigurationOf(h.leaves))
	}
}

// AssertLeaves fails the test if the active leaf set doesn't exactly match ids.
// Order must match the region definition order.
func (h *Harness[C]) AssertLeaves(t TB, ids ...string) {
	t.Helper()
	if len(h.leaves) != len(ids) {
		t.Errorf("leaves = %v, want %v", h.leaves, ids)
		return
	}
	for i, id := range ids {
		if h.leaves[i] != core.StateID(id) {
			t.Errorf("leaves[%d] = %q, want %q", i, h.leaves[i], id)
		}
	}
}

// AssertPreviousState fails the test if the previous state is not id.
func (h *Harness[C]) AssertPreviousState(t TB, id string) {
	t.Helper()
	if h.prevLeaf != core.StateID(id) {
		t.Errorf("previousState = %q, want %q", h.prevLeaf, id)
	}
}

// AssertContext fails the test if fn(currentContext) returns false.
func (h *Harness[C]) AssertContext(t TB, fn func(C) bool, msg ...string) {
	t.Helper()
	if !fn(h.ctx) {
		label := "context assertion failed"
		if len(msg) > 0 {
			label = msg[0]
		}
		t.Errorf("%s (leaves=%v)", label, h.leaves)
	}
}

// AssertFinal fails the test if the current state is not a final state.
func (h *Harness[C]) AssertFinal(t TB) {
	t.Helper()
	if !h.IsFinal() {
		t.Errorf("expected final state, got leaves=%v", h.leaves)
	}
}

// AssertNotFinal fails the test if the current state IS a final state.
func (h *Harness[C]) AssertNotFinal(t TB) {
	t.Helper()
	if h.IsFinal() {
		t.Errorf("expected non-final state, got leaves=%v", h.leaves)
	}
}

// AssertSteps fails the test if the recorded transition path doesn't match.
// Pass the expected sequence of "from→to" strings (uses leaves[0] for each).
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

// step collects enabled transitions from every active leaf and applies them.
// Returns true if any transition fired.
func (h *Harness[C]) step(ev core.Event) bool {
	var pending []pendingTrans[C]
	covered := make(map[core.StateID]bool)
	for _, leaf := range h.leaves {
		if covered[leaf] {
			continue
		}
		target, actions, ok := h.machine.ResolveTransition(leaf, h.ctx, ev)
		if !ok {
			continue
		}
		pending = append(pending, pendingTrans[C]{leaf, target, actions})
		covered[leaf] = true
	}
	if len(pending) == 0 {
		return false
	}
	h.applyTransitions(ev, pending)
	return true
}

// stepAlways checks all leaves for enabled always transitions.
// Applies the first one found and returns true.
func (h *Harness[C]) stepAlways() bool {
	covered := make(map[core.StateID]bool)
	for _, leaf := range h.leaves {
		if covered[leaf] {
			continue
		}
		target, actions, ok := h.machine.ResolveAlways(leaf, h.ctx)
		if !ok {
			covered[leaf] = true
			continue
		}
		h.applyTransitions(model.AlwaysEvent{}, []pendingTrans[C]{{leaf, target, actions}})
		return true
	}
	return false
}

// flush drains the internal queue and evaluates always transitions until
// the machine reaches a stable state.
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
		return
	}
	panic("testkit.Harness: flush exceeded 1000 iterations — always-transition loop in leaves " +
		fmt.Sprint(h.leaves))
}

// pendingTrans is a resolved, enabled transition waiting to be applied.
type pendingTrans[C any] struct {
	source  core.StateID
	target  core.StateID
	actions []model.ActionFn[C]
}

// applyTransitions executes one SCXML macrostep: LCCA-based exit/entry sets,
// preserving leaf ordering for parallel machines.
func (h *Harness[C]) applyTransitions(ev core.Event, pending []pendingTrans[C]) {
	prevLeaves := append([]core.StateID(nil), h.leaves...)
	ac := &model.ActionContext{}

	type transInfo struct {
		source      core.StateID
		leafTargets []core.StateID
		lcca        core.StateID
		exitPath    []core.StateID
		entryPaths  [][]core.StateID
	}

	infos := make([]transInfo, len(pending))
	for i, p := range pending {
		lt := h.machine.LeafTargets(p.target)
		lcca := h.machine.LCCA(p.source, lt[0])
		exitPath := h.machine.ExitPath(p.source, lcca)
		entryPaths := make([][]core.StateID, len(lt))
		for j, leafTarget := range lt {
			entryPaths[j] = h.machine.EntryPath(lcca, leafTarget)
		}
		infos[i] = transInfo{p.source, lt, lcca, exitPath, entryPaths}
	}

	// Unified exit set sorted innermost-first.
	exitSeen := make(map[core.StateID]bool)
	var exitOrder []core.StateID
	for _, info := range infos {
		for _, id := range info.exitPath {
			if !exitSeen[id] {
				exitSeen[id] = true
				exitOrder = append(exitOrder, id)
			}
		}
	}
	sort.SliceStable(exitOrder, func(i, j int) bool {
		return h.machine.Depth(exitOrder[i]) > h.machine.Depth(exitOrder[j])
	})

	for _, id := range exitOrder {
		h.doExit(id, ev)
		h.stopStateTimers(id)
		h.stopStateInvokes(id)
	}

	for _, p := range pending {
		for _, a := range p.actions {
			h.ctx = a(h.ctx, ev, ac)
		}
	}

	// New leaf set: replace exited sources in-place to preserve region order.
	sourceToInfo := make(map[core.StateID]*transInfo, len(infos))
	for i := range infos {
		sourceToInfo[infos[i].source] = &infos[i]
	}
	newLeaves := make([]core.StateID, 0, len(h.leaves))
	for _, leaf := range h.leaves {
		if info := sourceToInfo[leaf]; info != nil {
			newLeaves = append(newLeaves, info.leafTargets...)
		} else {
			newLeaves = append(newLeaves, leaf)
		}
	}
	h.prevLeaf = prevLeaves[0]
	h.leaves = newLeaves

	// Unified entry set outermost-first.
	entrySeen := make(map[core.StateID]bool)
	var entryOrder []core.StateID
	for _, info := range infos {
		for _, ep := range info.entryPaths {
			for _, id := range ep {
				if !entrySeen[id] {
					entrySeen[id] = true
					entryOrder = append(entryOrder, id)
				}
			}
		}
	}
	for _, id := range entryOrder {
		h.doEntry(id, ev)
		h.startStateTimers(id)
	}

	h.internal = append(h.internal, ac.Drain()...)
	h.steps = append(h.steps, Step[C]{
		Event:   ev,
		From:    prevLeaves[0],
		To:      h.leaves[0],
		Context: h.ctx,
	})
}

// ─── Lifecycle helpers ────────────────────────────────────────────────────────

func (h *Harness[C]) doEntry(id core.StateID, ev core.Event) {
	ac := &model.ActionContext{}
	for _, a := range h.machine.EntryActions(id) {
		h.ctx = a(h.ctx, ev, ac)
	}
	h.internal = append(h.internal, ac.Drain()...)
	// Invokes: send callback feeds the internal queue synchronously.
	// Callers must not spawn goroutines inside an invoke used with Harness.
	for _, fn := range h.machine.InvokeFns(id) {
		invokeCtx, cancel := context.WithCancel(context.Background())
		h.invokes[id] = append(h.invokes[id], cancel)
		fn(invokeCtx, h.ctx, ev, func(sendEv core.Event) {
			h.internal = append(h.internal, sendEv)
		})
	}
}

func (h *Harness[C]) doExit(id core.StateID, ev core.Event) {
	ac := &model.ActionContext{}
	for _, a := range h.machine.ExitActions(id) {
		h.ctx = a(h.ctx, ev, ac)
	}
	h.internal = append(h.internal, ac.Drain()...)
}

func (h *Harness[C]) stopStateInvokes(id core.StateID) {
	for _, cancel := range h.invokes[id] {
		cancel()
	}
	delete(h.invokes, id)
}

func (h *Harness[C]) startStateTimers(id core.StateID) {
	for _, conf := range h.machine.AfterConfs(id) {
		h.scheduler.start(conf.TimerID, conf.Delay, core.E(string(conf.EventType)))
	}
}

func (h *Harness[C]) stopStateTimers(id core.StateID) {
	for _, conf := range h.machine.AfterConfs(id) {
		h.scheduler.cancel(conf.TimerID)
	}
}

// ─── Entry path helpers ───────────────────────────────────────────────────────

// entryPaths returns the deduplicated outermost-first entry path for a set of
// leaf targets, relative to lcca ("" means from the top level).
func (h *Harness[C]) entryPaths(lcca core.StateID, leaves []core.StateID) []core.StateID {
	seen := make(map[core.StateID]bool)
	var result []core.StateID
	for _, leaf := range leaves {
		for _, id := range h.machine.EntryPath(lcca, leaf) {
			if !seen[id] {
				seen[id] = true
				result = append(result, id)
			}
		}
	}
	return result
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
