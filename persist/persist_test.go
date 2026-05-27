package persist_test

import (
	"context"
	"encoding/json"
	"statecraft/core"
	"statecraft/model"
	"statecraft/persist"
	"statecraft/runtime"
	"sync/atomic"
	"testing"
	"time"
)

// ─── fixtures ─────────────────────────────────────────────────────────────────

type counterCtx struct{ N int }

var counterMachine = model.New[counterCtx]("counter").
	Context(counterCtx{}).
	Initial("idle").
	State("idle", func(s *model.StateBuilder[counterCtx]) {
		s.On("INC", "idle",
			model.Do(model.Assign(func(c counterCtx, _ core.Event) counterCtx {
				c.N++
				return c
			})),
		)
		s.On("DONE", "final")
	}).
	State("final", func(s *model.StateBuilder[counterCtx]) { s.Final() }).
	MustBuild()

// ─── basic save / restore ─────────────────────────────────────────────────────

func TestPersist_SaveAndRestore_PreservesState(t *testing.T) {
	svc := runtime.Start(counterMachine)
	defer svc.Stop()

	// Advance the machine a few times.
	for range 3 {
		mustSend(t, svc, core.E("INC"))
	}
	time.Sleep(20 * time.Millisecond)

	data, err := persist.Save(svc)
	if err != nil {
		t.Fatalf("Save: %v", err)
	}
	svc.Stop()

	// Restore into a fresh service.
	svc2, err := persist.Restore(counterMachine, data)
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	defer svc2.Stop()

	snap := svc2.Snapshot()
	if !snap.Is("idle") {
		t.Errorf("restored state = %q, want idle", snap.State)
	}
	if snap.Context.N != 3 {
		t.Errorf("restored context.N = %d, want 3", snap.Context.N)
	}
}

func TestPersist_SaveAndRestore_RestoredServiceAcceptsEvents(t *testing.T) {
	svc := runtime.Start(counterMachine)
	defer svc.Stop()

	mustSend(t, svc, core.E("INC"))
	time.Sleep(10 * time.Millisecond)

	data, _ := persist.Save(svc)
	svc.Stop()

	svc2, err := persist.Restore(counterMachine, data)
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	defer svc2.Stop()

	// Send more events after restore.
	ch := awaitFinal(svc2)
	mustSend(t, svc2, core.E("INC"))
	mustSend(t, svc2, core.E("DONE"))
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for final state after restore")
	}

	if svc2.Snapshot().Context.N != 2 {
		t.Errorf("N = %d after restore+INC, want 2", svc2.Snapshot().Context.N)
	}
}

// TestPersist_EntryActionsNotReplayed verifies that entry actions do NOT
// re-fire on restore — the saved context already reflects them.
func TestPersist_EntryActionsNotReplayed(t *testing.T) {
	type Ctx struct{ EntryCount int }

	m := model.New[Ctx]("m").
		Initial("active").
		State("active", func(s *model.StateBuilder[Ctx]) {
			s.Entry(model.Assign(func(c Ctx, _ core.Event) Ctx {
				c.EntryCount++
				return c
			}))
			s.On("NOOP", "active")
		}).
		MustBuild()

	svc := runtime.Start(m)
	defer svc.Stop()

	// Entry fires once on initial start.
	if svc.Snapshot().Context.EntryCount != 1 {
		t.Fatalf("EntryCount = %d after start, want 1", svc.Snapshot().Context.EntryCount)
	}

	data, _ := persist.Save(svc)
	svc.Stop()

	svc2, err := persist.Restore(m, data)
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	defer svc2.Stop()

	// Must still be 1 — entry action must NOT have re-fired.
	if svc2.Snapshot().Context.EntryCount != 1 {
		t.Errorf("EntryCount = %d after restore, want 1 (entry must not replay)",
			svc2.Snapshot().Context.EntryCount)
	}
}

// TestPersist_InvokesRestartOnRestore verifies that invokes DO restart after
// restore (they are ephemeral side-effects, not part of the saved context).
func TestPersist_InvokesRestartOnRestore(t *testing.T) {
	var invokeCount atomic.Int32

	m := model.New[struct{}]("m").
		Initial("active").
		State("active", func(s *model.StateBuilder[struct{}]) {
			s.Invoke(func(_ context.Context, _ struct{}, _ core.Event, _ func(core.Event)) {
				invokeCount.Add(1)
			})
		}).
		MustBuild()

	svc := runtime.Start(m)
	time.Sleep(10 * time.Millisecond)
	if invokeCount.Load() != 1 {
		t.Fatalf("invoke count = %d after start, want 1", invokeCount.Load())
	}

	data, _ := persist.Save(svc)
	svc.Stop()

	svc2, err := persist.Restore(m, data)
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	defer svc2.Stop()

	time.Sleep(10 * time.Millisecond)
	if invokeCount.Load() != 2 {
		t.Errorf("invoke count = %d after restore, want 2 (invoke must restart)", invokeCount.Load())
	}
}

// ─── parallel state persistence ───────────────────────────────────────────────

type formCtx struct {
	F1Dirty bool
	F2Dirty bool
}

var formMachine = model.New[formCtx]("form").
	Initial("par").
	Parallel("par", func(s *model.StateBuilder[formCtx]) {
		s.State("field1", func(s *model.StateBuilder[formCtx]) {
			s.State("f1pristine", func(s *model.StateBuilder[formCtx]) {
				s.On("EDIT1", "f1dirty",
					model.Do(model.Assign(func(c formCtx, _ core.Event) formCtx {
						c.F1Dirty = true
						return c
					})))
			})
			s.State("f1dirty")
		})
		s.State("field2", func(s *model.StateBuilder[formCtx]) {
			s.State("f2pristine", func(s *model.StateBuilder[formCtx]) {
				s.On("EDIT2", "f2dirty",
					model.Do(model.Assign(func(c formCtx, _ core.Event) formCtx {
						c.F2Dirty = true
						return c
					})))
			})
			s.State("f2dirty")
		})
	}).
	MustBuild()

func TestPersist_ParallelState_SaveAndRestore(t *testing.T) {
	svc := runtime.Start(formMachine)
	defer svc.Stop()

	mustSend(t, svc, core.E("EDIT1"))
	time.Sleep(20 * time.Millisecond)

	snap := svc.Snapshot()
	if !snap.In("f1dirty") || !snap.In("f2pristine") {
		t.Fatalf("unexpected state before save: %v", snap.ActiveStates)
	}

	data, err := persist.Save(svc)
	if err != nil {
		t.Fatalf("Save: %v", err)
	}
	svc.Stop()

	svc2, err := persist.Restore(formMachine, data)
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	defer svc2.Stop()

	snap2 := svc2.Snapshot()
	if !snap2.In("f1dirty") {
		t.Errorf("after restore: field1 should be f1dirty; ActiveStates=%v", snap2.ActiveStates)
	}
	if !snap2.In("f2pristine") {
		t.Errorf("after restore: field2 should be f2pristine; ActiveStates=%v", snap2.ActiveStates)
	}
	if !snap2.Context.F1Dirty {
		t.Error("F1Dirty should be true after restore")
	}
	if snap2.Context.F2Dirty {
		t.Error("F2Dirty should be false after restore")
	}
}

// ─── checkpoint wire format ───────────────────────────────────────────────────

func TestPersist_CheckpointJSON_ContainsExpectedFields(t *testing.T) {
	svc := runtime.Start(counterMachine)
	defer svc.Stop()

	data, _ := persist.Save(svc)

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, key := range []string{"machine_id", "leaves", "context", "saved_at"} {
		if _, ok := raw[key]; !ok {
			t.Errorf("checkpoint missing field %q", key)
		}
	}
}

// ─── error cases ─────────────────────────────────────────────────────────────

func TestPersist_Restore_WrongMachineID(t *testing.T) {
	other := model.New[counterCtx]("other-machine").
		Initial("x").State("x").MustBuild()

	svc := runtime.Start(counterMachine)
	data, _ := persist.Save(svc)
	svc.Stop()

	_, err := persist.Restore(other, data)
	if err == nil {
		t.Fatal("expected error restoring checkpoint with wrong machine ID")
	}
}

func TestPersist_Restore_UnknownState(t *testing.T) {
	cp := &persist.Checkpoint{
		MachineID: "counter",
		Leaves:    []string{"nonexistent"},
		Context:   json.RawMessage(`{}`),
	}
	data, _ := json.Marshal(cp)

	_, err := persist.Restore(counterMachine, data)
	if err == nil {
		t.Fatal("expected error restoring checkpoint with unknown state")
	}
}

func TestPersist_Restore_CompoundStateAsLeaf(t *testing.T) {
	m := model.New[struct{}]("m").
		Initial("outer").
		State("outer", func(s *model.StateBuilder[struct{}]) {
			s.State("inner")
		}).
		MustBuild()

	cp := &persist.Checkpoint{
		MachineID: "m",
		Leaves:    []string{"outer"}, // compound, not a valid leaf
		Context:   json.RawMessage(`{}`),
	}
	data, _ := json.Marshal(cp)

	_, err := persist.Restore(m, data)
	if err == nil {
		t.Fatal("expected error restoring checkpoint with compound state as leaf")
	}
}

// ─── helpers ──────────────────────────────────────────────────────────────────

func mustSend(t *testing.T, svc interface{ Send(core.Event) error }, ev core.Event) {
	t.Helper()
	if err := svc.Send(ev); err != nil {
		t.Fatalf("Send: %v", err)
	}
}

func awaitFinal[C any](svc *runtime.Service[C]) <-chan struct{} {
	ch := make(chan struct{})
	var done atomic.Bool
	svc.Subscribe(func(snap runtime.Snapshot[C]) {
		if snap.Final && done.CompareAndSwap(false, true) {
			close(ch)
		}
	})
	return ch
}
