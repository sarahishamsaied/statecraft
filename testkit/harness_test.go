package testkit_test

import (
	"context"
	"statecraft/core"
	"statecraft/model"
	"statecraft/runtime"
	"statecraft/testkit"
	"testing"
	"time"
)

// ─── Always transitions ───────────────────────────────────────────────────────

func TestAlways_FiredOnInitialEntry(t *testing.T) {
	// The machine starts in "routing" which has a guardless always to "active".
	// The harness should land in "active" after NewHarness.
	m := model.New[struct{}]("m").
		Initial("routing").
		State("routing", func(s *model.StateBuilder[struct{}]) {
			s.Always("active")
		}).
		State("active").
		MustBuild()

	h := testkit.NewHarness(m)
	h.AssertState(t, "active")
	h.AssertSteps(t, "routing→active")
}

func TestAlways_ChainedThroughMultipleStates(t *testing.T) {
	// routing → a → b, all via always transitions.
	m := model.New[struct{}]("m").
		Initial("routing").
		State("routing", func(s *model.StateBuilder[struct{}]) { s.Always("a") }).
		State("a", func(s *model.StateBuilder[struct{}]) { s.Always("b") }).
		State("b").
		MustBuild()

	h := testkit.NewHarness(m)
	h.AssertState(t, "b")
	h.AssertSteps(t, "routing→a", "a→b")
}

func TestAlways_GuardedRouting(t *testing.T) {
	// "routing" state acts as a switch: redirect based on context.
	type Ctx struct{ Role string }

	m := model.New[Ctx]("m").
		Initial("routing").
		State("routing", func(s *model.StateBuilder[Ctx]) {
			s.Always("admin",
				model.When[Ctx](func(c Ctx, _ core.Event) bool { return c.Role == "admin" }))
			s.Always("user",
				model.When[Ctx](func(c Ctx, _ core.Event) bool { return c.Role == "user" }))
			s.Always("guest") // fallback
		}).
		State("admin").
		State("user").
		State("guest").
		MustBuild()

	cases := []struct {
		role string
		want string
	}{
		{"admin", "admin"},
		{"user", "user"},
		{"", "guest"},
		{"unknown", "guest"},
	}

	for _, tc := range cases {
		m2 := model.New[Ctx]("m").
			Context(Ctx{Role: tc.role}).
			Initial("routing").
			State("routing", func(s *model.StateBuilder[Ctx]) {
				s.Always("admin", model.When[Ctx](func(c Ctx, _ core.Event) bool { return c.Role == "admin" }))
				s.Always("user", model.When[Ctx](func(c Ctx, _ core.Event) bool { return c.Role == "user" }))
				s.Always("guest")
			}).
			State("admin").State("user").State("guest").
			MustBuild()

		h := testkit.NewHarness(m2)
		if h.State() != core.StateID(tc.want) {
			t.Errorf("role=%q: state=%q, want=%q", tc.role, h.State(), tc.want)
		}
	}
	_ = m
}

func TestAlways_FiredAfterExternalEvent(t *testing.T) {
	// Event "SUBMIT" moves to "validating", which has an always that
	// immediately redirects to "ok" or "err" based on context.
	type Ctx struct{ Valid bool }

	m := model.New[Ctx]("m").
		Initial("idle").
		State("idle", func(s *model.StateBuilder[Ctx]) {
			s.On("SUBMIT", "validating",
				model.Do(model.Assign(func(c Ctx, _ core.Event) Ctx {
					c.Valid = true
					return c
				})),
			)
		}).
		State("validating", func(s *model.StateBuilder[Ctx]) {
			s.Always("ok", model.When[Ctx](func(c Ctx, _ core.Event) bool { return c.Valid }))
			s.Always("err")
		}).
		State("ok").
		State("err").
		MustBuild()

	h := testkit.NewHarness(m)
	h.AssertState(t, "idle")

	h.MustTransition(t, core.E("SUBMIT"))
	// Should have passed through "validating" and landed in "ok"
	h.AssertState(t, "ok")
	// Steps: idle→validating, validating→ok
	h.AssertSteps(t, "idle→validating", "validating→ok")
}

func TestAlways_GuardedSelfLoop_DetectedAtCompile(t *testing.T) {
	_, err := model.New[struct{}]("m").
		Initial("a").
		State("a", func(s *model.StateBuilder[struct{}]) {
			s.Always("a") // guardless self-loop — must be rejected
		}).
		Build()
	if err == nil {
		t.Fatal("expected compile error for guardless self-loop, got nil")
	}
}

func TestAlways_RaisedEventFromAlways(t *testing.T) {
	// The always action raises an internal event which causes a further step.
	type Ctx struct{ Path []string }

	m := model.New[Ctx]("m").
		Initial("a").
		State("a", func(s *model.StateBuilder[Ctx]) {
			s.Always("b",
				model.Do(model.Raise(func(_ Ctx, _ core.Event) core.Event {
					return core.E("INTERNAL")
				})),
			)
		}).
		State("b", func(s *model.StateBuilder[Ctx]) {
			s.On("INTERNAL", "c")
		}).
		State("c").
		MustBuild()

	h := testkit.NewHarness(m)
	h.AssertState(t, "c")
}

// ─── Snapshot.PreviousState ───────────────────────────────────────────────────

func TestPreviousState_InRuntime(t *testing.T) {
	m := model.New[struct{}]("m").
		Initial("a").
		State("a", func(s *model.StateBuilder[struct{}]) { s.On("NEXT", "b") }).
		State("b", func(s *model.StateBuilder[struct{}]) { s.On("NEXT", "c") }).
		State("c").
		MustBuild()

	svc := startSvc(m)
	defer svc.Stop()

	snap := svc.Snapshot()
	if snap.PreviousState != "" {
		t.Errorf("initial PreviousState = %q, want empty", snap.PreviousState)
	}

	ch := awaitState(svc, "b")
	_ = svc.Send(core.E("NEXT"))
	snap = <-ch
	if snap.PreviousState != "a" {
		t.Errorf("PreviousState after a→b = %q, want %q", snap.PreviousState, "a")
	}

	ch = awaitState(svc, "c")
	_ = svc.Send(core.E("NEXT"))
	snap = <-ch
	if snap.PreviousState != "b" {
		t.Errorf("PreviousState after b→c = %q, want %q", snap.PreviousState, "b")
	}
}

func TestPreviousState_InHarness(t *testing.T) {
	m := model.New[struct{}]("m").
		Initial("a").
		State("a", func(s *model.StateBuilder[struct{}]) { s.On("GO", "b") }).
		State("b").
		MustBuild()

	h := testkit.NewHarness(m)
	if h.PreviousState() != "" {
		t.Errorf("initial PreviousState = %q, want empty", h.PreviousState())
	}

	h.MustTransition(t, core.E("GO"))
	h.AssertPreviousState(t, "a")
}

// ─── Harness: basic behaviour ─────────────────────────────────────────────────

func TestHarness_InitialState(t *testing.T) {
	m := trafficMachine()
	h := testkit.NewHarness(m)
	h.AssertState(t, "red")
	h.AssertNotFinal(t)
}

func TestHarness_Send(t *testing.T) {
	h := testkit.NewHarness(trafficMachine())
	h.MustTransition(t, core.E("TIMER"))
	h.AssertState(t, "green")
}

func TestHarness_MultipleTransitions(t *testing.T) {
	h := testkit.NewHarness(trafficMachine())
	for _, want := range []string{"green", "yellow", "red", "green"} {
		h.MustTransition(t, core.E("TIMER"))
		h.AssertState(t, want)
	}
}

func TestHarness_ContextMutation(t *testing.T) {
	type Ctx struct{ Count int }
	m := model.New[Ctx]("m").
		Context(Ctx{}).
		Initial("a").
		State("a", func(s *model.StateBuilder[Ctx]) {
			s.On("INC", "a",
				model.Do(model.Assign(func(c Ctx, _ core.Event) Ctx {
					c.Count++
					return c
				})),
			)
		}).
		MustBuild()

	h := testkit.NewHarness(m)
	for range 5 {
		h.Send(core.E("INC"))
	}
	h.AssertContext(t, func(c Ctx) bool { return c.Count == 5 }, "Count should be 5")
}

func TestHarness_UnhandledEvent(t *testing.T) {
	h := testkit.NewHarness(trafficMachine())
	if h.Send(core.E("UNKNOWN")) {
		t.Error("Send returned true for unhandled event")
	}
	h.AssertState(t, "red") // unchanged
}

func TestHarness_StepsRecorded(t *testing.T) {
	h := testkit.NewHarness(trafficMachine())
	h.Send(core.E("TIMER"))
	h.Send(core.E("TIMER"))
	h.AssertSteps(t, "red→green", "green→yellow")
}

func TestHarness_EntryExitActionsExecuted(t *testing.T) {
	type Ctx struct{ Log []string }
	m := model.New[Ctx]("m").
		Initial("a").
		State("a", func(s *model.StateBuilder[Ctx]) {
			s.Entry(model.Assign(func(c Ctx, _ core.Event) Ctx {
				c.Log = append(c.Log, "enter:a")
				return c
			}))
			s.Exit(model.Assign(func(c Ctx, _ core.Event) Ctx {
				c.Log = append(c.Log, "exit:a")
				return c
			}))
			s.On("GO", "b")
		}).
		State("b", func(s *model.StateBuilder[Ctx]) {
			s.Entry(model.Assign(func(c Ctx, _ core.Event) Ctx {
				c.Log = append(c.Log, "enter:b")
				return c
			}))
		}).
		MustBuild()

	h := testkit.NewHarness(m)
	h.Send(core.E("GO"))
	h.AssertContext(t, func(c Ctx) bool {
		return len(c.Log) == 3 &&
			c.Log[0] == "enter:a" &&
			c.Log[1] == "exit:a" &&
			c.Log[2] == "enter:b"
	}, "log mismatch")
}

func TestHarness_FinalState(t *testing.T) {
	m := model.New[struct{}]("m").
		Initial("active").
		State("active", func(s *model.StateBuilder[struct{}]) { s.On("DONE", "end") }).
		State("end", func(s *model.StateBuilder[struct{}]) { s.Final() }).
		MustBuild()

	h := testkit.NewHarness(m)
	h.Send(core.E("DONE"))
	h.AssertFinal(t)
}

func TestHarness_RaisedEventChain(t *testing.T) {
	m := model.New[struct{}]("m").
		Initial("a").
		State("a", func(s *model.StateBuilder[struct{}]) {
			s.On("KICK", "b", model.Do(model.Raise(func(_ struct{}, _ core.Event) core.Event {
				return core.E("BOUNCE")
			})))
		}).
		State("b", func(s *model.StateBuilder[struct{}]) {
			s.On("BOUNCE", "c")
		}).
		State("c").
		MustBuild()

	h := testkit.NewHarness(m)
	h.Send(core.E("KICK"))
	h.AssertState(t, "c")
}

// ─── Harness: timer (Tick) ────────────────────────────────────────────────────

func TestHarness_Tick_FiresAfterTimer(t *testing.T) {
	m := model.New[struct{}]("m").
		Initial("waiting").
		State("waiting", func(s *model.StateBuilder[struct{}]) {
			s.After(5*time.Second, "done")
		}).
		State("done", func(s *model.StateBuilder[struct{}]) { s.Final() }).
		MustBuild()

	h := testkit.NewHarness(m)
	h.AssertState(t, "waiting")

	n := h.Tick(4 * time.Second) // before deadline
	if n != 0 {
		t.Errorf("Tick(4s) fired %d transitions, want 0", n)
	}
	h.AssertState(t, "waiting")

	n = h.Tick(1 * time.Second) // exactly at deadline
	if n != 1 {
		t.Errorf("Tick(1s) fired %d transitions, want 1", n)
	}
	h.AssertFinal(t)
}

func TestHarness_Tick_CancelledOnExit(t *testing.T) {
	type Ctx struct{ Fired bool }
	m := model.New[Ctx]("m").
		Initial("waiting").
		State("waiting", func(s *model.StateBuilder[Ctx]) {
			s.On("LEAVE", "other")
			s.After(10*time.Second, "timeout",
				model.Do(model.Assign(func(c Ctx, _ core.Event) Ctx {
					c.Fired = true
					return c
				})))
		}).
		State("other").
		State("timeout").
		MustBuild()

	h := testkit.NewHarness(m)
	h.Send(core.E("LEAVE")) // exit waiting — timer should be cancelled
	h.Tick(20 * time.Second)

	h.AssertState(t, "other")
	h.AssertContext(t, func(c Ctx) bool { return !c.Fired }, "timer fired after exit")
}

func TestHarness_Tick_MultipleTimers(t *testing.T) {
	// Two states each with after-timers; machine only fires the one for the
	// state it's in.
	m := model.New[struct{}]("m").
		Initial("a").
		State("a", func(s *model.StateBuilder[struct{}]) {
			s.After(2*time.Second, "b")
		}).
		State("b", func(s *model.StateBuilder[struct{}]) {
			s.After(3*time.Second, "c")
		}).
		State("c", func(s *model.StateBuilder[struct{}]) { s.Final() }).
		MustBuild()

	h := testkit.NewHarness(m)
	h.Tick(2 * time.Second) // fires a→b
	h.AssertState(t, "b")
	h.Tick(3 * time.Second) // fires b→c
	h.AssertFinal(t)
}

// ─── Harness: invoke ──────────────────────────────────────────────────────────

func TestHarness_Invoke_SynchronousSend(t *testing.T) {
	// Invoke calls send() synchronously — event enters internal queue and the
	// harness flush drives the machine to final without any external Send.
	m := model.New[struct{}]("m").
		Initial("idle").
		State("idle", func(s *model.StateBuilder[struct{}]) {
			s.Invoke(func(_ context.Context, _ struct{}, _ core.Event, send func(core.Event)) {
				send(core.E("AUTO"))
			})
			s.On("AUTO", "done")
		}).
		State("done", func(s *model.StateBuilder[struct{}]) { s.Final() }).
		MustBuild()

	h := testkit.NewHarness(m)
	h.AssertState(t, "done")
	h.AssertFinal(t)
}

func TestHarness_Invoke_ContextCancelledOnExit(t *testing.T) {
	// Exiting a state cancels the invoke's context.
	cancelled := make(chan struct{}, 1)

	m := model.New[struct{}]("m").
		Initial("active").
		State("active", func(s *model.StateBuilder[struct{}]) {
			s.Invoke(func(ctx context.Context, _ struct{}, _ core.Event, _ func(core.Event)) {
				go func() {
					<-ctx.Done()
					cancelled <- struct{}{}
				}()
			})
			s.On("LEAVE", "done")
		}).
		State("done").
		MustBuild()

	h := testkit.NewHarness(m)
	h.Send(core.E("LEAVE"))

	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("invoke context not cancelled after state exit in harness")
	}
}

func TestHarness_Invoke_EntryContextReceivesMachineCtx(t *testing.T) {
	// The machineCtx snapshot passed to the invoke matches context at entry time.
	type Ctx struct{ Value int }
	var gotValue int

	m := model.New[Ctx]("m").
		Context(Ctx{Value: 42}).
		Initial("active").
		State("active", func(s *model.StateBuilder[Ctx]) {
			s.Invoke(func(_ context.Context, c Ctx, _ core.Event, _ func(core.Event)) {
				gotValue = c.Value
			})
		}).
		MustBuild()

	testkit.NewHarness(m)
	if gotValue != 42 {
		t.Errorf("invoke received machineCtx.Value = %d, want 42", gotValue)
	}
}

// ─── MockClock integration with live Service ──────────────────────────────────

func TestMockClock_WithService(t *testing.T) {
	m := model.New[struct{}]("m").
		Initial("waiting").
		State("waiting", func(s *model.StateBuilder[struct{}]) {
			s.After(100*time.Millisecond, "done")
		}).
		State("done", func(s *model.StateBuilder[struct{}]) { s.Final() }).
		MustBuild()

	clock := testkit.NewMockClock(time.Now())
	svc := startSvc(m, withClock(clock))
	defer svc.Stop()

	// Before advancing: still in "waiting".
	if svc.Snapshot().Is("done") {
		t.Fatal("already in done before clock advance")
	}

	// Subscribe to the transition.
	ch := make(chan struct{}, 1)
	svc.Subscribe(func(snap runtime.Snapshot[struct{}]) {
		if snap.Is("done") {
			select {
			case ch <- struct{}{}:
			default:
			}
		}
	})

	clock.Advance(100 * time.Millisecond)

	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for done state after clock advance")
	}
}

// ─── Viz: always transitions appear in diagram ───────────────────────────────

func TestAlways_InViz(t *testing.T) {
	m := model.New[struct{}]("m").
		Initial("routing").
		State("routing", func(s *model.StateBuilder[struct{}]) {
			s.Always("active")
		}).
		State("active").
		MustBuild()

	found := false
	for _, ti := range m.Transitions() {
		if ti.IsAlways && ti.From == "routing" && ti.To == "active" {
			found = true
		}
	}
	if !found {
		t.Error("always transition not found in Machine.Transitions()")
	}
}

// ─── helpers / fixtures ───────────────────────────────────────────────────────

type lightCtx struct{ Cycles int }

func trafficMachine() *model.Machine[lightCtx] {
	return model.New[lightCtx]("traffic").
		Context(lightCtx{}).
		Initial("red").
		State("red", func(s *model.StateBuilder[lightCtx]) { s.On("TIMER", "green") }).
		State("green", func(s *model.StateBuilder[lightCtx]) { s.On("TIMER", "yellow") }).
		State("yellow", func(s *model.StateBuilder[lightCtx]) { s.On("TIMER", "red") }).
		MustBuild()
}

// ─── runtime helpers ─────────────────────────────────────────────────────────

func startSvc[C any](m *model.Machine[C], opts ...func(*runtime.ServiceOptions)) *runtime.Service[C] {
	return runtime.Start(m, opts...)
}

func withClock(c core.Clock) func(*runtime.ServiceOptions) {
	return runtime.WithClock(c)
}

// awaitState subscribes and returns a channel that receives the first snapshot
// where the machine is in state id. The subscription is cancelled after delivery.
func awaitState[C any](svc *runtime.Service[C], id string) <-chan runtime.Snapshot[C] {
	ch := make(chan runtime.Snapshot[C], 1)
	var unsub runtime.UnsubscribeFn
	unsub = svc.Subscribe(func(snap runtime.Snapshot[C]) {
		if snap.Is(id) {
			select {
			case ch <- snap:
			default:
			}
			unsub()
		}
	})
	return ch
}

// ─── Hierarchical states ──────────────────────────────────────────────────────

func TestHarness_Hierarchical_InitialEntry(t *testing.T) {
	type Ctx struct{ Log []string }

	m := model.New[Ctx]("m").
		Initial("active").
		State("active", func(s *model.StateBuilder[Ctx]) {
			s.Entry(model.Assign(func(c Ctx, _ core.Event) Ctx {
				c.Log = append(c.Log, "enter:active")
				return c
			}))
			s.State("idle", func(s *model.StateBuilder[Ctx]) {
				s.Entry(model.Assign(func(c Ctx, _ core.Event) Ctx {
					c.Log = append(c.Log, "enter:idle")
					return c
				}))
				s.On("RUN", "running")
			})
			s.State("running")
		}).
		MustBuild()

	h := testkit.NewHarness(m)

	h.AssertState(t, "idle")
	h.AssertIn(t, "active")

	want := []string{"enter:active", "enter:idle"}
	if got := h.Context().Log; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("entry log = %v, want %v", got, want)
	}
}

func TestHarness_Hierarchical_SiblingTransition_DoesNotReenterParent(t *testing.T) {
	type Ctx struct{ Log []string }

	m := model.New[Ctx]("m").
		Initial("active").
		State("active", func(s *model.StateBuilder[Ctx]) {
			s.Entry(model.Assign(func(c Ctx, _ core.Event) Ctx {
				c.Log = append(c.Log, "enter:active")
				return c
			}))
			s.State("idle", func(s *model.StateBuilder[Ctx]) {
				s.Exit(model.Assign(func(c Ctx, _ core.Event) Ctx {
					c.Log = append(c.Log, "exit:idle")
					return c
				}))
				s.On("RUN", "running")
			})
			s.State("running", func(s *model.StateBuilder[Ctx]) {
				s.Entry(model.Assign(func(c Ctx, _ core.Event) Ctx {
					c.Log = append(c.Log, "enter:running")
					return c
				}))
			})
		}).
		MustBuild()

	h := testkit.NewHarness(m)
	h.MustTransition(t, core.E("RUN"))

	h.AssertState(t, "running")
	h.AssertIn(t, "active")

	// active must NOT have been re-entered.
	want := []string{"enter:active", "exit:idle", "enter:running"}
	if got := h.Context().Log; len(got) != len(want) {
		t.Errorf("log = %v, want %v", got, want)
	} else {
		for i, w := range want {
			if got[i] != w {
				t.Errorf("log[%d] = %q, want %q", i, got[i], w)
			}
		}
	}
}

func TestHarness_Hierarchical_EventBubbling(t *testing.T) {
	m := model.New[struct{}]("m").
		Initial("active").
		State("active", func(s *model.StateBuilder[struct{}]) {
			s.State("idle")
			// CANCEL not handled by any child — bubbles to parent.
			s.On("CANCEL", "done")
		}).
		State("done", func(s *model.StateBuilder[struct{}]) { s.Final() }).
		MustBuild()

	h := testkit.NewHarness(m)
	h.AssertState(t, "idle")

	h.MustTransition(t, core.E("CANCEL"))
	h.AssertState(t, "done")
	h.AssertFinal(t)
}

func TestHarness_Hierarchical_AssertIn_And_AssertNotIn(t *testing.T) {
	m := model.New[struct{}]("m").
		Initial("outer").
		State("outer", func(s *model.StateBuilder[struct{}]) {
			s.State("inner")
		}).
		MustBuild()

	h := testkit.NewHarness(m)

	h.AssertIn(t, "outer")
	h.AssertIn(t, "inner")
	h.AssertNotIn(t, "nonexistent")
}

// ─── Parallel states ──────────────────────────────────────────────────────────

func TestHarness_Parallel_InitialEntry(t *testing.T) {
	m := model.New[struct{}]("m").
		Initial("par").
		Parallel("par", func(s *model.StateBuilder[struct{}]) {
			s.State("A", func(s *model.StateBuilder[struct{}]) {
				s.State("a1")
			})
			s.State("B", func(s *model.StateBuilder[struct{}]) {
				s.State("b1")
			})
		}).
		MustBuild()

	h := testkit.NewHarness(m)

	h.AssertLeaves(t, "a1", "b1")
	h.AssertIn(t, "par")
	h.AssertIn(t, "A")
	h.AssertIn(t, "B")
}

func TestHarness_Parallel_IndependentRegions(t *testing.T) {
	m := model.New[struct{}]("m").
		Initial("par").
		Parallel("par", func(s *model.StateBuilder[struct{}]) {
			s.State("A", func(s *model.StateBuilder[struct{}]) {
				s.State("a_idle", func(s *model.StateBuilder[struct{}]) {
					s.On("RUN_A", "a_running")
				})
				s.State("a_running")
			})
			s.State("B", func(s *model.StateBuilder[struct{}]) {
				s.State("b_idle", func(s *model.StateBuilder[struct{}]) {
					s.On("RUN_B", "b_running")
				})
				s.State("b_running")
			})
		}).
		MustBuild()

	h := testkit.NewHarness(m)
	h.AssertLeaves(t, "a_idle", "b_idle")

	h.MustTransition(t, core.E("RUN_A"))
	h.AssertLeaves(t, "a_running", "b_idle")
	h.AssertNotIn(t, "b_running")

	h.MustTransition(t, core.E("RUN_B"))
	h.AssertLeaves(t, "a_running", "b_running")
}

func TestHarness_Parallel_ParentTransitionExitsAllRegions(t *testing.T) {
	m := model.New[struct{}]("m").
		Initial("par").
		Parallel("par", func(s *model.StateBuilder[struct{}]) {
			s.State("A", func(s *model.StateBuilder[struct{}]) { s.State("a1") })
			s.State("B", func(s *model.StateBuilder[struct{}]) { s.State("b1") })
			s.On("DONE", "final")
		}).
		State("final", func(s *model.StateBuilder[struct{}]) { s.Final() }).
		MustBuild()

	h := testkit.NewHarness(m)
	h.MustTransition(t, core.E("DONE"))

	h.AssertState(t, "final")
	h.AssertFinal(t)
	h.AssertNotIn(t, "par")
}

func TestHarness_Parallel_FinalWhenAllRegionsDone(t *testing.T) {
	m := model.New[struct{}]("m").
		Initial("par").
		Parallel("par", func(s *model.StateBuilder[struct{}]) {
			s.State("A", func(s *model.StateBuilder[struct{}]) {
				s.State("a_active", func(s *model.StateBuilder[struct{}]) {
					s.On("DONE_A", "a_done")
				})
				s.State("a_done", func(s *model.StateBuilder[struct{}]) { s.Final() })
			})
			s.State("B", func(s *model.StateBuilder[struct{}]) {
				s.State("b_active", func(s *model.StateBuilder[struct{}]) {
					s.On("DONE_B", "b_done")
				})
				s.State("b_done", func(s *model.StateBuilder[struct{}]) { s.Final() })
			})
		}).
		MustBuild()

	h := testkit.NewHarness(m)
	h.AssertNotFinal(t)

	h.MustTransition(t, core.E("DONE_A"))
	h.AssertNotFinal(t) // only region A done

	h.MustTransition(t, core.E("DONE_B"))
	h.AssertFinal(t) // both regions done
}
