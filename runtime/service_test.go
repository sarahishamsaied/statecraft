package runtime_test

import (
	"context"
	"statecraft/core"
	"statecraft/model"
	"statecraft/runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ─── fixtures ─────────────────────────────────────────────────────────────────

type lightCtx struct{ Cycles int }

var trafficLight = model.New[lightCtx]("traffic-light").
	Context(lightCtx{}).
	Initial("red").
	State("red", func(s *model.StateBuilder[lightCtx]) {
		s.On("TIMER", "green",
			model.Do(model.Assign(func(c lightCtx, _ core.Event) lightCtx {
				c.Cycles++
				return c
			})),
		)
	}).
	State("green", func(s *model.StateBuilder[lightCtx]) {
		s.On("TIMER", "yellow")
	}).
	State("yellow", func(s *model.StateBuilder[lightCtx]) {
		s.On("TIMER", "red")
	}).
	MustBuild()

// ─── basic behaviour ──────────────────────────────────────────────────────────

func TestService_InitialState(t *testing.T) {
	svc := runtime.Start(trafficLight)
	defer svc.Stop()

	snap := svc.Snapshot()
	if !snap.Is("red") {
		t.Errorf("initial state = %q, want %q", snap.State, "red")
	}
	if snap.Final {
		t.Error("initial state should not be final")
	}
}

func TestService_Transition(t *testing.T) {
	svc := runtime.Start(trafficLight)
	defer svc.Stop()

	ch := awaitState(svc, "green")
	mustSend(t, svc, core.E("TIMER"))
	assertState(t, ch, "green")
}

func TestService_MultipleTransitions(t *testing.T) {
	svc := runtime.Start(trafficLight)
	defer svc.Stop()

	for _, want := range []string{"green", "yellow", "red", "green"} {
		ch := awaitState(svc, want)
		mustSend(t, svc, core.E("TIMER"))
		assertState(t, ch, want)
	}
}

func TestService_ContextMutation(t *testing.T) {
	svc := runtime.Start(trafficLight)
	defer svc.Stop()

	ch := awaitChange(svc)
	mustSend(t, svc, core.E("TIMER")) // red → green, Cycles++
	<-ch
	if svc.Snapshot().Context.Cycles != 1 {
		t.Errorf("Cycles = %d, want 1", svc.Snapshot().Context.Cycles)
	}
}

func TestService_UnhandledEventIgnored(t *testing.T) {
	svc := runtime.Start(trafficLight)
	defer svc.Stop()

	mustSend(t, svc, core.E("UNKNOWN"))
	// Give the interpreter time to process.
	time.Sleep(10 * time.Millisecond)

	if !svc.Snapshot().Is("red") {
		t.Errorf("state should remain red after unhandled event, got %q", svc.State())
	}
}

// ─── subscribe / unsubscribe ──────────────────────────────────────────────────

func TestService_Subscribe(t *testing.T) {
	svc := runtime.Start(trafficLight)
	defer svc.Stop()

	var mu sync.Mutex
	var states []string

	unsub := svc.Subscribe(func(snap runtime.Snapshot[lightCtx]) {
		mu.Lock()
		states = append(states, string(snap.State))
		mu.Unlock()
	})
	defer unsub()

	for range 3 {
		mustSend(t, svc, core.E("TIMER"))
	}
	// Drain processing.
	time.Sleep(30 * time.Millisecond)

	mu.Lock()
	got := append([]string(nil), states...)
	mu.Unlock()

	want := []string{"green", "yellow", "red"}
	if len(got) < len(want) {
		t.Fatalf("subscriber got %v, want at least %v", got, want)
	}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("snap[%d] = %q, want %q", i, got[i], w)
		}
	}
}

func TestService_Unsubscribe(t *testing.T) {
	svc := runtime.Start(trafficLight)
	defer svc.Stop()

	var called atomic.Int32
	unsub := svc.Subscribe(func(_ runtime.Snapshot[lightCtx]) { called.Add(1) })

	mustSend(t, svc, core.E("TIMER"))
	time.Sleep(20 * time.Millisecond)
	unsub()

	before := called.Load()
	mustSend(t, svc, core.E("TIMER"))
	time.Sleep(20 * time.Millisecond)

	if after := called.Load(); after != before {
		t.Errorf("subscriber called after unsub: was %d, now %d", before, after)
	}
}

// ─── stop / done ──────────────────────────────────────────────────────────────

func TestService_StopIdempotent(t *testing.T) {
	svc := runtime.Start(trafficLight)
	svc.Stop()
	svc.Stop() // must not panic or deadlock
}

func TestService_DoneClosedAfterStop(t *testing.T) {
	svc := runtime.Start(trafficLight)
	svc.Stop()
	select {
	case <-svc.Done():
	case <-time.After(time.Second):
		t.Fatal("Done() not closed after Stop()")
	}
}

func TestService_SendAfterStop(t *testing.T) {
	svc := runtime.Start(trafficLight)
	svc.Stop()
	err := svc.Send(core.E("TIMER"))
	if err != core.ErrActorStopped {
		t.Errorf("Send after stop: got %v, want ErrActorStopped", err)
	}
}

// ─── entry / exit actions ────────────────────────────────────────────────────

func TestService_EntryExitActions(t *testing.T) {
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

	svc := runtime.Start(m)
	defer svc.Stop()

	ch := awaitState(svc, "b")
	mustSend(t, svc, core.E("GO"))
	assertState(t, ch, "b")

	log := svc.Snapshot().Context.Log
	want := []string{"enter:a", "exit:a", "enter:b"}
	if len(log) != len(want) {
		t.Fatalf("log = %v, want %v", log, want)
	}
	for i, w := range want {
		if log[i] != w {
			t.Errorf("log[%d] = %q, want %q", i, log[i], w)
		}
	}
}

// ─── raise (internal events) ─────────────────────────────────────────────────

func TestService_RaisedEventProcessedBeforeExternal(t *testing.T) {
	// State A on "KICK" raises "INTERNAL" → both transitions complete before
	// any further external events are processed.
	type Ctx struct{ Path []string }

	m := model.New[Ctx]("m").
		Initial("a").
		State("a", func(s *model.StateBuilder[Ctx]) {
			s.On("KICK", "b",
				model.Do(model.Raise(func(_ Ctx, _ core.Event) core.Event {
					return core.E("INTERNAL")
				})),
			)
		}).
		State("b", func(s *model.StateBuilder[Ctx]) {
			s.Entry(model.Assign(func(c Ctx, _ core.Event) Ctx {
				c.Path = append(c.Path, "b")
				return c
			}))
			s.On("INTERNAL", "c")
		}).
		State("c", func(s *model.StateBuilder[Ctx]) {
			s.Entry(model.Assign(func(c Ctx, _ core.Event) Ctx {
				c.Path = append(c.Path, "c")
				return c
			}))
		}).
		MustBuild()

	svc := runtime.Start(m)
	defer svc.Stop()

	ch := awaitState(svc, "c")
	mustSend(t, svc, core.E("KICK"))
	assertState(t, ch, "c")

	// Path must be ["b", "c"] — b was entered and then immediately advanced
	// via the raised internal event, all before any more external events.
	path := svc.Snapshot().Context.Path
	if len(path) != 2 || path[0] != "b" || path[1] != "c" {
		t.Errorf("path = %v, want [b c]", path)
	}
}

// ─── state-scoped timers (After) ─────────────────────────────────────────────

func TestService_After_FiresAndTransitions(t *testing.T) {
	m := model.New[struct{}]("m").
		Initial("waiting").
		State("waiting", func(s *model.StateBuilder[struct{}]) {
			s.After(50*time.Millisecond, "done")
		}).
		State("done", func(s *model.StateBuilder[struct{}]) {
			s.Final()
		}).
		MustBuild()

	svc := runtime.Start(m)
	defer svc.Stop()

	select {
	case <-waitForFinal(svc):
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for final state after After(50ms)")
	}
	if !svc.Snapshot().Is("done") {
		t.Errorf("state = %q, want done", svc.State())
	}
}

func TestService_After_CancelledOnExit(t *testing.T) {
	// If we leave "waiting" before the timer fires, the timer must be
	// cancelled and must NOT fire in the new state.
	type Ctx struct{ TimerFired bool }

	m := model.New[Ctx]("m").
		Initial("waiting").
		State("waiting", func(s *model.StateBuilder[Ctx]) {
			s.On("LEAVE", "idle")
			s.After(200*time.Millisecond, "timeout",
				model.Do(model.Assign(func(c Ctx, _ core.Event) Ctx {
					c.TimerFired = true
					return c
				})),
			)
		}).
		State("idle").
		State("timeout").
		MustBuild()

	svc := runtime.Start(m)
	defer svc.Stop()

	// Leave before the 200ms timer fires.
	ch := awaitState(svc, "idle")
	mustSend(t, svc, core.E("LEAVE"))
	assertState(t, ch, "idle")

	// Wait long enough that the timer would have fired if not cancelled.
	time.Sleep(300 * time.Millisecond)

	if svc.Snapshot().Context.TimerFired {
		t.Error("timer fired after state exit — it should have been cancelled")
	}
	if !svc.Snapshot().Is("idle") {
		t.Errorf("state = %q, want idle", svc.State())
	}
}

// ─── final state ─────────────────────────────────────────────────────────────

func TestService_FinalStateReported(t *testing.T) {
	m := model.New[struct{}]("m").
		Initial("active").
		State("active", func(s *model.StateBuilder[struct{}]) {
			s.On("DONE", "end")
		}).
		State("end", func(s *model.StateBuilder[struct{}]) {
			s.Final()
		}).
		MustBuild()

	svc := runtime.Start(m)
	defer svc.Stop()

	ch := awaitState(svc, "end")
	mustSend(t, svc, core.E("DONE"))
	assertState(t, ch, "end")

	if !svc.Snapshot().Final {
		t.Error("snapshot.Final should be true in final state")
	}
}

// ─── concurrent send safety ───────────────────────────────────────────────────

func TestService_ConcurrentSend(t *testing.T) {
	svc := runtime.Start(trafficLight)
	defer svc.Stop()

	const goroutines = 50
	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = svc.Send(core.E("TIMER"))
		}()
	}
	wg.Wait()
	// Must not race or panic. Final state is non-deterministic (that's fine).
}

func TestService_ConcurrentSubscribeAndSend(t *testing.T) {
	svc := runtime.Start(trafficLight)
	defer svc.Stop()

	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			unsub := svc.Subscribe(func(_ runtime.Snapshot[lightCtx]) {})
			time.Sleep(time.Millisecond)
			unsub()
		}()
	}
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = svc.Send(core.E("TIMER"))
		}()
	}
	wg.Wait()
}

// ─── TrySend ──────────────────────────────────────────────────────────────────

func TestService_TrySendFull(t *testing.T) {
	// Use a size-1 mailbox and fill it without the interpreter running.
	// We achieve this by stopping the service first — the goroutine exits
	// and the channel is no longer drained.
	m := model.New[struct{}]("m").Initial("a").State("a").MustBuild()
	svc := runtime.Start(m, runtime.WithMailboxSize(1))
	svc.Stop()

	// Service is stopped — TrySend must return ErrActorStopped, not ErrMailboxFull.
	err := svc.TrySend(core.E("X"))
	if err != core.ErrActorStopped {
		t.Errorf("TrySend on stopped service: got %v, want ErrActorStopped", err)
	}
}

// ─── invoke (async services) ──────────────────────────────────────────────────

func TestService_Invoke_SendsEventOnEntry(t *testing.T) {
	// An invoke that calls send() synchronously drives the machine to final.
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

	svc := runtime.Start(m)
	defer svc.Stop()

	select {
	case <-waitForFinal(svc):
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for final state")
	}
}

func TestService_Invoke_ContextCancelledOnExit(t *testing.T) {
	cancelled := make(chan struct{})

	m := model.New[struct{}]("m").
		Initial("active").
		State("active", func(s *model.StateBuilder[struct{}]) {
			s.Invoke(func(ctx context.Context, _ struct{}, _ core.Event, _ func(core.Event)) {
				go func() {
					<-ctx.Done()
					close(cancelled)
				}()
			})
			s.On("LEAVE", "done")
		}).
		State("done", func(s *model.StateBuilder[struct{}]) { s.Final() }).
		MustBuild()

	svc := runtime.Start(m)
	defer svc.Stop()

	ch := awaitState(svc, "done")
	mustSend(t, svc, core.E("LEAVE"))
	assertState(t, ch, "done")

	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("invoke context not cancelled after state exit")
	}
}

func TestService_Invoke_AsyncResult(t *testing.T) {
	// Invoke spawns a goroutine that sends an event after a short delay.
	m := model.New[struct{}]("m").
		Initial("fetching").
		State("fetching", func(s *model.StateBuilder[struct{}]) {
			s.Invoke(func(ctx context.Context, _ struct{}, _ core.Event, send func(core.Event)) {
				go func() {
					select {
					case <-ctx.Done():
						return
					case <-time.After(20 * time.Millisecond):
						send(core.E("DONE"))
					}
				}()
			})
			s.On("DONE", "complete")
		}).
		State("complete", func(s *model.StateBuilder[struct{}]) { s.Final() }).
		MustBuild()

	svc := runtime.Start(m)
	defer svc.Stop()

	select {
	case <-waitForFinal(svc):
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for async invoke result")
	}
}

func TestService_Invoke_CancelledOnStop(t *testing.T) {
	// Stopping the service cancels the active invocation's context.
	cancelled := make(chan struct{})

	m := model.New[struct{}]("m").
		Initial("running").
		State("running", func(s *model.StateBuilder[struct{}]) {
			s.Invoke(func(ctx context.Context, _ struct{}, _ core.Event, _ func(core.Event)) {
				go func() {
					<-ctx.Done()
					close(cancelled)
				}()
			})
		}).
		MustBuild()

	svc := runtime.Start(m)
	svc.Stop()

	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("invoke context not cancelled after service stop")
	}
}

func TestService_Invoke_MultipleInvokes(t *testing.T) {
	// Two invokes on the same state — both must start and both contexts
	// must be cancelled when the state is exited.
	var wg sync.WaitGroup
	wg.Add(2)

	m := model.New[struct{}]("m").
		Initial("active").
		State("active", func(s *model.StateBuilder[struct{}]) {
			for range 2 {
				s.Invoke(func(ctx context.Context, _ struct{}, _ core.Event, _ func(core.Event)) {
					go func() {
						<-ctx.Done()
						wg.Done()
					}()
				})
			}
			s.On("STOP", "done")
		}).
		State("done", func(s *model.StateBuilder[struct{}]) { s.Final() }).
		MustBuild()

	svc := runtime.Start(m)
	defer svc.Stop()

	ch := awaitState(svc, "done")
	mustSend(t, svc, core.E("STOP"))
	assertState(t, ch, "done")

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("not all invoke contexts were cancelled after state exit")
	}
}

// ─── hierarchical statecharts ─────────────────────────────────────────────────

// TestService_Hierarchical_InitialEntry verifies that starting a machine whose
// initial state is compound enters the full ancestor→leaf path.
func TestService_Hierarchical_InitialEntry(t *testing.T) {
	type Ctx struct{ Log []string }

	m := model.New[Ctx]("h").
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

	svc := runtime.Start(m)
	defer svc.Stop()

	snap := svc.Snapshot()
	if !snap.Is("idle") {
		t.Errorf("leaf state = %q, want idle", snap.State)
	}
	if !snap.In("active") {
		t.Error("active should be in configuration")
	}
	want := []string{"enter:active", "enter:idle"}
	if len(snap.Context.Log) != len(want) {
		t.Fatalf("entry log = %v, want %v", snap.Context.Log, want)
	}
	for i, w := range want {
		if snap.Context.Log[i] != w {
			t.Errorf("log[%d] = %q, want %q", i, snap.Context.Log[i], w)
		}
	}
}

// TestService_Hierarchical_SiblingTransition verifies that transitioning
// between sibling states exits and enters only the siblings, not the parent.
func TestService_Hierarchical_SiblingTransition(t *testing.T) {
	type Ctx struct{ Log []string }

	m := model.New[Ctx]("h").
		Initial("active").
		State("active", func(s *model.StateBuilder[Ctx]) {
			s.Entry(model.Assign(func(c Ctx, _ core.Event) Ctx {
				c.Log = append(c.Log, "enter:active")
				return c
			}))
			s.Exit(model.Assign(func(c Ctx, _ core.Event) Ctx {
				c.Log = append(c.Log, "exit:active")
				return c
			}))
			s.State("idle", func(s *model.StateBuilder[Ctx]) {
				s.Entry(model.Assign(func(c Ctx, _ core.Event) Ctx {
					c.Log = append(c.Log, "enter:idle")
					return c
				}))
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

	svc := runtime.Start(m)
	defer svc.Stop()

	ch := awaitState(svc, "running")
	mustSend(t, svc, core.E("RUN"))
	assertState(t, ch, "running")

	snap := svc.Snapshot()
	if !snap.In("active") {
		t.Error("active should still be in configuration after sibling transition")
	}
	// active must NOT have been re-entered.
	want := []string{"enter:active", "enter:idle", "exit:idle", "enter:running"}
	if len(snap.Context.Log) != len(want) {
		t.Fatalf("log = %v, want %v", snap.Context.Log, want)
	}
	for i, w := range want {
		if snap.Context.Log[i] != w {
			t.Errorf("log[%d] = %q, want %q", i, snap.Context.Log[i], w)
		}
	}
}

// TestService_Hierarchical_EventBubbling verifies that an unhandled event in a
// child state is handled by the parent.
func TestService_Hierarchical_EventBubbling(t *testing.T) {
	m := model.New[struct{}]("h").
		Initial("active").
		State("active", func(s *model.StateBuilder[struct{}]) {
			s.State("idle", func(s *model.StateBuilder[struct{}]) {
				s.On("INNER", "running")
			})
			s.State("running")
			// CANCEL is not handled by any child; it bubbles up to "active".
			s.On("CANCEL", "cancelled")
		}).
		State("cancelled", func(s *model.StateBuilder[struct{}]) { s.Final() }).
		MustBuild()

	svc := runtime.Start(m)
	defer svc.Stop()

	// CANCEL is not handled by "idle", it must bubble up to "active".
	ch := waitForFinal(svc)
	mustSend(t, svc, core.E("CANCEL"))
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out — event bubbling to parent did not fire; state=%q", svc.State())
	}
	if !svc.Snapshot().Is("cancelled") {
		t.Errorf("state = %q, want cancelled", svc.State())
	}
}

// TestService_Hierarchical_CrossLevelTransition verifies that transitioning
// from a nested state to a top-level state correctly exits all intermediate
// ancestors.
func TestService_Hierarchical_CrossLevelTransition(t *testing.T) {
	type Ctx struct{ Log []string }

	m := model.New[Ctx]("h").
		Initial("outer").
		State("outer", func(s *model.StateBuilder[Ctx]) {
			s.Entry(model.Assign(func(c Ctx, _ core.Event) Ctx {
				c.Log = append(c.Log, "enter:outer")
				return c
			}))
			s.Exit(model.Assign(func(c Ctx, _ core.Event) Ctx {
				c.Log = append(c.Log, "exit:outer")
				return c
			}))
			s.State("inner", func(s *model.StateBuilder[Ctx]) {
				s.Entry(model.Assign(func(c Ctx, _ core.Event) Ctx {
					c.Log = append(c.Log, "enter:inner")
					return c
				}))
				s.Exit(model.Assign(func(c Ctx, _ core.Event) Ctx {
					c.Log = append(c.Log, "exit:inner")
					return c
				}))
				s.On("ESCAPE", "done")
			})
		}).
		State("done", func(s *model.StateBuilder[Ctx]) {
			s.Entry(model.Assign(func(c Ctx, _ core.Event) Ctx {
				c.Log = append(c.Log, "enter:done")
				return c
			}))
			s.Final()
		}).
		MustBuild()

	svc := runtime.Start(m)
	defer svc.Stop()

	ch := waitForFinal(svc)
	mustSend(t, svc, core.E("ESCAPE"))
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for final state")
	}

	want := []string{"enter:outer", "enter:inner", "exit:inner", "exit:outer", "enter:done"}
	log := svc.Snapshot().Context.Log
	if len(log) != len(want) {
		t.Fatalf("log = %v, want %v", log, want)
	}
	for i, w := range want {
		if log[i] != w {
			t.Errorf("log[%d] = %q, want %q", i, log[i], w)
		}
	}
}

// TestService_Hierarchical_ActiveStates verifies Snapshot.ActiveStates and In().
func TestService_Hierarchical_ActiveStates(t *testing.T) {
	m := model.New[struct{}]("h").
		Initial("outer").
		State("outer", func(s *model.StateBuilder[struct{}]) {
			s.State("mid", func(s *model.StateBuilder[struct{}]) {
				s.State("leaf")
			})
		}).
		MustBuild()

	svc := runtime.Start(m)
	defer svc.Stop()

	snap := svc.Snapshot()
	if !snap.Is("leaf") {
		t.Errorf("State = %q, want leaf", snap.State)
	}
	for _, id := range []string{"outer", "mid", "leaf"} {
		if !snap.In(id) {
			t.Errorf("In(%q) = false, want true", id)
		}
	}
	want := []core.StateID{"outer", "mid", "leaf"}
	if len(snap.ActiveStates) != len(want) {
		t.Fatalf("ActiveStates = %v, want %v", snap.ActiveStates, want)
	}
	for i, w := range want {
		if snap.ActiveStates[i] != w {
			t.Errorf("ActiveStates[%d] = %q, want %q", i, snap.ActiveStates[i], w)
		}
	}
}

// TestService_Hierarchical_InvokeOnlyExitsForExitedState verifies that an
// invoke on a parent state is NOT cancelled during a sibling transition.
func TestService_Hierarchical_InvokeOnlyExitsForExitedState(t *testing.T) {
	var parentCancelled atomic.Int32

	m := model.New[struct{}]("h").
		Initial("active").
		State("active", func(s *model.StateBuilder[struct{}]) {
			s.Invoke(func(ctx context.Context, _ struct{}, _ core.Event, _ func(core.Event)) {
				go func() {
					<-ctx.Done()
					parentCancelled.Add(1)
				}()
			})
			s.State("idle", func(s *model.StateBuilder[struct{}]) {
				s.On("RUN", "running")
			})
			s.State("running", func(s *model.StateBuilder[struct{}]) {
				s.Final()
			})
		}).
		MustBuild()

	svc := runtime.Start(m)
	defer svc.Stop()

	ch := waitForFinal(svc)
	mustSend(t, svc, core.E("RUN"))
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for final state")
	}

	// Give the goroutine a moment — it should NOT have been cancelled.
	time.Sleep(30 * time.Millisecond)
	if parentCancelled.Load() != 0 {
		t.Error("parent invoke was cancelled during sibling transition — it should not be")
	}
}

// ─── parallel states ──────────────────────────────────────────────────────────

// parallelFormMachine is a reusable fixture: two independent field regions.
//
//	form (parallel)
//	  ├─ field1:  pristine ──EDIT──► dirty
//	  └─ field2:  pristine ──EDIT──► dirty
//	  └─ (on SUBMIT) ──► submitted
func parallelFormMachine() *model.Machine[struct{}] {
	return model.New[struct{}]("form").
		Initial("form").
		Parallel("form", func(s *model.StateBuilder[struct{}]) {
			s.State("field1", func(s *model.StateBuilder[struct{}]) {
				s.State("f1pristine", func(s *model.StateBuilder[struct{}]) {
					s.On("EDIT1", "f1dirty")
				})
				s.State("f1dirty")
			})
			s.State("field2", func(s *model.StateBuilder[struct{}]) {
				s.State("f2pristine", func(s *model.StateBuilder[struct{}]) {
					s.On("EDIT2", "f2dirty")
				})
				s.State("f2dirty")
			})
			s.On("SUBMIT", "submitted")
		}).
		State("submitted", func(s *model.StateBuilder[struct{}]) { s.Final() }).
		MustBuild()
}

// TestService_Parallel_InitialEntry verifies that starting a machine with a
// parallel state enters all regions simultaneously.
func TestService_Parallel_InitialEntry(t *testing.T) {
	svc := runtime.Start(parallelFormMachine())
	defer svc.Stop()

	snap := svc.Snapshot()
	for _, want := range []string{"form", "field1", "f1pristine", "field2", "f2pristine"} {
		if !snap.In(want) {
			t.Errorf("In(%q) = false, want true; ActiveStates=%v", want, snap.ActiveStates)
		}
	}
}

// TestService_Parallel_IndependentRegionTransitions verifies that an event
// handled in one region does not affect the other region.
func TestService_Parallel_IndependentRegionTransitions(t *testing.T) {
	svc := runtime.Start(parallelFormMachine())
	defer svc.Stop()

	ch := awaitState(svc, "f1dirty")
	mustSend(t, svc, core.E("EDIT1"))
	assertState(t, ch, "f1dirty")

	snap := svc.Snapshot()
	if !snap.In("f1dirty") {
		t.Error("field1 region should be in f1dirty")
	}
	// field2 region must be untouched.
	if !snap.In("f2pristine") {
		t.Errorf("field2 region should still be f2pristine; ActiveStates=%v", snap.ActiveStates)
	}
	if snap.In("f2dirty") {
		t.Error("field2 region should NOT be f2dirty")
	}
}

// TestService_Parallel_BothRegionsTransition verifies that two independent
// events each advance their own region.
func TestService_Parallel_BothRegionsTransition(t *testing.T) {
	svc := runtime.Start(parallelFormMachine())
	defer svc.Stop()

	mustSend(t, svc, core.E("EDIT1"))
	time.Sleep(20 * time.Millisecond)
	mustSend(t, svc, core.E("EDIT2"))
	time.Sleep(20 * time.Millisecond)

	snap := svc.Snapshot()
	for _, want := range []string{"form", "field1", "f1dirty", "field2", "f2dirty"} {
		if !snap.In(want) {
			t.Errorf("In(%q) = false; ActiveStates=%v", want, snap.ActiveStates)
		}
	}
}

// TestService_Parallel_ParentTransitionExitsAllRegions verifies that a
// transition on the parallel state itself exits all regions at once.
func TestService_Parallel_ParentTransitionExitsAllRegions(t *testing.T) {
	svc := runtime.Start(parallelFormMachine())
	defer svc.Stop()

	ch := waitForFinal(svc)
	mustSend(t, svc, core.E("SUBMIT"))
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for final state after SUBMIT")
	}

	snap := svc.Snapshot()
	if !snap.Is("submitted") {
		t.Errorf("State = %q, want submitted", snap.State)
	}
	// No region states should remain active.
	for _, gone := range []string{"form", "field1", "field2", "f1pristine", "f2pristine"} {
		if snap.In(gone) {
			t.Errorf("In(%q) = true after exiting parallel state; ActiveStates=%v", gone, snap.ActiveStates)
		}
	}
}

// TestService_Parallel_EntryExitOrder verifies the correct entry/exit order
// when entering and leaving a parallel state.
func TestService_Parallel_EntryExitOrder(t *testing.T) {
	type Ctx struct{ Log []string }

	m := model.New[Ctx]("m").
		Initial("par").
		Parallel("par", func(s *model.StateBuilder[Ctx]) {
			s.Entry(model.Assign(func(c Ctx, _ core.Event) Ctx {
				c.Log = append(c.Log, "enter:par")
				return c
			}))
			s.Exit(model.Assign(func(c Ctx, _ core.Event) Ctx {
				c.Log = append(c.Log, "exit:par")
				return c
			}))
			s.State("A", func(s *model.StateBuilder[Ctx]) {
				s.Entry(model.Assign(func(c Ctx, _ core.Event) Ctx {
					c.Log = append(c.Log, "enter:A")
					return c
				}))
				s.Exit(model.Assign(func(c Ctx, _ core.Event) Ctx {
					c.Log = append(c.Log, "exit:A")
					return c
				}))
				s.State("A1", func(s *model.StateBuilder[Ctx]) {
					s.Entry(model.Assign(func(c Ctx, _ core.Event) Ctx {
						c.Log = append(c.Log, "enter:A1")
						return c
					}))
				})
			})
			s.State("B", func(s *model.StateBuilder[Ctx]) {
				s.Entry(model.Assign(func(c Ctx, _ core.Event) Ctx {
					c.Log = append(c.Log, "enter:B")
					return c
				}))
				s.Exit(model.Assign(func(c Ctx, _ core.Event) Ctx {
					c.Log = append(c.Log, "exit:B")
					return c
				}))
				s.State("B1", func(s *model.StateBuilder[Ctx]) {
					s.Entry(model.Assign(func(c Ctx, _ core.Event) Ctx {
						c.Log = append(c.Log, "enter:B1")
						return c
					}))
				})
			})
			s.On("DONE", "final")
		}).
		State("final", func(s *model.StateBuilder[Ctx]) {
			s.Entry(model.Assign(func(c Ctx, _ core.Event) Ctx {
				c.Log = append(c.Log, "enter:final")
				return c
			}))
			s.Final()
		}).
		MustBuild()

	svc := runtime.Start(m)
	defer svc.Stop()

	ch := waitForFinal(svc)
	mustSend(t, svc, core.E("DONE"))
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out")
	}

	log := svc.Snapshot().Context.Log
	// Entry: par → A → A1 → B → B1 (parallel: par entered once, then each region)
	// Exit:  A1 (implicit from exiting A) then exit:A, B1 (implicit) then exit:B, exit:par
	// We only have explicit entry/exit logs on par, A, B (leaves A1/B1 only have entry).
	wantEntry := []string{"enter:par", "enter:A", "enter:A1", "enter:B", "enter:B1"}
	wantExit := []string{"exit:A", "exit:B", "exit:par"}
	wantTail := []string{"enter:final"}

	want := append(append(wantEntry, wantExit...), wantTail...)
	if len(log) != len(want) {
		t.Fatalf("log = %v\nwant %v", log, want)
	}
	for i, w := range want {
		if log[i] != w {
			t.Errorf("log[%d] = %q, want %q", i, log[i], w)
		}
	}
}

// TestService_Parallel_FinalWhenAllRegionsDone verifies that Snapshot.Final
// is true only when every region's active leaf is a final state.
func TestService_Parallel_FinalWhenAllRegionsDone(t *testing.T) {
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

	svc := runtime.Start(m)
	defer svc.Stop()

	if svc.Snapshot().Final {
		t.Error("Final should be false initially")
	}

	mustSend(t, svc, core.E("DONE_A"))
	time.Sleep(20 * time.Millisecond)
	if svc.Snapshot().Final {
		t.Error("Final should be false when only region A is done")
	}

	ch := waitForFinal(svc)
	mustSend(t, svc, core.E("DONE_B"))
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for both regions to be final")
	}
	if !svc.Snapshot().Final {
		t.Error("Final should be true when all regions are done")
	}
}

// ─── test helpers ─────────────────────────────────────────────────────────────

func mustSend(t *testing.T, svc interface{ Send(core.Event) error }, ev core.Event) {
	t.Helper()
	if err := svc.Send(ev); err != nil {
		t.Fatalf("Send: %v", err)
	}
}

// awaitState returns a channel that receives when the service enters id.
func awaitState[C any](svc *runtime.Service[C], id string) <-chan runtime.Snapshot[C] {
	ch := make(chan runtime.Snapshot[C], 1)
	var once sync.Once
	svc.Subscribe(func(snap runtime.Snapshot[C]) {
		if snap.Is(id) {
			once.Do(func() { ch <- snap })
		}
	})
	return ch
}

// awaitChange returns a channel that receives on the next state-changing snapshot.
func awaitChange[C any](svc *runtime.Service[C]) <-chan runtime.Snapshot[C] {
	ch := make(chan runtime.Snapshot[C], 1)
	var once sync.Once
	svc.Subscribe(func(snap runtime.Snapshot[C]) {
		if snap.Changed {
			once.Do(func() { ch <- snap })
		}
	})
	return ch
}

// waitForFinal returns a channel closed when the service reaches a final state.
func waitForFinal[C any](svc *runtime.Service[C]) <-chan struct{} {
	ch := make(chan struct{})
	var once sync.Once
	svc.Subscribe(func(snap runtime.Snapshot[C]) {
		if snap.Final {
			once.Do(func() { close(ch) })
		}
	})
	return ch
}

func assertState[C any](t *testing.T, ch <-chan runtime.Snapshot[C], want string) {
	t.Helper()
	select {
	case snap := <-ch:
		if !snap.Is(want) {
			t.Errorf("state = %q, want %q", snap.State, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for state %q", want)
	}
}

// ─── done events ──────────────────────────────────────────────────────────────

// TestDone_CompoundStateFires verifies that completing a compound state's
// final child automatically fires and resolves the OnDone transition.
func TestDone_CompoundStateFires(t *testing.T) {
	m := model.New[struct{}]("m").
		Initial("wizard").
		State("wizard", func(s *model.StateBuilder[struct{}]) {
			s.State("step1", func(s *model.StateBuilder[struct{}]) {
				s.On("NEXT", "step2")
			})
			s.State("step2", func(s *model.StateBuilder[struct{}]) {
				s.Final()
			})
			s.OnDone("done")
		}).
		State("done", func(s *model.StateBuilder[struct{}]) { s.Final() }).
		MustBuild()

	svc := runtime.Start(m)
	defer svc.Stop()

	ch := waitForFinal(svc)
	if err := svc.Send(core.E("NEXT")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for final state")
	}
	snap := svc.Snapshot()
	if !snap.Is("done") {
		t.Errorf("state = %q, want done", snap.State)
	}
}

// TestDone_ParallelStateFires verifies that a parallel state fires done when
// ALL regions reach a final state.
func TestDone_ParallelStateFires(t *testing.T) {
	m := model.New[struct{}]("m").
		Initial("par").
		Parallel("par", func(s *model.StateBuilder[struct{}]) {
			s.State("regionA", func(s *model.StateBuilder[struct{}]) {
				s.State("a_active", func(s *model.StateBuilder[struct{}]) {
					s.On("DONE_A", "a_final")
				})
				s.State("a_final", func(s *model.StateBuilder[struct{}]) { s.Final() })
			})
			s.State("regionB", func(s *model.StateBuilder[struct{}]) {
				s.State("b_active", func(s *model.StateBuilder[struct{}]) {
					s.On("DONE_B", "b_final")
				})
				s.State("b_final", func(s *model.StateBuilder[struct{}]) { s.Final() })
			})
			s.OnDone("complete")
		}).
		State("complete", func(s *model.StateBuilder[struct{}]) { s.Final() }).
		MustBuild()

	svc := runtime.Start(m)
	defer svc.Stop()

	ch := waitForFinal(svc)

	if err := svc.Send(core.E("DONE_A")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	// Only region A is done — should NOT have fired done yet.
	if svc.Snapshot().Is("complete") {
		t.Fatal("done fired too early — both regions must be final first")
	}

	if err := svc.Send(core.E("DONE_B")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for final state")
	}
	if !svc.Snapshot().Is("complete") {
		t.Errorf("state = %q, want complete", svc.Snapshot().State)
	}
}

// TestDone_Cascades verifies that done events cascade: completing an inner
// compound state can propagate and complete an outer compound state.
func TestDone_Cascades(t *testing.T) {
	// outer → inner → leaf (final) → inner.OnDone → inner_done (final)
	//                                             → outer.OnDone → finished
	m := model.New[struct{}]("m").
		Initial("outer").
		State("outer", func(s *model.StateBuilder[struct{}]) {
			s.State("inner", func(s *model.StateBuilder[struct{}]) {
				s.State("leaf", func(s *model.StateBuilder[struct{}]) { s.Final() })
				s.OnDone("inner_done")
			})
			s.State("inner_done", func(s *model.StateBuilder[struct{}]) { s.Final() })
			s.OnDone("finished")
		}).
		State("finished", func(s *model.StateBuilder[struct{}]) { s.Final() }).
		MustBuild()

	svc := runtime.Start(m)
	defer svc.Stop()

	ch := waitForFinal(svc)
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out — cascading done events did not resolve")
	}
	if !svc.Snapshot().Is("finished") {
		t.Errorf("state = %q, want finished", svc.Snapshot().State)
	}
}

// TestDone_OnDoneWithGuard verifies that a guarded OnDone only fires when the
// guard passes.
func TestDone_OnDoneWithGuard(t *testing.T) {
	type Ctx struct{ OK bool }

	m := model.New[Ctx]("m").
		Initial("flow").
		State("flow", func(s *model.StateBuilder[Ctx]) {
			s.State("step", func(s *model.StateBuilder[Ctx]) { s.Final() })
			s.OnDone("success",
				model.When[Ctx](func(c Ctx, _ core.Event) bool { return c.OK }),
			)
			s.OnDone("failure")
		}).
		State("success", func(s *model.StateBuilder[Ctx]) { s.Final() }).
		State("failure", func(s *model.StateBuilder[Ctx]) { s.Final() }).
		MustBuild()

	// Guard passes — should go to success.
	svc1 := runtime.Start(model.New[Ctx]("m").
		Initial("flow").
		State("flow", func(s *model.StateBuilder[Ctx]) {
			s.State("step", func(s *model.StateBuilder[Ctx]) { s.Final() })
			s.OnDone("success",
				model.When[Ctx](func(c Ctx, _ core.Event) bool { return c.OK }),
			)
			s.OnDone("failure")
		}).
		State("success", func(s *model.StateBuilder[Ctx]) { s.Final() }).
		State("failure", func(s *model.StateBuilder[Ctx]) { s.Final() }).
		Context(Ctx{OK: true}).
		MustBuild(),
	)
	defer svc1.Stop()
	time.Sleep(30 * time.Millisecond)
	if !svc1.Snapshot().Is("success") {
		t.Errorf("guard=true: state = %q, want success", svc1.Snapshot().State)
	}

	// Guard fails — should fall through to failure.
	svc2 := runtime.Start(m)
	defer svc2.Stop()
	time.Sleep(30 * time.Millisecond)
	if !svc2.Snapshot().Is("failure") {
		t.Errorf("guard=false: state = %q, want failure", svc2.Snapshot().State)
	}
}
