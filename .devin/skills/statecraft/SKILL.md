---
name: statecraft
description: Build and modify statecraft FSM/statechart definitions, runtime behavior, and tests
argument-hint: "[task description]"
allowed-tools:
  - read
  - grep
  - glob
  - exec
triggers:
  - user
  - model
---

You are working on **statecraft**, a Go statechart runtime with flat FSMs, compound states, parallel regions, actor supervision, and JSON persistence.

## Quick reference

**Flat FSM:**

```go
m := statecraft.New[MyCtx]("id").
    Context(MyCtx{}).
    Initial("idle").
    State("idle", func(s *statecraft.StateBuilder[MyCtx]) {
        s.On("START", "running", statecraft.When(guard), statecraft.Do(actions...))
        s.Always("other", statecraft.When(guard))
        s.After(time.Second, "timeout")
        s.Entry(actions...)
        s.Exit(actions...)
        s.Invoke(invokeFn)
        s.Final()
    }).
    MustBuild()
```

**Compound (OR) state:**

```go
s.State("checkout", func(s *statecraft.StateBuilder[Ctx]) {
    s.Initial("cart")
    s.State("cart", func(s *statecraft.StateBuilder[Ctx]) {
        s.On("SUBMIT", "payment")
    })
    s.State("payment", func(s *statecraft.StateBuilder[Ctx]) {
        s.Final()
    })
    s.OnDone("confirmed") // fires when payment (final) is reached
    s.On("CANCEL", "cancelled") // parent handles events unhandled by children
})
```

**Parallel (AND) state:**

```go
s.Parallel("editor", func(s *statecraft.StateBuilder[Ctx]) {
    s.State("content", func(s *statecraft.StateBuilder[Ctx]) {
        s.State("viewing")
        s.State("editing")
    })
    s.State("sync", func(s *statecraft.StateBuilder[Ctx]) {
        s.State("clean")
        s.State("dirty")
        s.State("saving", func(s *statecraft.StateBuilder[Ctx]) { s.Final() })
    })
    s.OnDone("finished") // fires when ALL regions have a final active leaf
})
```

**Run it:**

```go
svc := statecraft.Start(m, statecraft.WithClock(clock), statecraft.WithMailboxSize(256))
defer svc.Stop()
svc.Send(statecraft.E("START"))
snap := svc.Snapshot()
snap.State         // first active leaf
snap.Leaves        // all active leaf states (one per parallel region)
snap.ActiveStates  // full configuration outermost-first
snap.In("editor")  // true if inside compound/parallel state
```

**Actor system:**

```go
sys := statecraft.NewSystem()
ref, _ := statecraft.Spawn(sys, "worker", machine,
    statecraft.WithActorStrategy(statecraft.RestartAlways, 5))
ref.Send(statecraft.E("PING"))
sys.Stop()
```

**Persistence:**

```go
data, _ := statecraft.Save(svc)        // JSON checkpoint (context + leaf states)
svc2, _ := statecraft.Restore(m, data) // resume; entry actions NOT re-run
```

**Visualization:**

```go
fmt.Println(viz.ToMermaid(m))    // mermaid stateDiagram-v2
fmt.Println(viz.ToGraphviz(m))   // Graphviz DOT with clusters
```

## Event loop order

1. Drain internal queue (`Raise` from actions, done events)
2. Evaluate `Always` transitions (max 1000 iterations, then panic)
3. Wait for mailbox events or scheduler timer fires

## Transition execution order (SCXML macrostep)

exit actions → cancel timers/invokes → transition actions → update leaves → entry actions → start timers/invokes → drain action-raised events → raise done events → snapshot + notify subscribers

## Done events

`s.OnDone("target")` is sugar for `s.On("done.state."+id, "target")`.

- Compound OR: fires when the active leaf inside the state is final.
- Parallel AND: fires when every region has a final active leaf.
- Cascades automatically: inner done → transition → outer done → transition.
- `raiseDoneEvents(entryOrder)` in both `runtime/service.go` and `testkit/harness.go`.

## Synchronization rule

`testkit/harness.go` implements the same SCXML macrostep as `runtime/service.go`. Any change to the interpreter (applyTransitions, raiseDoneEvents, done event check) **must be mirrored** in the harness. Both use the same helpers: `compoundIsDone`, `raiseDoneEvents`.

## Testing

```go
h := testkit.NewHarness(m)
h.MustTransition(t, core.E("LOGIN"))
h.AssertState(t, "authenticating")
h.AssertIn(t, "compound_parent")  // compound/parallel membership
h.AssertLeaves(t, "regionA_leaf", "regionB_leaf")
h.Tick(2 * time.Second)           // fire After timers
h.AssertFinal(t)
```

## Files to read first

- `statecraft.go` — public API
- `model/builder.go` — declaration API (State, Parallel, OnDone, After, Always)
- `model/machine.go` — LCCA, ExitPath, EntryPath, IsDescendantOf, ConfigurationOf
- `runtime/service.go` — interpreter, applyTransitions, raiseDoneEvents
- `examples/doceditor/main.go` — parallel + OnDone + Save/Restore in action

## Rules

- Do not add external dependencies.
- Guards: pure predicates only.
- Model layer: no goroutines or channels.
- Mirror harness when changing interpreter semantics.
- Run `go test ./...` after changes.

$ARGUMENTS
