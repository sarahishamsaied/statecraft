# AGENTS.md — statecraft

Instructions for AI agents (Devin, Cursor, Claude Code, etc.) working on this repository.

## Project

**statecraft** is a concurrency-first Go statechart runtime. Module path: `statecraft`, Go 1.23+.

Public API lives in the root `statecraft` package. Internal layers:

| Package | Role |
|---------|------|
| `core/` | Events, StateID, Clock, errors — no internal deps |
| `model/` | Builder → compile → immutable `Machine` — pure, no goroutines |
| `runtime/` | Actor-style interpreter, timers, snapshots, subscriptions, Restore |
| `actor/` | Named actor registry with supervision (NoRestart, RestartAlways, RestartN) |
| `persist/` | JSON snapshot persistence — Save / Restore across process restarts |
| `viz/` | Mermaid / Graphviz export, compound and parallel state aware |
| `testkit/` | Synchronous harness for deterministic unit tests |
| `examples/` | Runnable demos (trafficlight, authflow, doceditor) |
| `bench/` | Benchmarks |

## Setup commands

```bash
# Run all tests
go test ./...

# Run a single package
go test ./runtime/...

# Run an example
go run ./examples/trafficlight
go run ./examples/authflow
go run ./examples/doceditor

# Benchmarks
go test -bench=. ./bench/...
```

No external dependencies beyond the Go standard library.

## Architecture constraints

- **One goroutine owns mutable state** in `runtime.Service` — do not add mutexes around interpreter state.
- **`Machine` is immutable** after `Build()` — safe to share across many `Service` instances.
- **Guards must be pure** — no side effects; first matching guard wins in definition order.
- **Actions return updated context** — use `Assign`, `Log`, or `Raise` helpers; side effects go through `ActionContext`.
- **Event priority** (SCXML-style): internal queue → always transitions → mailbox/timers.
- **Timers are state-scoped** — cancelled on exit; synthetic event types use `statecraft.after.*` prefix.
- **Done events are internal** — `done.state.X` is injected after entering states; processed before any external event.
- **Compile-time validation** in `model/compile.go` — prefer failing at `Build()` over runtime panics.
- **Parallel leaf ordering** — `leaves[0]` always tracks the first declared region; in-place replacement preserves order.

## Hierarchical / parallel states

- `s.State("id", configure)` — compound OR state; exactly one child is active at a time.
- `s.Parallel("id", configure)` — AND state; all children (regions) are active simultaneously.
- Event bubbling: unhandled events walk up the ancestor chain.
- LCCA-based exit/entry: shared ancestors are not re-entered on sibling transitions.
- `s.OnDone("target")` — fires when a compound state's active leaf is final, or when all parallel regions contain a final leaf. Cascades naturally through the internal queue.

## Actor runtime

```go
sys := statecraft.NewSystem()
ref, _ := statecraft.Spawn(sys, "name", machine,
    statecraft.WithActorStrategy(statecraft.RestartAlways, 3))
ref.Send(statecraft.E("EVENT"))
sys.Stop()
```

Actors are supervised. On panic, the strategy controls restart behavior.

## Persistence

```go
data, _ := statecraft.Save(svc)        // JSON checkpoint
svc2, _ := statecraft.Restore(m, data) // resume; entry actions NOT re-run
```

Entry actions are not replayed (context already serialized). Timers and invokes restart fresh.

## Code style

- Match existing naming: `Builder`, `StateBuilder`, `ActionFn`, `GuardFn`, `InvokeFn`.
- Keep layers separated — `model` must not import `runtime`; `viz` depends only on `model`.
- Minimize scope in diffs; avoid over-abstraction.
- Comments only for non-obvious behavior (event loop ordering, concurrency ownership).
- Re-export public types from `statecraft.go` rather than exposing internal packages to users.

## Testing guidelines

- Use `testkit.NewHarness(m)` for synchronous, deterministic tests (no goroutines).
- Use `runtime.WithClock(mockClock)` for timer integration tests in `runtime`.
- Use `h.AssertIn(t, "state")` for compound/parallel membership; `h.AssertLeaves(t, "a", "b")` for exact parallel leaf assertions.
- Assert transition paths with `h.AssertSteps(t, "idle→running", ...)`.
- Keep `testkit/harness.go` in sync with `runtime/service.go` when changing interpreter semantics — both implement the same SCXML macrostep algorithm.
- Run `go test ./...` before committing.

## Common tasks

### Add a new state or transition

Edit the `Builder` chain in tests or examples, then `MustBuild()`. Validation runs in `compile()`.

### Add a compound or parallel state

```go
s.State("checkout", func(s *statecraft.StateBuilder[Ctx]) {
    s.Initial("cart")
    s.State("cart", ...)
    s.State("payment", func(s *statecraft.StateBuilder[Ctx]) { s.Final() })
    s.OnDone("confirmed")
})

s.Parallel("editor", func(s *statecraft.StateBuilder[Ctx]) {
    s.State("content", ...)
    s.State("sync", ...)
    s.OnDone("finished")
})
```

### Add a runtime feature

Changes usually touch `runtime/service.go` (event loop / applyTransitions) and require matching updates in `testkit/harness.go` to keep test/runtime behavior aligned.

### Visualize a machine

```go
import "statecraft/viz"
fmt.Println(viz.ToMermaid(m))
fmt.Println(viz.ToGraphviz(m))
```

## Devin CLI

Project skills live in `.devin/skills/`. Run `/statecraft` in Devin CLI for state-machine-specific guidance.
