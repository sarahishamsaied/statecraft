# [statecraft](https://app.devin.ai/org/sarahishamsaied/wiki/sarahishamsaied/statecraft)

A concurrency-first statechart runtime for Go.

[statecraft](https://app.devin.ai/org/sarahishamsaied/wiki/sarahishamsaied/statecraft)
 lets you model complex application logic as hierarchical state machines ---> with compound states, parallel regions, guarded transitions, async invocations, actor supervision, and JSON persistence ---> all in idiomatic Go with no external dependencies.

```go
m := statecraft.New[TrafficCtx]("traffic-light").
    Initial("red").
    State("red",    func(s *statecraft.StateBuilder[TrafficCtx]) { s.On("TIMER", "green") }).
    State("green",  func(s *statecraft.StateBuilder[TrafficCtx]) { s.On("TIMER", "yellow") }).
    State("yellow", func(s *statecraft.StateBuilder[TrafficCtx]) { s.On("TIMER", "red") }).
    MustBuild()

svc := statecraft.Start(m)
defer svc.Stop()
svc.Send(statecraft.E("TIMER"))
fmt.Println(svc.State()) // "green"
```

## Features

- **Flat, compound, and parallel states** ---> OR and AND regions, event bubbling, LCCA-based exit/entry
- **Done events** ---> `s.OnDone("next")` fires automatically when a compound/parallel state completes
- **Delayed transitions** ---> `s.After(500*time.Millisecond, "timeout")`
- **Async invocations** ---> long-running side effects scoped to a state's lifetime
- **Actor system** ---> named actors with supervision strategies (NoRestart, RestartAlways, RestartN)
- **Persistence** ---> `Save` / `Restore` across process restarts via JSON checkpoints
- **Visualization** ---> Mermaid and Graphviz diagrams from any compiled machine
- **Testkit** ---> synchronous harness for deterministic unit tests, no goroutines needed

## Diagrams

**Traffic light** — flat FSM

![traffic-light](docs/diagrams/traffic-light.svg)

**Auth flow** — guards, delayed transitions, final state

![auth-flow](docs/diagrams/auth-flow.svg)

**Doc editor** — parallel regions, OnDone

![doc-editor](docs/diagrams/doc-editor.svg)

> Regenerate with `go run ./cmd/viz` (requires `graphviz`: `brew install graphviz`)

## Quick start

```bash
go test ./...
go run ./examples/trafficlight
go run ./examples/doceditor   # parallel regions + persistence
go run ./cmd/viz               # regenerate SVG diagrams → docs/diagrams/
```

## Prior art & research

statecraft stands on the shoulders of these works:

| Paper | What it contributes |
|-------|-------------------|
| Harel, D. ---> *Statecharts: A Visual Formalism for Complex Systems* (1987) | The core model: hierarchy, orthogonality, history, broadcast events |
| Harel & Pnueli ---> *On the Development of Reactive Systems* (1985) | Defines reactive systems and why statecharts are the right tool |
| W3C ---> *SCXML: State Machine Notation for Control Abstraction* (2015) | The normative macrostep algorithm statecraft implements |
| Hewitt, Bishop & Steiger ---> *A Universal Modular ACTOR Formalism* (1973) | Actor model: private mailbox, async message passing |
| Agha, G. ---> *Actors: A Model of Concurrent Computation* (1985) | Formal actor semantics: mail queues, behavior replacement |
| Armstrong, J. ---> *Making Reliable Distributed Systems* (2003) | Erlang/OTP supervision trees and let-it-crash philosophy |
 
more on this: [check out devin docs](https://app.devin.ai/org/sarahishamsaied/wiki/sarahishamsaied/statecraft)


