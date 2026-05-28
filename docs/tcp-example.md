# TCP connection state machine (RFC 793)

`examples/tcp` implements the full TCP connection state machine from RFC 793 using statecraft as an Extended Finite Automaton.

The interesting thing about this example is the gap between the problem size and the model size. TCP has an enormous state space when you factor in sequence numbers, window sizes, retransmit queues, and timers , but the *connection lifecycle* is described by exactly 11 named states. The state machine captures what a connection *is doing* (establishing, transferring, tearing down), while the typed context carries the numeric state that guards and actions operate on.

## The formal model

```
Q  = { CLOSED, LISTEN, SYN_SENT, SYN_RECEIVED, ESTABLISHED,
       FIN_WAIT_1, FIN_WAIT_2, CLOSE_WAIT, CLOSING, LAST_ACK, TIME_WAIT }
Σ  = { ACTIVE_OPEN, PASSIVE_OPEN, CLOSE, SEND, DATA,
       SYN, SYN_ACK, ACK, FIN, RST }
δ  = ResolveTransition, guarded by TCPCtx (sequence numbers)
q0 = CLOSED
F  = { CLOSED }
```

This maps directly to the statecraft 5-tuple. Each state in Q is a `State(...)` call; each symbol in Σ is an event type string; δ is `machine.ResolveTransition`; guards on sequence numbers are `statecraft.When(fn)` predicates on the context.

## State diagram

![tcp-rfc793](diagrams/tcp-rfc793.svg)

## How the three paths work

### Active open (client)

```
CLOSED  --[ACTIVE_OPEN]-->  SYN_SENT    sends SYN
SYN_SENT --[SYN_ACK]-->    ESTABLISHED  sends ACK
```

The three-way handshake is the textbook example of why state matters: the same `ACK` event means something completely different in `SYN_SENT` vs `ESTABLISHED` vs `FIN_WAIT_1`. The machine routes it correctly because the *current state* is part of δ.

### Four-way teardown (active close)

```
ESTABLISHED --[CLOSE]-->   FIN_WAIT_1  sends FIN
FIN_WAIT_1  --[ACK]-->     FIN_WAIT_2
FIN_WAIT_2  --[FIN]-->     TIME_WAIT   sends ACK
TIME_WAIT   --[2*MSL]-->   CLOSED
```

`TIME_WAIT` is a direct expression of the RFC: the connection stays here for two Maximum Segment Lifetimes to absorb any delayed duplicates still in the network. In statecraft this is just `s.After(2*MSL, "CLOSED")` , a delayed transition with no extra machinery.

### Passive close (server-side teardown)

```
ESTABLISHED --[FIN]-->   CLOSE_WAIT  sends ACK
CLOSE_WAIT  --[CLOSE]--> LAST_ACK    sends FIN
LAST_ACK    --[ACK]-->   CLOSED
```

### Simultaneous close

When both sides send FIN at the same time:

```
FIN_WAIT_1 --[FIN]--> CLOSING  sends ACK
CLOSING    --[ACK]--> TIME_WAIT
```

This path exists in the RFC and is modelled here , it never appears in the normal demo run but is handled correctly.

## Invokes as peer simulation

Three `Invoke` callbacks simulate the remote peer so the demo drives itself without manual event injection:

| State | Invoke fires after | Sends |
|---|---|---|
| `SYN_SENT` | 1 RTT | `SYN_ACK` |
| `FIN_WAIT_1` | 1 RTT | `ACK` |
| `FIN_WAIT_2` | 1 RTT | `FIN` |

Each invoke runs in a goroutine and respects `ctx.Done()` , if the state is exited before the timer fires (e.g. an RST arrives), the goroutine exits cleanly. This is the standard statecraft invoke pattern.

## Context: sequence numbers

```go
type TCPCtx struct {
    ISS     uint32  // Initial Send Sequence number
    SND_NXT uint32  // next sequence number to send
    RCV_NXT uint32  // next sequence number expected from peer
    Sent    int
    Recv    int
}
```

Actions are pure functions over this context , each one returns a new `TCPCtx` with updated fields. No mutation, no shared state. The machine serialises all transitions through its event loop, so there are no races even though invokes run in separate goroutines.

## Sample output

```
── three-way handshake ──────────────────────────────
  → SYN       seq=4958
  ← SYN,ACK   seq=0      ack=4959
  → ACK       seq=4959   ack=1
SYN_SENT       → ESTABLISHED

── data transfer ────────────────────────────────────
  → DATA      seq=4960   len=12
  ← DATA      seq=1      len=12

── four-way teardown ────────────────────────────────
  → FIN       seq=4972   ack=13
ESTABLISHED    → FIN_WAIT_1
  ← ACK       seq=13     ack=4973
FIN_WAIT_1     → FIN_WAIT_2
  ← FIN       seq=14     ack=4973
  → ACK       seq=4973   ack=15
FIN_WAIT_2     → TIME_WAIT
TIME_WAIT      → CLOSED

── connection released ──────────────────────────────
final state : CLOSED
seq/ack     : SND_NXT=4974   RCV_NXT=15
segments    : sent=5  recv=4
```

## Running it

```bash
go run ./examples/tcp
```

Requires graphviz for the SVG (`brew install graphviz`); the Mermaid diagram prints to stdout regardless.
