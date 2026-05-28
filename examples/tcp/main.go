// Package main implements the RFC 793 TCP connection state machine using statecraft.
//
// The 11 states from the spec map directly to Q; the 10 segment types and user
// calls map to Σ; guarded transitions on sequence numbers map to δ(q, C, σ).
//
//	Q  = { CLOSED, LISTEN, SYN_SENT, SYN_RECEIVED, ESTABLISHED,
//	       FIN_WAIT_1, FIN_WAIT_2, CLOSE_WAIT, CLOSING, LAST_ACK, TIME_WAIT }
//	Σ  = { ACTIVE_OPEN, PASSIVE_OPEN, CLOSE, SEND, DATA,
//	       SYN, SYN_ACK, ACK, FIN, RST }
//	δ  = ResolveTransition , guarded by context (sequence numbers, flags)
//	q0 = CLOSED
//	F  = { CLOSED }   (connection fully released)
//
// Three Invokes simulate the remote peer, so the machine drives itself:
//   - SYN_SENT    → peer sends SYN_ACK after one RTT
//   - FIN_WAIT_1  → peer sends ACK after one RTT
//   - FIN_WAIT_2  → peer sends FIN after one RTT
//
// TIME_WAIT uses s.After(2*MSL, "CLOSED") , a native statecraft delayed
// transition, matching the RFC verbatim.
//
// Run with:
//
//	go run ./examples/tcp
package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"statecraft"
	"statecraft/core"
	"statecraft/model"
	"statecraft/viz"
	"strings"
	"time"
)

// ─── timing ──────────────────────────────────────────────────────────────────

const (
	RTT = 200 * time.Millisecond // simulated round-trip time
	MSL = 500 * time.Millisecond // Maximum Segment Lifetime (real = 2 min; scaled for demo)
)

// ─── context ─────────────────────────────────────────────────────────────────

// TCPCtx tracks the sequence-number state of one endpoint.
// Real TCP carries more fields; these are enough to print authentic headers.
type TCPCtx struct {
	ISS     uint32 // Initial Send Sequence number (set when SYN is sent)
	SND_NXT uint32 // next sequence number we will send
	RCV_NXT uint32 // next sequence number we expect to receive
	Sent    int    // total segments sent
	Recv    int    // total segments received
}

// ─── segment helpers ─────────────────────────────────────────────────────────

func logSend(flags string) model.ActionFn[TCPCtx] {
	return statecraft.Assign(func(c TCPCtx, _ core.Event) TCPCtx {
		fmt.Printf("  → %-9s seq=%-6d ack=%d\n", flags, c.SND_NXT, c.RCV_NXT)
		c.SND_NXT++
		c.Sent++
		return c
	})
}

func logRecv(flags string) model.ActionFn[TCPCtx] {
	return statecraft.Assign(func(c TCPCtx, _ core.Event) TCPCtx {
		fmt.Printf("  ← %-9s seq=%-6d ack=%d\n", flags, c.RCV_NXT, c.SND_NXT)
		c.RCV_NXT++
		c.Recv++
		return c
	})
}

var (
	sendSYN = statecraft.Assign(func(c TCPCtx, _ core.Event) TCPCtx {
		c.ISS = rand.Uint32() % 10000
		c.SND_NXT = c.ISS
		fmt.Printf("  → %-9s seq=%-6d\n", "SYN", c.ISS)
		c.SND_NXT++
		c.Sent++
		return c
	})

	recvSYNACK = statecraft.Assign(func(c TCPCtx, _ core.Event) TCPCtx {
		fmt.Printf("  ← %-9s seq=%-6d ack=%d\n", "SYN,ACK", c.RCV_NXT, c.SND_NXT)
		c.RCV_NXT++
		c.Recv++
		return c
	})

	sendACK = logSend("ACK")
	sendFIN = logSend("FIN")
	recvFIN = logRecv("FIN")
	recvACK = logRecv("ACK")

	sendData = statecraft.Assign(func(c TCPCtx, _ core.Event) TCPCtx {
		fmt.Printf("  → %-9s seq=%-6d len=12\n", "DATA", c.SND_NXT)
		c.SND_NXT += 12
		c.Sent++
		return c
	})
	recvData = statecraft.Assign(func(c TCPCtx, _ core.Event) TCPCtx {
		fmt.Printf("  ← %-9s seq=%-6d len=12\n", "DATA", c.RCV_NXT)
		c.RCV_NXT += 12
		c.Recv++
		return c
	})
)

// ─── machine ─────────────────────────────────────────────────────────────────

func buildMachine() *model.Machine[TCPCtx] {
	return statecraft.New[TCPCtx]("tcp-rfc793").
		Context(TCPCtx{}).
		Initial("CLOSED").

		// ── CLOSED ────────────────────────────────────────────────────────
		State("CLOSED", func(s *statecraft.StateBuilder[TCPCtx]) {
			s.On("ACTIVE_OPEN", "SYN_SENT", statecraft.Do(sendSYN))
			s.On("PASSIVE_OPEN", "LISTEN")
		}).

		// ── LISTEN ────────────────────────────────────────────────────────
		State("LISTEN", func(s *statecraft.StateBuilder[TCPCtx]) {
			s.On("SYN", "SYN_RECEIVED",
				statecraft.Do(recvACK), statecraft.Do(logSend("SYN,ACK")))
			s.On("CLOSE", "CLOSED")
		}).

		// ── SYN_SENT ──────────────────────────────────────────────────────
		// Invoke simulates the remote peer responding after one RTT.
		State("SYN_SENT", func(s *statecraft.StateBuilder[TCPCtx]) {
			s.On("SYN_ACK", "ESTABLISHED",
				statecraft.Do(recvSYNACK), statecraft.Do(sendACK))
			s.On("SYN", "SYN_RECEIVED", // simultaneous open
				statecraft.Do(recvACK), statecraft.Do(logSend("SYN,ACK")))
			s.On("RST", "CLOSED")
			s.On("CLOSE", "CLOSED")
			s.Invoke(func(ctx context.Context, _ TCPCtx, _ core.Event, send func(core.Event)) {
				go func() {
					select {
					case <-ctx.Done():
					case <-time.After(RTT):
						send(statecraft.E("SYN_ACK"))
					}
				}()
			})
		}).

		// ── SYN_RECEIVED ──────────────────────────────────────────────────
		State("SYN_RECEIVED", func(s *statecraft.StateBuilder[TCPCtx]) {
			s.On("ACK", "ESTABLISHED")
			s.On("RST", "LISTEN")
			s.On("CLOSE", "FIN_WAIT_1", statecraft.Do(sendFIN))
		}).

		// ── ESTABLISHED ───────────────────────────────────────────────────
		State("ESTABLISHED", func(s *statecraft.StateBuilder[TCPCtx]) {
			s.On("SEND", "ESTABLISHED", statecraft.Do(sendData))
			s.On("DATA", "ESTABLISHED", statecraft.Do(recvData))
			s.On("CLOSE", "FIN_WAIT_1", statecraft.Do(sendFIN))
			s.On("FIN", "CLOSE_WAIT", statecraft.Do(recvFIN), statecraft.Do(sendACK))
			s.On("RST", "CLOSED")
		}).

		// ── FIN_WAIT_1 ────────────────────────────────────────────────────
		// Invoke simulates peer ACK-ing our FIN after one RTT.
		State("FIN_WAIT_1", func(s *statecraft.StateBuilder[TCPCtx]) {
			s.On("ACK", "FIN_WAIT_2", statecraft.Do(recvACK))
			s.On("FIN", "CLOSING", // simultaneous close
				statecraft.Do(recvFIN), statecraft.Do(sendACK))
			s.On("FIN_ACK", "TIME_WAIT", // peer ACK+FIN in one segment
				statecraft.Do(recvACK), statecraft.Do(sendACK))
			s.Invoke(func(ctx context.Context, _ TCPCtx, _ core.Event, send func(core.Event)) {
				go func() {
					select {
					case <-ctx.Done():
						return
					case <-time.After(RTT):
					}
					send(statecraft.E("ACK"))
				}()
			})
		}).

		// ── FIN_WAIT_2 ────────────────────────────────────────────────────
		// Invoke simulates peer sending its own FIN after one RTT.
		State("FIN_WAIT_2", func(s *statecraft.StateBuilder[TCPCtx]) {
			s.On("FIN", "TIME_WAIT", statecraft.Do(recvFIN), statecraft.Do(sendACK))
			s.Invoke(func(ctx context.Context, _ TCPCtx, _ core.Event, send func(core.Event)) {
				go func() {
					select {
					case <-ctx.Done():
						return
					case <-time.After(RTT):
					}
					send(statecraft.E("FIN"))
				}()
			})
		}).

		// ── CLOSING ───────────────────────────────────────────────────────
		State("CLOSING", func(s *statecraft.StateBuilder[TCPCtx]) {
			s.On("ACK", "TIME_WAIT", statecraft.Do(recvACK))
		}).

		// ── TIME_WAIT ─────────────────────────────────────────────────────
		// RFC 793: stay here for 2*MSL to absorb any delayed duplicates.
		State("TIME_WAIT", func(s *statecraft.StateBuilder[TCPCtx]) {
			s.After(2*MSL, "CLOSED")
		}).

		// ── CLOSE_WAIT ────────────────────────────────────────────────────
		State("CLOSE_WAIT", func(s *statecraft.StateBuilder[TCPCtx]) {
			s.On("CLOSE", "LAST_ACK", statecraft.Do(sendFIN))
		}).

		// ── LAST_ACK ──────────────────────────────────────────────────────
		State("LAST_ACK", func(s *statecraft.StateBuilder[TCPCtx]) {
			s.On("ACK", "CLOSED", statecraft.Do(recvACK))
		}).
		MustBuild()
}

// ─── diagram ─────────────────────────────────────────────────────────────────

func generateDiagram(m *model.Machine[TCPCtx]) {
	fmt.Println("=== Mermaid ===")
	fmt.Println(viz.ToMermaid(m))

	if _, err := exec.LookPath("dot"); err != nil {
		fmt.Println("(graphviz not found , skipping SVG, install with: brew install graphviz)")
		return
	}
	outDir := "docs/diagrams"
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "mkdir %s: %v\n", outDir, err)
		return
	}
	outPath := outDir + "/tcp-rfc793.svg"
	cmd := exec.Command("dot", "-Tsvg", "-o", outPath)
	cmd.Stdin = strings.NewReader(viz.ToGraphviz(m))
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "dot: %v\n", err)
		return
	}
	fmt.Printf("wrote %s\n\n", outPath)
}

// ─── main ─────────────────────────────────────────────────────────────────────

func main() {
	m := buildMachine()
	generateDiagram(m)

	fmt.Println("=== RFC 793 TCP state machine ===")
	fmt.Printf("Q  = %v\n", m.StateIDs())
	fmt.Printf("q0 = CLOSED\n")
	fmt.Printf("F  = {CLOSED}\n\n")

	svc := statecraft.Start(m)
	defer svc.Stop()

	done := make(chan struct{})
	var prev core.StateID

	svc.Subscribe(func(snap statecraft.Snapshot[TCPCtx]) {
		if snap.State != prev {
			if prev != "" {
				fmt.Printf("%-14s → %s\n", prev, snap.State)
			}
			prev = snap.State
		}
		// Signal when we return to CLOSED after the full teardown.
		if snap.State == "CLOSED" && snap.Context.Sent > 0 {
			select {
			case <-done:
			default:
				close(done)
			}
		}
	})

	step := func(ev string, label string) {
		if label != "" {
			fmt.Printf("\n[%s]\n", label)
		}
		svc.Send(statecraft.E(ev))
	}

	// ── 1. Three-way handshake (active open) ─────────────────────────────
	fmt.Println("── three-way handshake ──────────────────────────────")
	step("ACTIVE_OPEN", "user: connect")
	// Peer SYN_ACK arrives via Invoke (RTT delay) → ESTABLISHED

	time.Sleep(RTT + 50*time.Millisecond) // wait for handshake

	// ── 2. Data transfer ─────────────────────────────────────────────────
	fmt.Println("\n── data transfer ────────────────────────────────────")
	step("SEND", "user: write(\"hello world\")")
	time.Sleep(20 * time.Millisecond)
	step("DATA", "peer: write(\"hello world\")")
	time.Sleep(20 * time.Millisecond)

	// ── 3. Four-way teardown (active close) ──────────────────────────────
	fmt.Println("\n── four-way teardown ────────────────────────────────")
	step("CLOSE", "user: close()")
	// Peer ACK arrives via FIN_WAIT_1 Invoke → FIN_WAIT_2
	// Peer FIN arrives via FIN_WAIT_2 Invoke → TIME_WAIT
	// TIME_WAIT After(2*MSL) → CLOSED

	<-done

	snap := svc.Snapshot()
	fmt.Printf("\n── connection released ──────────────────────────────\n")
	fmt.Printf("final state : %s\n", snap.State)
	fmt.Printf("seq/ack     : SND_NXT=%-6d RCV_NXT=%d\n",
		snap.Context.SND_NXT, snap.Context.RCV_NXT)
	fmt.Printf("segments    : sent=%d  recv=%d\n",
		snap.Context.Sent, snap.Context.Recv)
}
