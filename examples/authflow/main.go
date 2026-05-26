// Package main demonstrates a more realistic auth flow with:
//   - Typed events with payloads
//   - Guards on transitions
//   - Context mutation via Assign
//   - State-scoped timeout (After)
//   - Subscription observer
//   - Final state
//
// Run with:
//
//	go run ./examples/authflow
package main

import (
	"fmt"
	"statecraft"
	"statecraft/core"
	"time"
)

// ─── Events ───────────────────────────────────────────────────────────────────

type LoginEvent struct {
	Username string
	Password string
}

func (e LoginEvent) Type() core.EventType { return "LOGIN" }

type ResponseEvent struct {
	OK      bool
	UserID  string
	ErrMsg  string
}

func (e ResponseEvent) Type() core.EventType { return "RESPONSE" }

type LogoutEvent struct{}

func (e LogoutEvent) Type() core.EventType { return "LOGOUT" }

// ─── Context ──────────────────────────────────────────────────────────────────

type AuthCtx struct {
	PendingUser string
	UserID      string
	ErrMsg      string
	Attempts    int
}

// ─── Machine ──────────────────────────────────────────────────────────────────

func buildAuthMachine() *statecraft.Machine[AuthCtx] {
	isValidResponse := statecraft.GuardFn[AuthCtx](func(c AuthCtx, ev core.Event) bool {
		r, ok := ev.(ResponseEvent)
		return ok && r.OK
	})

	// Guards evaluate the context BEFORE transition actions run.
	// Attempts is incremented in the retry action, so "already failed twice"
	// (Attempts == 2) means this is the third attempt — trigger lockout.
	isMaxAttempts := statecraft.GuardFn[AuthCtx](func(c AuthCtx, _ core.Event) bool {
		return c.Attempts >= 2
	})

	return statecraft.New[AuthCtx]("auth").
		Context(AuthCtx{}).
		Initial("idle").

		State("idle", func(s *statecraft.StateBuilder[AuthCtx]) {
			s.On("LOGIN", "authenticating",
				statecraft.Do(statecraft.Assign(func(c AuthCtx, ev core.Event) AuthCtx {
					if login, ok := ev.(LoginEvent); ok {
						c.PendingUser = login.Username
						c.ErrMsg = ""
					}
					return c
				})),
			)
		}).

		State("authenticating", func(s *statecraft.StateBuilder[AuthCtx]) {
			s.Entry(statecraft.Log(func(c AuthCtx, _ core.Event) string {
				return fmt.Sprintf("→ authenticating user %q (attempt %d)", c.PendingUser, c.Attempts+1)
			}))

			// Success path
			s.On("RESPONSE", "active",
				statecraft.When(isValidResponse),
				statecraft.Do(statecraft.Assign(func(c AuthCtx, ev core.Event) AuthCtx {
					if r, ok := ev.(ResponseEvent); ok {
						c.UserID = r.UserID
						c.PendingUser = ""
						c.Attempts = 0
					}
					return c
				})),
			)

			// Failure + lockout
			s.On("RESPONSE", "locked",
				statecraft.When(statecraft.And(
					statecraft.Not(isValidResponse),
					isMaxAttempts,
				)),
				statecraft.Do(statecraft.Assign(func(c AuthCtx, ev core.Event) AuthCtx {
					if r, ok := ev.(ResponseEvent); ok {
						c.ErrMsg = r.ErrMsg
					}
					return c
				})),
			)

			// Failure, retry
			s.On("RESPONSE", "idle",
				statecraft.When(statecraft.Not(isValidResponse)),
				statecraft.Do(statecraft.Assign(func(c AuthCtx, ev core.Event) AuthCtx {
					c.Attempts++
					if r, ok := ev.(ResponseEvent); ok {
						c.ErrMsg = r.ErrMsg
					}
					return c
				})),
			)

			// Authentication timeout
			s.After(2*time.Second, "idle",
				statecraft.Do(statecraft.Assign(func(c AuthCtx, _ core.Event) AuthCtx {
					c.ErrMsg = "authentication timed out"
					return c
				})),
			)
		}).

		State("active", func(s *statecraft.StateBuilder[AuthCtx]) {
			s.Entry(statecraft.Log(func(c AuthCtx, _ core.Event) string {
				return fmt.Sprintf("✓ logged in as %q (userID=%s)", c.PendingUser, c.UserID)
			}))
			s.On("LOGOUT", "idle",
				statecraft.Do(statecraft.Assign(func(c AuthCtx, _ core.Event) AuthCtx {
					c.UserID = ""
					return c
				})),
			)
		}).

		State("locked", func(s *statecraft.StateBuilder[AuthCtx]) {
			s.Entry(statecraft.Log(func(c AuthCtx, _ core.Event) string {
				return fmt.Sprintf("✗ account locked: %s", c.ErrMsg)
			}))
			s.Final()
		}).

		MustBuild()
}

// ─── Main ─────────────────────────────────────────────────────────────────────

func main() {
	m := buildAuthMachine()
	svc := statecraft.Start(m)
	defer svc.Stop()

	// Subscribe to all state changes.
	svc.Subscribe(func(snap statecraft.Snapshot[AuthCtx]) {
		fmt.Printf("  [snap] state=%-16q  user=%q  err=%q  attempts=%d\n",
			snap.State, snap.Context.UserID, snap.Context.ErrMsg, snap.Context.Attempts)
	})

	fmt.Println("=== Auth Flow Demo ===")
	fmt.Println()

	send := func(ev core.Event) {
		fmt.Printf("► SEND %T\n", ev)
		if err := svc.Send(ev); err != nil {
			fmt.Printf("  error: %v\n", err)
		}
		time.Sleep(50 * time.Millisecond) // let interpreter process
	}

	// Scenario 1: failed login, then success
	fmt.Println("--- Scenario 1: fail then succeed ---")
	send(LoginEvent{Username: "alice", Password: "wrong"})
	send(ResponseEvent{OK: false, ErrMsg: "bad password"})

	send(LoginEvent{Username: "alice", Password: "correct"})
	send(ResponseEvent{OK: true, UserID: "usr_001"})

	send(LogoutEvent{})

	// Scenario 2: lockout after 3 failures
	fmt.Println("\n--- Scenario 2: lockout ---")
	for i := range 3 {
		send(LoginEvent{Username: "bob", Password: "wrong"})
		send(ResponseEvent{OK: false, ErrMsg: fmt.Sprintf("attempt %d failed", i+1)})
	}

	snap := svc.Snapshot()
	fmt.Printf("\nFinal state: %q  final=%v\n", snap.State, snap.Final)
}
