// Package main demonstrates a classic traffic-light state machine.
//
// Run with:
//
//	go run ./examples/trafficlight
package main

import (
	"fmt"
	"statecraft"
	"time"
)

type LightCtx struct {
	Cycles int
}

func main() {
	m := statecraft.New[LightCtx]("traffic-light").
		Context(LightCtx{}).
		Initial("red").
		State("red", func(s *statecraft.StateBuilder[LightCtx]) {
			s.Entry(statecraft.Log(func(c LightCtx, _ statecraft.Event) string {
				return fmt.Sprintf("🔴 RED  (cycle %d)", c.Cycles)
			}))
			s.On("TIMER", "green",
				statecraft.Do(statecraft.Assign(func(c LightCtx, _ statecraft.Event) LightCtx {
					c.Cycles++
					return c
				})),
			)
		}).
		State("green", func(s *statecraft.StateBuilder[LightCtx]) {
			s.Entry(statecraft.Log(func(c LightCtx, _ statecraft.Event) string {
				return fmt.Sprintf("🟢 GREEN (cycle %d)", c.Cycles)
			}))
			s.On("TIMER", "yellow")
		}).
		State("yellow", func(s *statecraft.StateBuilder[LightCtx]) {
			s.Entry(statecraft.Log(func(c LightCtx, _ statecraft.Event) string {
				return fmt.Sprintf("🟡 YELLOW (cycle %d)", c.Cycles)
			}))
			s.On("TIMER", "red")
		}).
		MustBuild()

	svc := statecraft.Start(m)
	defer svc.Stop()

	fmt.Printf("Machine: %q  |  states: %v\n\n", m.ID(), m.StateIDs())

	// Tick through 2 full cycles.
	timer := statecraft.E("TIMER")
	for range 6 {
		time.Sleep(300 * time.Millisecond)
		if err := svc.Send(timer); err != nil {
			fmt.Printf("send error: %v\n", err)
			return
		}
	}

	snap := svc.Snapshot()
	fmt.Printf("\nFinal state: %q  |  cycles: %d\n", snap.State, snap.Context.Cycles)
}
