package stateless_test

import (
	"context"
	"fmt"

	"github.com/justinramos101/stateless"
)

func ExampleStateMachine_SetExecutionObserver() {
	sm := stateless.NewStateMachine("idle")
	sm.Configure("idle").Permit("start", "running")
	sm.SetExecutionObserver(func(_ context.Context, e stateless.Execution) {
		if e.CompletedTransition != nil {
			fmt.Printf("execution %d: %v -> %v\n", e.ID,
				e.CompletedTransition.Source, e.CompletedTransition.Destination)
		}
	})
	if err := sm.FireCtx(context.Background(), "start"); err != nil {
		panic(err)
	}
	// Output: execution 1: idle -> running
}
