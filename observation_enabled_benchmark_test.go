package stateless

import (
	"context"
	"testing"
)

func BenchmarkQueuedReentryObserved(b *testing.B) {
	benchmarkQueuedReentry(b, func(sm *StateMachine) {
		sm.SetExecutionObserver(func(context.Context, Execution) {})
	})
}
