package stateless

import (
	"context"
	"testing"
)

func benchmarkQueuedReentry(b *testing.B, configure func(*StateMachine)) {
	sm := NewStateMachine("active")
	calls := 0
	sm.Configure("active").
		PermitReentry("tick", func(context.Context, ...any) bool { calls++; return true }).
		OnEntry(func(context.Context, ...any) error { calls++; return nil }).
		OnExit(func(context.Context, ...any) error { calls++; return nil })
	sm.OnTransitioning(func(context.Context, Transition) { calls++ })
	sm.OnTransitioned(func(context.Context, Transition) { calls++ })
	if configure != nil {
		configure(sm)
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := sm.FireCtx(ctx, "tick"); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if calls != 5*b.N {
		b.Fatalf("callbacks = %d; want %d", calls, 5*b.N)
	}
}

func BenchmarkQueuedReentryDisabled(b *testing.B) {
	benchmarkQueuedReentry(b, nil)
}
