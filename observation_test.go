package stateless_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/justinramos101/stateless"
)

type executionCollector struct {
	mu    sync.Mutex
	items []stateless.Execution
}

func (c *executionCollector) observe(_ context.Context, e stateless.Execution) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items = append(c.items, e)
}

func (c *executionCollector) records() []stateless.Execution {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]stateless.Execution(nil), c.items...)
}

func checkExecution(t *testing.T, e stateless.Execution) {
	t.Helper()
	if e.ID == 0 || e.StartedAt.IsZero() || e.FinishedAt.Before(e.StartedAt) || e.QueueWait < 0 {
		t.Fatalf("invalid execution timing: %+v", e)
	}
}

func TestExecutionKinds(t *testing.T) {
	for _, tc := range []struct {
		name      string
		initial   any
		configure func(*stateless.StateMachine)
		kind      stateless.ExecutionKind
		selected  stateless.KnownState
		completed *stateless.Transition
		final     any
	}{
		{"transition", "a", func(sm *stateless.StateMachine) { sm.Configure("a").Permit("go", "b") }, stateless.KindTransition, stateless.KnownState{Value: "b", Known: true}, &stateless.Transition{Source: "a", Destination: "b", Trigger: "go"}, "b"},
		{"dynamic same state", "a", func(sm *stateless.StateMachine) {
			sm.Configure("a").PermitDynamic("go", func(context.Context, ...any) (any, error) { return "a", nil })
		}, stateless.KindDynamic, stateless.KnownState{Value: "a", Known: true}, &stateless.Transition{Source: "a", Destination: "a", Trigger: "go"}, "a"},
		{"suppressed", "a", func(sm *stateless.StateMachine) {
			sm.Configure("a").SubstateOf("parent")
			sm.Configure("parent").Permit("go", "a")
		}, stateless.KindSuppressed, stateless.KnownState{Value: "a", Known: true}, nil, "a"},
		{"reentry", "a", func(sm *stateless.StateMachine) { sm.Configure("a").PermitReentry("go") }, stateless.KindReentry, stateless.KnownState{Value: "a", Known: true}, &stateless.Transition{Source: "a", Destination: "a", Trigger: "go"}, "a"},
		{"inherited reentry", "a", func(sm *stateless.StateMachine) {
			sm.Configure("a").SubstateOf("parent")
			sm.Configure("parent").PermitReentry("go")
		}, stateless.KindReentry, stateless.KnownState{Value: "parent", Known: true}, &stateless.Transition{Source: "parent", Destination: "parent", Trigger: "go"}, "parent"},
		{"hierarchy", "a", func(sm *stateless.StateMachine) {
			sm.Configure("a").Permit("go", "b")
			sm.Configure("b").InitialTransition("c")
			sm.Configure("c").SubstateOf("b")
		}, stateless.KindTransition, stateless.KnownState{Value: "b", Known: true}, &stateless.Transition{Source: "a", Destination: "c", Trigger: "go"}, "c"},
		{"ignored", "a", func(sm *stateless.StateMachine) { sm.Configure("a").Ignore("go") }, stateless.KindIgnored, stateless.KnownState{}, nil, "a"},
		{"internal", "a", func(sm *stateless.StateMachine) {
			sm.Configure("a").InternalTransition("go", func(context.Context, ...any) error { return nil })
		}, stateless.KindInternal, stateless.KnownState{}, nil, "a"},
		{"custom unhandled", "a", func(sm *stateless.StateMachine) {
			sm.OnUnhandledTrigger(func(context.Context, any, any, []string) error { return nil })
		}, stateless.KindUnhandled, stateless.KnownState{}, nil, "a"},
		{"known nil", nil, func(sm *stateless.StateMachine) { sm.Configure(nil).Permit("go", "b") }, stateless.KindTransition, stateless.KnownState{Value: "b", Known: true}, &stateless.Transition{Source: nil, Destination: "b", Trigger: "go"}, "b"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sm := stateless.NewStateMachine(tc.initial)
			tc.configure(sm)
			var c executionCollector
			var hook []stateless.Transition
			sm.OnTransitioned(func(_ context.Context, tr stateless.Transition) { hook = append(hook, tr) })
			sm.SetExecutionObserver(c.observe)
			if err := sm.FireCtx(context.Background(), "go"); err != nil {
				t.Fatal(err)
			}
			items := c.records()
			if len(items) != 1 {
				t.Fatalf("records = %d", len(items))
			}
			e := items[0]
			checkExecution(t, e)
			if e.Kind != tc.kind || e.Mode != stateless.FiringQueued || e.Trigger != "go" || e.Outcome != stateless.ExecutionSucceeded || e.Err != nil || e.Source != (stateless.KnownState{Value: tc.initial, Known: true}) || e.SelectedDestination != tc.selected || !reflect.DeepEqual(e.CompletedTransition, tc.completed) || sm.MustState() != tc.final {
				t.Fatalf("execution = %+v; state = %v", e, sm.MustState())
			}
			if tc.completed != nil && (len(hook) != 1 || !reflect.DeepEqual(*e.CompletedTransition, hook[0])) {
				t.Fatalf("completion differs from hook: %v", hook)
			}
		})
	}
}

func TestExecutionErrors(t *testing.T) {
	for _, stage := range []string{"accessor", "internal accessor", "selector", "mutator", "exit", "entry", "internal action", "unhandled"} {
		t.Run(stage, func(t *testing.T) {
			sentinel := errors.New(stage)
			state := "a"
			reads := 0
			sm := stateless.NewStateMachineWithExternalStorage(func(context.Context) (any, error) {
				reads++
				if stage == "accessor" || stage == "internal accessor" && reads == 2 {
					return nil, sentinel
				}
				return state, nil
			}, func(_ context.Context, s any) error {
				state = s.(string)
				if stage == "mutator" {
					return sentinel
				}
				return nil
			}, stateless.FiringQueued)
			a := sm.Configure("a")
			switch stage {
			case "internal accessor", "internal action":
				a.InternalTransition("go", func(context.Context, ...any) error { return sentinel })
			case "selector":
				a.PermitDynamic("go", func(context.Context, ...any) (any, error) { return nil, sentinel })
			case "unhandled":
				sm.OnUnhandledTrigger(func(context.Context, any, any, []string) error { return sentinel })
			default:
				a.Permit("go", "b")
			}
			if stage == "exit" {
				a.OnExit(func(context.Context, ...any) error { return sentinel })
			}
			if stage == "entry" {
				sm.Configure("b").OnEntry(func(context.Context, ...any) error { return sentinel })
			}
			var c executionCollector
			sm.SetExecutionObserver(c.observe)
			if err := sm.FireCtx(context.Background(), "go"); err != sentinel {
				t.Fatalf("returned %v", err)
			}
			items := c.records()
			if len(items) != 1 {
				t.Fatalf("records = %d", len(items))
			}
			e := items[0]
			checkExecution(t, e)
			if e.Err != sentinel || e.Outcome != stateless.ExecutionFailed || e.CompletedTransition != nil {
				t.Fatalf("execution = %+v", e)
			}
			if e.Source.Known != (stage != "accessor") {
				t.Fatalf("source = %+v", e.Source)
			}
			if stage == "accessor" && e.Kind != stateless.KindUnresolved || stage == "selector" && (e.Kind != stateless.KindDynamic || e.SelectedDestination.Known) {
				t.Fatalf("execution = %+v", e)
			}
			if stage == "mutator" || stage == "entry" {
				if state != "b" {
					t.Fatalf("state = %s", state)
				}
			} else if state != "a" {
				t.Fatalf("state = %s", state)
			}
		})
	}
	t.Run("default unhandled", func(t *testing.T) {
		sm := stateless.NewStateMachine("a")
		var c executionCollector
		sm.SetExecutionObserver(c.observe)
		err := sm.FireCtx(context.Background(), "missing")
		items := c.records()
		if err == nil || len(items) != 1 || items[0].Err != err || items[0].Kind != stateless.KindUnhandled {
			t.Fatalf("error %v; records %+v", err, items)
		}
	})
}

func TestExecutionUnwinding(t *testing.T) {
	for _, mode := range []stateless.FiringMode{stateless.FiringQueued, stateless.FiringImmediate} {
		for _, stage := range []string{"action", "completion", "validation", "goexit"} {
			t.Run(fmt.Sprint(mode, "/", stage), func(t *testing.T) {
				sm := stateless.NewStateMachineWithMode("a", mode)
				marker := &struct{ name string }{"original panic"}
				sm.Configure("a").Permit("go", "b")
				if stage == "action" || stage == "goexit" {
					sm.Configure("b").OnEntry(func(context.Context, ...any) error {
						if stage == "goexit" {
							runtime.Goexit()
						}
						panic(marker)
					})
				}
				if stage == "completion" {
					sm.OnTransitioned(func(context.Context, stateless.Transition) { panic(marker) })
				}
				if stage == "validation" {
					sm.SetTriggerParameters("go", reflect.TypeOf(0))
				}
				var c executionCollector
				sm.SetExecutionObserver(func(ctx context.Context, e stateless.Execution) { c.observe(ctx, e); panic("observer panic") })
				done := make(chan struct{})
				var recovered any
				returned := false
				go func() {
					defer close(done)
					defer func() { recovered = recover() }()
					_ = sm.FireCtx(context.Background(), "go")
					returned = true
				}()
				<-done
				if returned || sm.Firing() {
					t.Fatalf("returned=%v firing=%v", returned, sm.Firing())
				}
				if stage == "goexit" {
					if recovered != nil {
						t.Fatalf("panic = %v", recovered)
					}
				} else if stage == "validation" {
					if recovered == nil {
						t.Fatal("validation did not panic")
					}
				} else if recovered != marker {
					t.Fatalf("panic = %v", recovered)
				}
				items := c.records()
				if len(items) != 1 {
					t.Fatalf("records = %d", len(items))
				}
				e := items[0]
				checkExecution(t, e)
				if e.Outcome != stateless.ExecutionInterrupted || e.Err != nil || e.CompletedTransition != nil {
					t.Fatalf("execution = %+v", e)
				}
				if stage == "validation" && (e.Source.Known || e.Kind != stateless.KindUnresolved) {
					t.Fatalf("validation = %+v", e)
				}
			})
		}
	}
}

func TestExecutionObserverPanicAndReplacement(t *testing.T) {
	sm := stateless.NewStateMachine("a")
	sm.Configure("a").Ignore("go")
	sm.SetExecutionObserver(func(context.Context, stateless.Execution) { t.Error("replaced observer called") })
	calls := 0
	sm.SetExecutionObserver(func(context.Context, stateless.Execution) { calls++; panic("observer") })
	if err := sm.FireCtx(context.Background(), "go"); err != nil {
		t.Fatal(err)
	}
	sentinel := errors.New("unhandled")
	sm.OnUnhandledTrigger(func(context.Context, any, any, []string) error { return sentinel })
	if err := sm.FireCtx(context.Background(), "other"); err != sentinel {
		t.Fatalf("error = %v", err)
	}
	sm.SetExecutionObserver(nil)
	if err := sm.FireCtx(context.Background(), "go"); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("calls = %d", calls)
	}
}

func TestExecutionQueuedObserverOwnsSlot(t *testing.T) {
	sm := stateless.NewStateMachine("a")
	sm.Configure("a").Ignore("first").Ignore("nested").Ignore("other")
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	var c executionCollector
	otherCtx := context.WithValue(context.Background(), struct{}{}, "other caller")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	sm.SetExecutionObserver(func(got context.Context, e stateless.Execution) {
		c.observe(got, e)
		if e.Trigger == "other" && got != otherCtx {
			t.Error("queued item lost its caller context")
		}
		if e.Trigger == "nested" && got != ctx {
			t.Error("nested item lost its caller context")
		}
		if e.Trigger == "first" {
			if got != ctx || got.Err() != context.Canceled || !sm.Firing() {
				t.Error("observer lost context or execution slot")
			}
			if err := sm.FireCtx(got, "nested"); err != nil {
				t.Error(err)
			}
			close(entered)
			<-release
		}
	})
	go func() { done <- sm.FireCtx(ctx, "first") }()
	<-entered
	beforeSubmit := time.Now()
	if err := sm.FireCtx(otherCtx, "other"); err != nil {
		t.Fatal(err)
	}
	if items := c.records(); len(items) != 1 {
		t.Fatalf("executed while observer held slot: %+v", items)
	}
	releasedAt := time.Now()
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	items := c.records()
	if len(items) != 3 {
		t.Fatalf("records = %d", len(items))
	}
	for i, e := range items {
		checkExecution(t, e)
		if e.ID != uint64(i+1) {
			t.Fatalf("id = %d", e.ID)
		}
	}
	if items[0].Trigger != "first" || items[1].Trigger != "nested" || items[2].Trigger != "other" {
		t.Fatalf("order = %+v", items)
	}
	if items[0].FinishedAt.After(beforeSubmit) || items[1].StartedAt.Before(releasedAt) || items[2].StartedAt.Before(items[1].FinishedAt) || items[2].QueueWait < items[2].StartedAt.Sub(releasedAt) {
		t.Fatalf("timing = %+v", items)
	}
}

func TestExecutionPendingAfterFailure(t *testing.T) {
	sm := stateless.NewStateMachine("a")
	sentinel := errors.New("action")
	sm.Configure("a").InternalTransition("fail", func(ctx context.Context, _ ...any) error {
		if err := sm.FireCtx(ctx, "pending"); err != nil {
			return err
		}
		return sentinel
	}).Ignore("pending").Ignore("resume")
	var c executionCollector
	sm.SetExecutionObserver(c.observe)
	if err := sm.FireCtx(context.Background(), "fail"); err != sentinel {
		t.Fatalf("error = %v", err)
	}
	items := c.records()
	if len(items) != 1 || items[0].Trigger != "fail" || items[0].Err != sentinel {
		t.Fatalf("premature observation: %+v", items)
	}
	resumeAt := time.Now()
	if err := sm.FireCtx(context.Background(), "resume"); err != nil {
		t.Fatal(err)
	}
	items = c.records()
	if len(items) != 3 || items[1].Trigger != "pending" || items[2].Trigger != "resume" || items[1].StartedAt.Before(resumeAt) || items[1].QueueWait < items[1].StartedAt.Sub(resumeAt) {
		t.Fatalf("records = %+v", items)
	}
}

func TestExecutionImmediateNestedAndConcurrent(t *testing.T) {
	t.Run("nested", func(t *testing.T) {
		sm := stateless.NewStateMachineWithMode("a", stateless.FiringImmediate)
		sm.Configure("a").InternalTransition("outer", func(ctx context.Context, _ ...any) error { return sm.FireCtx(ctx, "child") }).Ignore("child")
		var c executionCollector
		var childObserved time.Time
		sm.SetExecutionObserver(func(ctx context.Context, e stateless.Execution) {
			c.observe(ctx, e)
			if e.Trigger == "child" {
				childObserved = time.Now()
			}
		})
		if err := sm.FireCtx(context.Background(), "outer"); err != nil {
			t.Fatal(err)
		}
		items := c.records()
		if len(items) != 2 {
			t.Fatalf("records = %+v", items)
		}
		child, parent := items[0], items[1]
		if child.Trigger != "child" || parent.Trigger != "outer" || child.ID != 2 || parent.ID != 1 || parent.StartedAt.After(child.StartedAt) || parent.FinishedAt.Before(childObserved) || child.FinishedAt.After(childObserved) || parent.QueueWait != 0 || child.QueueWait != 0 {
			t.Fatalf("nested records = %+v", items)
		}
	})
	t.Run("concurrent", func(t *testing.T) {
		sm := stateless.NewStateMachineWithMode("a", stateless.FiringImmediate)
		entered := make(chan struct{}, 2)
		release := make(chan struct{})
		sm.Configure("a").Ignore("go", func(context.Context, ...any) bool { entered <- struct{}{}; <-release; return true })
		var c executionCollector
		sm.SetExecutionObserver(c.observe)
		var wg sync.WaitGroup
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := sm.FireCtx(context.Background(), "go"); err != nil {
					t.Error(err)
				}
			}()
		}
		<-entered
		<-entered
		overlapAt := time.Now()
		close(release)
		wg.Wait()
		items := c.records()
		if len(items) != 2 || items[0].ID == items[1].ID {
			t.Fatalf("records = %+v", items)
		}
		for _, e := range items {
			checkExecution(t, e)
			if e.Mode != stateless.FiringImmediate || e.QueueWait != 0 || e.StartedAt.After(overlapAt) || e.FinishedAt.Before(overlapAt) {
				t.Fatalf("overlap = %+v", e)
			}
		}
	})
	t.Run("observer nesting panic", func(t *testing.T) {
		sm := stateless.NewStateMachineWithMode("a", stateless.FiringImmediate)
		sm.Configure("a").Ignore("outer").InternalTransition("child", func(context.Context, ...any) error { panic("child") })
		var c executionCollector
		sm.SetExecutionObserver(func(ctx context.Context, e stateless.Execution) {
			c.observe(ctx, e)
			if e.Trigger == "outer" {
				_ = sm.FireCtx(ctx, "child")
			}
		})
		if err := sm.FireCtx(context.Background(), "outer"); err != nil {
			t.Fatal(err)
		}
		items := c.records()
		if len(items) != 2 || items[0].Outcome != stateless.ExecutionSucceeded || items[1].Outcome != stateless.ExecutionInterrupted || sm.Firing() {
			t.Fatalf("records = %+v", items)
		}
	})
}

func TestExecutionPreservesStorageAndCallbackOrder(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			var trace []string
			var moments []time.Time
			state := "a"
			note := func(s string) { trace = append(trace, s); moments = append(moments, time.Now()) }
			sm := stateless.NewStateMachineWithExternalStorage(func(context.Context) (any, error) { note("read"); return state, nil }, func(_ context.Context, s any) error { note("write " + s.(string)); state = s.(string); return nil }, stateless.FiringQueued)
			sm.Configure("a").Permit("go", "b", func(context.Context, ...any) bool { note("guard"); return true }).OnExit(func(context.Context, ...any) error { note("exit a"); return nil })
			sm.Configure("b").InitialTransition("c").OnEntry(func(context.Context, ...any) error { note("entry b"); return nil })
			sm.Configure("c").SubstateOf("b").OnEntry(func(context.Context, ...any) error { note("entry c"); return nil }).InternalTransition("internal", func(context.Context, ...any) error { note("action"); return nil }, func(context.Context, ...any) bool { note("internal guard"); return true })
			sm.OnTransitioning(func(_ context.Context, tr stateless.Transition) {
				note(fmt.Sprint("before ", tr.Source, tr.Destination))
			})
			sm.OnTransitioned(func(_ context.Context, tr stateless.Transition) {
				note(fmt.Sprint("after ", tr.Source, tr.Destination))
			})
			var c executionCollector
			if enabled {
				sm.SetExecutionObserver(c.observe)
			}
			if err := sm.FireCtx(context.Background(), "go"); err != nil {
				t.Fatal(err)
			}
			if err := sm.FireCtx(context.Background(), "internal"); err != nil {
				t.Fatal(err)
			}
			want := []string{"read", "guard", "exit a", "before ab", "write b", "entry b", "before bc", "entry c", "write c", "after ac", "read", "internal guard", "read", "internal guard", "action"}
			if !reflect.DeepEqual(trace, want) || state != "c" {
				t.Fatalf("trace = %v; state = %s", trace, state)
			}
			if enabled {
				items := c.records()
				if len(items) != 2 {
					t.Fatalf("records = %+v", items)
				}
				for i, m := range moments {
					j := 0
					if i >= 10 {
						j = 1
					}
					if m.Before(items[j].StartedAt) || m.After(items[j].FinishedAt) {
						t.Fatalf("callback %s outside interval", trace[i])
					}
				}
			}
		})
	}
}

func TestExecutionReentryAndDynamicActionOrder(t *testing.T) {
	for _, dynamic := range []bool{false, true} {
		for _, enabled := range []bool{false, true} {
			t.Run(fmt.Sprint(dynamic, "/", enabled), func(t *testing.T) {
				var trace []string
				sm := stateless.NewStateMachineWithExternalStorage(func(context.Context) (any, error) {
					trace = append(trace, "read")
					return "a", nil
				}, func(context.Context, any) error {
					trace = append(trace, "write")
					return nil
				}, stateless.FiringQueued)
				a := sm.Configure("a").OnExit(func(context.Context, ...any) error { trace = append(trace, "exit"); return nil }).OnEntry(func(context.Context, ...any) error { trace = append(trace, "entry"); return nil })
				if dynamic {
					a.PermitDynamic("go", func(context.Context, ...any) (any, error) { trace = append(trace, "select"); return "a", nil })
				} else {
					a.PermitReentry("go")
				}
				sm.OnTransitioning(func(context.Context, stateless.Transition) { trace = append(trace, "before") })
				sm.OnTransitioned(func(context.Context, stateless.Transition) { trace = append(trace, "after") })
				var c executionCollector
				if enabled {
					sm.SetExecutionObserver(c.observe)
				}
				if err := sm.FireCtx(context.Background(), "go"); err != nil {
					t.Fatal(err)
				}
				want := []string{"read", "exit", "before", "entry", "write", "after"}
				if dynamic {
					want = []string{"read", "select", "exit", "before", "write", "entry", "after"}
				}
				if !reflect.DeepEqual(trace, want) {
					t.Fatalf("trace = %v", trace)
				}
				if enabled {
					items := c.records()
					if len(items) != 1 || items[0].CompletedTransition == nil {
						t.Fatalf("records = %+v", items)
					}
				}
			})
		}
	}
}

func TestExecutionKnownNilDestination(t *testing.T) {
	sm := stateless.NewStateMachine("a")
	sm.Configure("a").PermitDynamic("go", func(context.Context, ...any) (any, error) { return nil, nil })
	var c executionCollector
	sm.SetExecutionObserver(c.observe)
	if err := sm.FireCtx(context.Background(), "go"); err != nil {
		t.Fatal(err)
	}
	items := c.records()
	if len(items) != 1 || items[0].SelectedDestination != (stateless.KnownState{Known: true}) || items[0].CompletedTransition == nil || items[0].CompletedTransition.Destination != nil || sm.MustState() != nil {
		t.Fatalf("records = %+v", items)
	}
}
