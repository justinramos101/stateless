<p align="center"><img width="650" src="./assets/stateless.svg" alt="Stateless logo. Fire gopher designed by https://www.deviantart.com/quasilyte"></p>

<p align="center">
    <a href="https://pkg.go.dev/github.com/justinramos101/stateless?tab=doc"><img src="https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white" alt="go.dev"></a>
    <a href="https://github.com/justinramos101/stateless/actions/workflows/test.yml"><img src="https://github.com/justinramos101/stateless/actions/workflows/test.yml/badge.svg" alt="Build Status"></a>
    <a href="https://goreportcard.com/report/github.com/justinramos101/stateless"><img src="https://goreportcard.com/badge/github.com/justinramos101/stateless" alt="Go Report Card"></a>
    <a href="https://opensource.org/licenses/BSD-2-Clause"><img src="https://img.shields.io/badge/License-BSD%202--Clause-orange.svg" alt="Licenses"></a>
    <a href="https://github.com/avelino/awesome-go"><img src="https://awesome.re/mentioned-badge.svg" alt="Mentioned in Awesome Go"></a>
</p>

# Stateless

This fork of [qmuntal/stateless](https://github.com/qmuntal/stateless) adds opt-in trigger execution timing. It retains the upstream BSD-2-Clause license and state-machine behavior.

## Install this fork

```sh
go get github.com/justinramos101/stateless@feat/timing-observer
```

Import `github.com/justinramos101/stateless`. Inherited upstream tags still declare the upstream module path. Use this branch or a pseudo-version from a fork commit until the fork has its own release tag.

## Observe trigger executions

Register one observer before firing or sharing the machine. `SetExecutionObserver(nil)` disables observation. Registration replaces the previous observer and must not race with use.

```go
sm := stateless.NewStateMachine("idle")
sm.Configure("idle").Permit("start", "running")
sm.SetExecutionObserver(func(ctx context.Context, e stateless.Execution) {
	log.Printf("execution=%d wait=%s run=%s outcome=%v",
		e.ID, e.QueueWait, e.FinishedAt.Sub(e.StartedAt), e.Outcome)
	if e.CompletedTransition != nil {
		log.Printf("%v -> %v", e.CompletedTransition.Source,
			e.CompletedTransition.Destination)
	}
})
err := sm.FireCtx(context.Background(), "start")
```

The [runnable external-package example](observation_example_test.go) uses this API.

### Measurement contract

One `Execution` describes one attempted trigger execution. It includes ignored, internal, unhandled, failed, and interrupted executions. Queued items receive no ID or record until they execute. A queued `FireCtx` call can return before its item runs, or drain another caller's item and receive that item's error.

`StartedAt` and `FinishedAt` bracket parameter validation, existing storage access, guards, selectors, actions, and transition hooks. Their monotonic difference includes instrumentation bookkeeping within that interval. It excludes the execution's own observer and mode bookkeeping. Immediate parent intervals include nested executions and their observers, so they are not exclusive durations. Activation, deactivation, queries, and individual action substages are outside this API.

`QueueWait` starts immediately before append under the queue mutex and ends at `StartedAt`. It excludes contention before acquiring the enqueue mutex. It includes earlier executions and observers, queue-fetch contention, and delays while work remains pending after a failure. Immediate executions report zero queue wait.

`ID` is a machine-local atomic sequence allocated when observed execution begins. It identifies an execution, not a submission or durable operation. IDs and callback arrival order need not match timestamp order under concurrency.

`Source` is the first successful existing state read. `SelectedDestination` is the handler's destination before hierarchy descent. `KnownState.Known` distinguishes unknown state from a known nil value. `CompletedTransition` copies the exact final `OnTransitioned` argument after all existing completion hooks return. It can differ from the selected destination during hierarchy entry or inherited reentry. Its absence does not imply that storage was unchanged. State values are shallow copies; consumers must not mutate referenced values concurrently.

`Kind` identifies the selected handler independently of `Outcome`. `KindSuppressed` means the existing fixed-transition path skipped a same-state destination. Dynamic same-state transitions still execute their existing actions. `KindUnresolved` means execution stopped before identifying a handler. A custom unhandled action can produce `KindUnhandled` with `ExecutionSucceeded`.

`ExecutionFailed` retains the exact returned error in `Err`. There is no wrapping, rollback, retry, or queue flush. `ExecutionInterrupted` means the trigger body did not return normally, including panic or `runtime.Goexit`. Original trigger panics propagate unchanged. Panic values are not included in records.

### Observer delivery

The observer runs synchronously after `FinishedAt`, outside queue and storage locks. Queued execution keeps its slot until the observer returns, and `Firing()` remains true during delivery. A queued fire from the observer enqueues work that executes afterward. Never wait synchronously for queued work submitted from an observer or a callback.

Immediate observers can overlap and can fire recursively. Protect a shared collector with a mutex and bound observer-triggered recursion. A panic escaping the observer, including one from its nested immediate fire, is suppressed without replacing an original trigger error or panic. Observers must return. Recovery cannot isolate `runtime.Goexit`, process exit, or permanent blocking in an observer. Process termination can prevent delivery entirely.

The callback receives the executed item's original context, including cancellation. The library does not cancel execution or skip observation because that context is canceled. Callbacks retain their existing responsibility for honoring cancellation.

### Interpreting a timeline

Records with `CompletedTransition != nil` describe completed logical transitions. Their execution intervals and reported destinations support a transition timeline. For sequential queued executions, the next start minus the previous finish is an inter-execution gap. Differences between finish times measure intervals between completion observations.

These measurements do not establish true state residence. Ordinary state writes precede entry, reentry writes follow entry, entry can fail after a write, and external storage can change independently. Immediate calls can overlap or leave storage different from an outer reported destination. Exact mutation histories require instrumentation at the storage owner. Observation adds no state reads or writes.

### Benchmarking

`BenchmarkQueuedReentryDisabled` and `BenchmarkQueuedReentryObserved` exercise queued reentry with guards, entry and exit actions, and transition hooks.

```sh
go test -run '^$' -bench '^BenchmarkQueuedReentry' -benchtime=200ms -count=3 -benchmem .
```

Disabled observation skips clocks, execution IDs, records, and observer delivery. Each queue element still carries one extra optional timestamp pointer, which can increase allocation bytes during queue growth. Enabled queued submissions allocate timestamp storage. Benchmark results depend on the workload and machine.


**Create *state machines* and lightweight *state machine-based workflows* directly in Go code:**

```go
phoneCall := stateless.NewStateMachine(stateOffHook)

phoneCall.Configure(stateOffHook).Permit(triggerCallDialed, stateRinging)

phoneCall.Configure(stateRinging).
  OnEntryFrom(triggerCallDialed, func(_ context.Context, args ...any) error {
    onDialed(args[0].(string))
    return nil
  }).
  Permit(triggerCallConnected, stateConnected)

phoneCall.Configure(stateConnected).
  OnEntry(func(_ context.Context, _ ...any) error {
    startCallTimer()
    return nil
  }).
  OnExit(func(_ context.Context, _ ...any) error {
    stopCallTimer()
    return nil
  }).
  Permit(triggerLeftMessage, stateOffHook).
  Permit(triggerPlacedOnHold, stateOnHold)

// ...

phoneCall.Fire(triggerCallDialed, "qmuntal")
```

This project, as well as the example above, is almost a direct, yet idiomatic, port of [dotnet-state-machine/stateless](https://github.com/dotnet-state-machine/stateless), which is written in C#.

The state machine implemented in this library is based on the theory of [UML statechart](https://en.wikipedia.org/wiki/UML_state_machine). The concepts behind it are about organizing the way a device, computer program, or other (often technical) process works such that an entity or each of its sub-entities is always in exactly one of a number of possible states and where there are well-defined conditional transitions between these states.

## Features

Most standard state machine constructs are supported:

* Support for states and triggers of any comparable type (int, strings, boolean, structs, etc.)
* Hierarchical states
* Entry/exit events for states
* Guard clauses to support conditional transitions
* Introspection

Some useful extensions are also provided:

* Ability to store state externally (for example, in a property tracked by an ORM)
* Parameterised triggers
* Reentrant states
* Thread-safe
* Export to DOT graph

### Hierarchical States

In the example below, the `OnHold` state is a substate of the `Connected` state. This means that an `OnHold` call is still connected.

```go
phoneCall.Configure(stateOnHold).
  SubstateOf(stateConnected).
  Permit(triggerTakenOffHold, stateConnected).
  Permit(triggerPhoneHurledAgainstWall, statePhoneDestroyed)
```

In addition to the `StateMachine.State` property, which will report the precise current state, an `IsInState(State)` method is provided. `IsInState(State)` will take substates into account, so that if the example above was in the `OnHold` state, `IsInState(State.Connected)` would also evaluate to `true`.

### Entry/Exit Events

In the example, the `StartCallTimer()` method will be executed when a call is connected. The `StopCallTimer()` will be executed when call completes (by either hanging up or hurling the phone against the wall.)

The call can move between the `Connected` and `OnHold` states without the `StartCallTimer()` and `StopCallTimer()` methods being called repeatedly because the `OnHold` state is a substate of the `Connected` state.

Entry/Exit event handlers can be supplied with a parameter of type `Transition` that describes the trigger, source and destination states.

### Initial state transitions

A substate can be marked as initial state. When the state machine enters the super state it will also automatically enter the substate. This can be configured like this:

```go
sm.Configure(State.B)
  .InitialTransition(State.C);

sm.Configure(State.C)
  .SubstateOf(State.B);
```

### External State Storage

Stateless is designed to be embedded in various application models. For example, some ORMs place requirements upon where mapped data may be stored, and UI frameworks often require state to be stored in special "bindable" properties. To this end, the `StateMachine` constructor can accept function arguments that will be used to read and write the state values:

```go
machine := stateless.NewStateMachineWithExternalStorage(func(_ context.Context) (stateless.State, error) {
  return myState.Value, nil
}, func(_ context.Context, state stateless.State) error {
  myState.Value  = state
  return nil
}, stateless.FiringQueued)
```

In this example the state machine will use the `myState` object for state storage.

This can further be extended to support more complex scenarios, such as when not only the current state is required but also the arguments which were supplied to that state. This can be useful when using error states that additional metadata can be stored or acted upon via callbacks.

```go
machine := stateless.NewStateMachineWithExternalStorageAndArgs(func(_ context.Context) (stateless.State, []any, error) {
  return myState.Value, myState.Args, nil
}, func(_ context.Context, state stateless.State, args ...any) error {
  myState.Value = state
  myState.Args = args
  return nil
}, stateless.FiringQueued)
```


### Activation / Deactivation

It might be necessary to perform some code before storing the object state, and likewise when restoring the object state. Use `Deactivate` and `Activate` for this. Activation should only be called once before normal operation starts, and once before state storage.

### Introspection

The state machine can provide a list of the triggers that can be successfully fired within the current state via the `StateMachine.PermittedTriggers` property.

### Guard Clauses

The state machine will choose between multiple transitions based on guard clauses, e.g.:

```go
phoneCall.Configure(stateOffHook).
  Permit(triggerCallDialled, stateRinging, func(_ context.Context, _ ...any) bool {
    return IsValidNumber()
  }).
  Permit(triggerCallDialled, stateBeeping, func(_ context.Context, _ ...any) bool {
    return !IsValidNumber()
  })
```

Guard clauses within a state must be mutually exclusive (multiple guard clauses cannot be valid at the same time). Substates can override transitions by respecifying them, however substates cannot disallow transitions that are allowed by the superstate.

The guard clauses will be evaluated whenever a trigger is fired. Guards should therefor be made side effect free.

### Parameterised Triggers

Strongly-typed parameters can be assigned to triggers:

```go
stateMachine.SetTriggerParameters(triggerCallDialed, reflect.TypeOf(""))

stateMachine.Configure(stateRinging).
  OnEntryFrom(triggerCallDialed, func(_ context.Context, args ...any) error {
    fmt.Println(args[0].(string))
    return nil
  })

stateMachine.Fire(triggerCallDialed, "qmuntal")
```

It is runtime safe to cast parameters to the ones specified in `SetTriggerParameters`. If the parameters passed in `Fire` do not match the ones specified it will panic.

Trigger parameters can be used to dynamically select the destination state using the `PermitDynamic()` configuration method.

### Ignored Transitions and Reentrant States

Firing a trigger that does not have an allowed transition associated with it will cause a panic to be thrown.

To ignore triggers within certain states, use the `Ignore(Trigger)` directive:

```go
phoneCall.Configure(stateConnected).
  Ignore(triggerCallDialled)
```

Alternatively, a state can be marked reentrant so its entry and exit events will fire even when transitioning from/to itself:

```go
stateMachine.Configure(stateAssigned).
  PermitReentry(triggerAssigned).
  OnEntry(func(_ context.Context, _ ...any) error {
    startCallTimer()
    return nil
  })
```

By default, triggers must be ignored explicitly. To override Stateless's default behaviour of throwing a panic when an unhandled trigger is fired, configure the state machine using the `OnUnhandledTrigger` method:

```go
stateMachine.OnUnhandledTrigger( func (_ context.Context, state State, _ Trigger, _ []string) {})
```

### Export to DOT graph

It can be useful to visualize state machines on runtime. With this approach the code is the authoritative source and state diagrams are by-products which are always up to date.

```go
sm := stateMachine.Configure(stateOffHook).
  Permit(triggerCallDialed, stateRinging, isValidNumber)
graph := sm.ToGraph()
```

The StateMachine.ToGraph() method returns a string representation of the state machine in the DOT graph language, e.g.:

```dot
digraph {
  OffHook -> Ringing [label="CallDialled [isValidNumber]"];
}
```

This can then be rendered by tools that support the DOT graph language, such as the dot command line tool from graphviz.org or viz.js. See [webgraphviz.com](http://www.webgraphviz.com) for instant gratification. Command line example: dot -T pdf -o phoneCall.pdf phoneCall.dot to generate a PDF file.

This is the complete Phone Call graph as builded in `example_test.go`.

![Phone Call graph](assets/phone-graph.svg?raw=true "Phone Call complete DOT")

## Project Goals

This page is an almost-complete description of Stateless, and its explicit aim is to remain minimal.

Please use the issue tracker or the if you'd like to report problems or discuss features.

(_Why the name? Stateless implements the set of rules regarding state transitions, but, at least when the delegate version of the constructor is used, doesn't maintain any internal state itself._)
