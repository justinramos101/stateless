package stateless

import (
	"context"
	"time"
)

// ExecutionKind identifies the handler selected for a trigger execution.
type ExecutionKind uint8

const (
	KindUnresolved ExecutionKind = iota
	KindUnhandled
	KindIgnored
	KindInternal
	KindTransition
	KindDynamic
	KindReentry
	KindSuppressed
)

// ExecutionOutcome distinguishes returned errors from interrupted executions.
type ExecutionOutcome uint8

const (
	ExecutionSucceeded ExecutionOutcome = iota
	ExecutionFailed
	ExecutionInterrupted
)

// KnownState distinguishes an unknown state from a known nil state.
// Value is a shallow copy of the state supplied by the machine or its handler.
type KnownState struct {
	Value State
	Known bool
}

// Execution describes one attempted trigger execution, not its FireCtx call.
// StartedAt and FinishedAt bracket validation, storage, guards, actions, and hooks.
// The executing observer's own time is excluded. Immediate nested executions and
// their observers are included in their parent's interval.
// Source is the first successful state read. SelectedDestination precedes initial
// substate descent. CompletedTransition copies the final OnTransitioned argument
// after all those hooks return. None of these fields is a final storage readback.
// QueueWait starts at queue admission under the queue mutex and is zero in
// immediate mode. ID is a machine-local execution sequence, not a submission ID.
type Execution struct {
	ID                  uint64
	Mode                FiringMode
	Trigger             Trigger
	Kind                ExecutionKind
	StartedAt           time.Time
	FinishedAt          time.Time
	QueueWait           time.Duration
	Outcome             ExecutionOutcome
	Err                 error
	Source              KnownState
	SelectedDestination KnownState
	CompletedTransition *Transition
}

// SetExecutionObserver replaces the synchronous completion observer. Nil disables
// observation. Configure it before firing or sharing the machine, as registration
// must not race with use. Immediate observers may overlap and must synchronize
// shared data. Queued observers run before the execution slot is released.
// The observer receives the execution's original context, even when canceled.
// Observer panics are suppressed. Original trigger panics propagate unchanged and
// produce ExecutionInterrupted, as does runtime.Goexit in the trigger body.
// Observers must return. Recovery cannot isolate their Goexit, process exit, or
// permanent blocking. Bound observer-triggered recursion and never wait for queued
// work submitted from an observer or callback.
func (sm *StateMachine) SetExecutionObserver(observer func(context.Context, Execution)) {
	sm.executionObserver = observer
}

func (sm *StateMachine) fireOne(ctx context.Context, trigger Trigger, mode FiringMode, admittedAt *time.Time, args ...any) error {
	observer := sm.executionObserver
	if observer == nil {
		return sm.internalFireOne(ctx, nil, trigger, args...)
	}
	e := Execution{ID: sm.executionID.Add(1), Mode: mode, Trigger: trigger, Outcome: ExecutionInterrupted}
	defer func() {
		e.FinishedAt = time.Now()
		deliverExecution(observer, ctx, e)
	}()
	e.StartedAt = time.Now()
	if admittedAt != nil {
		e.QueueWait = e.StartedAt.Sub(*admittedAt)
	}
	e.Err = sm.internalFireOne(ctx, &e, trigger, args...)
	if e.Err != nil {
		e.Outcome = ExecutionFailed
	} else {
		e.Outcome = ExecutionSucceeded
	}
	return e.Err
}

func deliverExecution(observer func(context.Context, Execution), ctx context.Context, e Execution) {
	defer func() { recover() }()
	observer(ctx, e)
}
