package main

import (
	"sync"
	"time"
)

type watchdogDecision struct {
	Recover    bool
	QueuedJobs int
	WaitingFor time.Duration
	Generation uint64
}

// jobWatchdog contains only scheduling state. The Scaler owns the recovery
// side effects, which keeps the timeout logic deterministic and testable.
type jobWatchdog struct {
	mu           sync.Mutex
	timeout      time.Duration
	assignedJobs int
	queuedJobs   int
	waitingSince time.Time
	recovering   bool
	needsSignal  bool
	generation   uint64
}

func newJobWatchdog(timeout time.Duration) *jobWatchdog {
	return &jobWatchdog{timeout: timeout}
}

func (w *jobWatchdog) update(now time.Time, assignedJobs, busyRunners int, progress bool) watchdogDecision {
	w.mu.Lock()
	defer w.mu.Unlock()

	queuedJobs := max(0, assignedJobs-busyRunners)
	w.assignedJobs = assignedJobs
	w.queuedJobs = queuedJobs
	if queuedJobs == 0 {
		w.generation++
		w.waitingSince = time.Time{}
		w.needsSignal = false
		return watchdogDecision{}
	}

	if w.needsSignal {
		w.needsSignal = false
		w.waitingSince = now
		w.generation++
	} else if progress || w.waitingSince.IsZero() {
		w.waitingSince = now
		w.generation++
	}

	return w.decision(now)
}

func (w *jobWatchdog) jobStarted(now time.Time, busyRunners int) watchdogDecision {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.queuedJobs = max(0, w.assignedJobs-busyRunners)
	if w.queuedJobs == 0 {
		w.generation++
		w.waitingSince = time.Time{}
		w.needsSignal = false
		return watchdogDecision{}
	}

	// Starting any job proves that the pool made progress. Give remaining
	// queued work a fresh timeout instead of recycling a healthy runner.
	w.waitingSince = now
	w.needsSignal = false
	w.generation++
	return w.decision(now)
}

func (w *jobWatchdog) hasQueuedJobs() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.queuedJobs > 0
}

func (w *jobWatchdog) check(now time.Time) watchdogDecision {
	w.mu.Lock()
	defer w.mu.Unlock()

	decision := w.decision(now)
	if decision.Recover {
		w.recovering = true
	}
	return decision
}

func (w *jobWatchdog) decision(now time.Time) watchdogDecision {
	if w.queuedJobs == 0 || w.waitingSince.IsZero() {
		return watchdogDecision{}
	}

	waitingFor := now.Sub(w.waitingSince)
	return watchdogDecision{
		Recover:    !w.recovering && waitingFor >= w.timeout,
		QueuedJobs: w.queuedJobs,
		WaitingFor: waitingFor,
		Generation: w.generation,
	}
}

func (w *jobWatchdog) recoveryStillDue(now time.Time, generation uint64) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.queuedJobs > 0 &&
		!w.waitingSince.IsZero() &&
		w.generation == generation &&
		now.Sub(w.waitingSince) >= w.timeout
}

func (w *jobWatchdog) finishRecovery() {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.recovering = false
	w.generation++
	if w.queuedJobs > 0 {
		// Do not loop forever from one stale signal. A subsequent listener update
		// must confirm that work is still queued before another full timeout starts.
		w.waitingSince = time.Time{}
		w.needsSignal = true
	}
}
