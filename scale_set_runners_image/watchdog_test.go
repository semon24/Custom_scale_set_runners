package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestJobWatchdogDoesNotArmWithoutQueuedJobs(t *testing.T) {
	now := time.Date(2026, time.July, 24, 12, 0, 0, 0, time.UTC)
	watchdog := newJobWatchdog(5 * time.Minute)

	watchdog.update(now, 0, 0, false)
	decision := watchdog.check(now.Add(30 * time.Minute))

	require.False(t, decision.Recover)
	require.Equal(t, 0, decision.QueuedJobs)
	require.Zero(t, decision.WaitingFor)
}

func TestJobWatchdogRecoversAfterQueuedJobTimeout(t *testing.T) {
	now := time.Date(2026, time.July, 24, 12, 0, 0, 0, time.UTC)
	watchdog := newJobWatchdog(5 * time.Minute)

	watchdog.update(now, 1, 0, false)
	require.False(t, watchdog.check(now.Add(4*time.Minute+59*time.Second)).Recover)

	decision := watchdog.check(now.Add(5 * time.Minute))
	require.True(t, decision.Recover)
	require.Equal(t, 1, decision.QueuedJobs)
	require.Equal(t, 5*time.Minute, decision.WaitingFor)
}

func TestJobWatchdogJobStartClearsOrResetsWaitingPeriod(t *testing.T) {
	now := time.Date(2026, time.July, 24, 12, 0, 0, 0, time.UTC)
	watchdog := newJobWatchdog(5 * time.Minute)
	watchdog.update(now, 2, 0, false)

	progressAt := now.Add(4 * time.Minute)
	watchdog.jobStarted(progressAt, 1)
	require.False(t, watchdog.check(now.Add(6*time.Minute)).Recover)

	watchdog.update(now.Add(6*time.Minute), 1, 1, false)
	decision := watchdog.check(now.Add(20 * time.Minute))
	require.False(t, decision.Recover)
	require.Equal(t, 0, decision.QueuedJobs)
}

func TestJobWatchdogRepeatedSignalDoesNotPostponeRecovery(t *testing.T) {
	now := time.Date(2026, time.July, 24, 12, 0, 0, 0, time.UTC)
	watchdog := newJobWatchdog(5 * time.Minute)
	watchdog.update(now, 1, 0, false)

	watchdog.update(now.Add(4*time.Minute), 1, 0, false)

	require.True(t, watchdog.check(now.Add(5*time.Minute)).Recover)
}

func TestJobWatchdogDisarmsWhenGitHubReportsNoAssignedJobs(t *testing.T) {
	now := time.Date(2026, time.July, 24, 12, 0, 0, 0, time.UTC)
	watchdog := newJobWatchdog(5 * time.Minute)
	watchdog.update(now, 1, 0, false)

	watchdog.update(now.Add(4*time.Minute), 0, 0, false)

	require.False(t, watchdog.check(now.Add(30*time.Minute)).Recover)
}

func TestJobWatchdogProgressInvalidatesPendingRecoveryDecision(t *testing.T) {
	now := time.Date(2026, time.July, 24, 12, 0, 0, 0, time.UTC)
	watchdog := newJobWatchdog(5 * time.Minute)
	watchdog.update(now, 2, 0, false)
	decision := watchdog.check(now.Add(5 * time.Minute))
	require.True(t, decision.Recover)

	watchdog.jobStarted(now.Add(5*time.Minute), 1)

	require.False(t, watchdog.recoveryStillDue(now.Add(5*time.Minute), decision.Generation))
}

func TestJobWatchdogRequiresFreshSignalAndFullTimeoutAfterRecovery(t *testing.T) {
	now := time.Date(2026, time.July, 24, 12, 0, 0, 0, time.UTC)
	watchdog := newJobWatchdog(5 * time.Minute)
	watchdog.update(now, 1, 0, false)

	require.True(t, watchdog.check(now.Add(5*time.Minute)).Recover)
	watchdog.finishRecovery()

	require.False(t, watchdog.check(now.Add(30*time.Minute)).Recover)

	freshSignalAt := now.Add(31 * time.Minute)
	watchdog.update(freshSignalAt, 1, 0, false)
	require.False(t, watchdog.check(freshSignalAt.Add(4*time.Minute+59*time.Second)).Recover)
	require.True(t, watchdog.check(freshSignalAt.Add(5*time.Minute)).Recover)
}

func TestRunnerStateRetiresOnlySelectedIdleRunner(t *testing.T) {
	now := time.Date(2026, time.July, 24, 12, 0, 0, 0, time.UTC)
	state := runnerState{
		idle: map[string]runnerInfo{
			"newer":  {createdAt: now.Add(time.Minute)},
			"oldest": {createdAt: now},
		},
		busy: map[string]runnerInfo{
			"busy": {createdAt: now.Add(-time.Hour)},
		},
		retiring: make(map[string]runnerInfo),
	}

	name, _, ok := state.oldestIdle()
	require.True(t, ok)
	require.Equal(t, "oldest", name)

	info, ok := state.beginRetirement(name)

	require.True(t, ok)
	require.Equal(t, now, info.createdAt)
	require.NotContains(t, state.idle, "oldest")
	require.Contains(t, state.idle, "newer")
	require.Contains(t, state.busy, "busy")
	require.Contains(t, state.retiring, "oldest")
}

func TestRunnerStateJobStartCancelsRetirement(t *testing.T) {
	state := runnerState{
		idle:     map[string]runnerInfo{"runner": {}},
		busy:     make(map[string]runnerInfo),
		retiring: make(map[string]runnerInfo),
	}
	_, ok := state.beginRetirement("runner")
	require.True(t, ok)

	require.True(t, state.markBusy("runner"))
	require.NotContains(t, state.retiring, "runner")
	require.Contains(t, state.busy, "runner")
	_, ok = state.finishRetirement("runner")
	require.False(t, ok)
}

func TestRunnerStateRetiringRunnerStillConsumesCapacityUntilItExits(t *testing.T) {
	state := runnerState{
		idle:     map[string]runnerInfo{"idle": {}},
		busy:     map[string]runnerInfo{"busy": {}},
		retiring: map[string]runnerInfo{"retiring": {}},
	}

	require.Equal(t, 3, state.count())
}

func TestRunnerStateIgnoresLateEventsForUnknownRunner(t *testing.T) {
	state := runnerState{idle: make(map[string]runnerInfo), busy: make(map[string]runnerInfo)}

	require.False(t, state.markBusy("already-recycled"))
	_, ok := state.markDone("already-recycled")
	require.False(t, ok)
}

func TestRunnerStateRemembersJobStartedBeforeRunnerBecomesTracked(t *testing.T) {
	state := runnerState{
		idle:        make(map[string]runnerInfo),
		busy:        make(map[string]runnerInfo),
		retiring:    make(map[string]runnerInfo),
		pendingBusy: make(map[string]struct{}),
	}

	require.False(t, state.markBusy("starting-runner"))
	require.False(t, state.addIdle("starting-runner", runnerInfo{}))

	idle, busy := state.counts()
	require.Equal(t, 0, idle)
	require.Equal(t, 1, busy)
}

func TestRunnerStateDoesNotResurrectRunnerCompletedBeforeTracking(t *testing.T) {
	state := runnerState{
		idle:        make(map[string]runnerInfo),
		busy:        make(map[string]runnerInfo),
		retiring:    make(map[string]runnerInfo),
		pendingBusy: make(map[string]struct{}),
		pendingDone: make(map[string]struct{}),
	}

	require.False(t, state.markBusy("fast-runner"))
	_, known := state.markDone("fast-runner")
	require.False(t, known)
	require.True(t, state.addIdle("fast-runner", runnerInfo{}))

	idle, busy := state.counts()
	require.Zero(t, idle)
	require.Zero(t, busy)
}

func TestConfigRejectsZeroJobStartTimeout(t *testing.T) {
	config := Config{
		RegistrationURL: "https://github.com/example",
		ScaleSetName:    "example",
		Token:           "test-token",
		MaxRunners:      1,
	}

	require.ErrorContains(t, config.Validate(), "job start timeout")
}

func TestConfigRejectsNegativeJobStartTimeout(t *testing.T) {
	config := Config{
		RegistrationURL: "https://github.com/example",
		ScaleSetName:    "example",
		Token:           "test-token",
		MaxRunners:      1,
		JobStartTimeout: -time.Second,
	}

	require.ErrorContains(t, config.Validate(), "job start timeout")
}
