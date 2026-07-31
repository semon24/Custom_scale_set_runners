package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/actions/scaleset"
	"github.com/actions/scaleset/listener"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/api/types/volume"
	dockerclient "github.com/docker/docker/client"
	"github.com/docker/docker/errdefs"
	"github.com/google/uuid"
)

const runnerUID = "1001"

const volumeJanitorInterval = 3 * time.Minute
const volumeJanitorMinAge = 10 * time.Minute
const runnerReadyTimeout = 90 * time.Second
const retirementRaceGrace = 15 * time.Second
const retirementPollInterval = 2 * time.Second
const retirementDegradedAfter = 2 * time.Minute

const managedLabel = "ft-soft.runner-scale-set.managed"
const scaleSetNameLabel = "ft-soft.runner-scale-set.name"
const resourceTypeLabel = "ft-soft.runner-scale-set.resource"

type Scaler struct {
	countRunnersUp  int
	justStarted     bool
	runners         runnerState
	scaleMu         sync.Mutex
	janitorOnce     sync.Once
	watchdogOnce    sync.Once
	watchdogWG      sync.WaitGroup
	watchdogCancel  context.CancelFunc
	runnerImage     string
	dindImage       string
	scaleSetName    string
	resourcePrefix  string
	scaleSetID      int
	dockerClient    *dockerclient.Client
	scalesetClient  *scaleset.Client
	minRunners      int
	maxRunners      int
	logger          *slog.Logger
	jobWatchdog     *jobWatchdog
	jobStartTimeout time.Duration
}

func (a *Scaler) HandleDesiredRunnerCount(ctx context.Context, count int) (int, error) {
	a.scaleMu.Lock()
	defer a.scaleMu.Unlock()

	a.startVolumeJanitor(ctx)
	a.startJobWatchdog(ctx)

	currentCount := a.runners.count()
	_, busyCount := a.runners.counts()
	decision := a.jobWatchdog.update(time.Now(), count, busyCount, false)
	if count > 0 {
		a.logger.Info(
			"Received assigned-job signal",
			slog.Int("assignedJobs", count),
			slog.Int("busyRunners", busyCount),
			slog.Int("queuedJobs", decision.QueuedJobs),
			slog.Duration("jobStartTimeout", a.jobStartTimeout),
		)
	}
	targetRunnerCount := min(a.maxRunners, a.minRunners+count)

	switch {
	case targetRunnerCount == currentCount:
		// No scaling needed
		return currentCount, nil
	case targetRunnerCount > currentCount:
		// Scale up
		scaleUp := targetRunnerCount - currentCount
		a.logger.Info(
			"Scaling up runners",
			slog.Int("currentCount", currentCount),
			slog.Int("desiredCount", targetRunnerCount),
			slog.Int("scaleUp", scaleUp),
		)

		errCh := make(chan error, scaleUp)
		var wg sync.WaitGroup

		for i := 0; i < scaleUp; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := a.startRunner(ctx); err != nil {
					errCh <- err
				}
			}()
		}

		wg.Wait()
		close(errCh)

		var failed int
		var firstErr error
		for err := range errCh {
			if failed == 0 {
				firstErr = err
			}
			failed++
		}

		if failed > 0 {
			return a.runners.count(), fmt.Errorf("failed to start %d of %d runners: %w", failed, scaleUp, firstErr)
		}
		_, busyCount = a.runners.counts()
		a.jobWatchdog.update(time.Now(), count, busyCount, true)
		return a.runners.count(), nil
	default:
		// No need to handle scale down events, since:
		// 1. JobCompleted events will first remove runners
		// 2. If the count is still below the current runner count, the JobCompleted event will be delivered in the next batch.
		// 3. Removal after JobCompleted events is handled synchronously.
		// 4. If the job is cancelled, the JobCompleted event will still be delivered.
	}
	return a.runners.count(), nil
}

func (a *Scaler) startJobWatchdog(ctx context.Context) {
	a.watchdogOnce.Do(func() {
		watchdogCtx, cancel := context.WithCancel(ctx)
		a.watchdogCancel = cancel
		checkInterval := min(10*time.Second, a.jobStartTimeout/10)
		if checkInterval <= 0 {
			checkInterval = time.Second
		}
		a.logger.Info(
			"Starting assigned-job watchdog",
			slog.Duration("jobStartTimeout", a.jobStartTimeout),
			slog.Duration("checkInterval", checkInterval),
		)

		a.watchdogWG.Add(1)
		go func() {
			defer a.watchdogWG.Done()
			ticker := time.NewTicker(checkInterval)
			defer ticker.Stop()
			for {
				select {
				case <-watchdogCtx.Done():
					a.logger.Info("Assigned-job watchdog stopped")
					return
				case now := <-ticker.C:
					decision := a.jobWatchdog.check(now)
					if !decision.Recover {
						continue
					}
					a.recoverStaleIdleRunner(watchdogCtx, decision)
				}
			}
		}()
	})
}

func (a *Scaler) stopJobWatchdog() {
	if a.watchdogCancel != nil {
		a.watchdogCancel()
	}
	a.watchdogWG.Wait()
}

func (a *Scaler) recoverStaleIdleRunner(parentCtx context.Context, decision watchdogDecision) {
	recoveryStartedAt := time.Now()
	defer func() {
		a.jobWatchdog.finishRecovery()
	}()

	a.logger.Warn(
		"Assigned job did not start before watchdog timeout",
		slog.Int("queuedJobs", decision.QueuedJobs),
		slog.Duration("waitingFor", decision.WaitingFor),
		slog.Duration("jobStartTimeout", a.jobStartTimeout),
	)

	ctx, cancel := context.WithTimeout(parentCtx, 5*time.Minute)
	defer cancel()

	name, info, ok := a.runners.oldestIdle()
	if ok {
		a.logRunnerDiagnostics(ctx, name, info)
	} else {
		a.logger.Warn("Watchdog found no idle runner to diagnose; busy runners will not be touched")
	}
	a.logGitHubConnectivity(ctx)

	a.scaleMu.Lock()
	defer a.scaleMu.Unlock()

	if !a.jobWatchdog.recoveryStillDue(time.Now(), decision.Generation) {
		a.logger.Info("Watchdog recovery cancelled because runner pool made progress")
		return
	}

	info, ok = a.runners.beginRetirement(name)
	if !ok {
		a.logger.Info("Watchdog recovery cancelled because the diagnosed runner is no longer idle", slog.String("runner", name))
		return
	}
	if info.registrationID == 0 {
		a.runners.restoreRetirement(name)
		a.logger.Error("Cannot safely retire runner without a GitHub registration ID", slog.String("runner", name))
		return
	}

	a.logger.Warn(
		"Deregistering oldest idle runner before replacement",
		slog.String("runner", name),
		slog.String("runnerContainerID", info.runnerID),
		slog.Int64("runnerRegistrationID", info.registrationID),
		slog.Time("runnerCreatedAt", info.createdAt),
	)
	if err := a.scalesetClient.RemoveRunner(ctx, info.registrationID); err != nil {
		a.runners.restoreRetirement(name)
		a.logger.Error(
			"Failed to deregister stale runner; container was left untouched",
			slog.String("runner", name),
			slog.String("error", err.Error()),
		)
		return
	}

	replacementStarted := false
	if a.jobWatchdog.hasQueuedJobs() && a.runners.count() < a.maxRunners {
		replacementName, err := a.startRunner(ctx)
		if err != nil {
			a.logger.Error(
				"Failed to create replacement runner",
				slog.String("retiringRunner", name),
				slog.String("error", err.Error()),
			)
		} else {
			replacementStarted = true
			a.logger.Info(
				"Safe replacement runner started",
				slog.String("retiringRunner", name),
				slog.String("replacementRunner", replacementName),
			)
		}
	}

	a.startRetirementReaper(parentCtx, name, info, !replacementStarted)
	a.logger.Info(
		"Safe idle runner retirement initiated",
		slog.String("retiringRunner", name),
		slog.Bool("replacementStarted", replacementStarted),
		slog.Duration("recoveryDuration", time.Since(recoveryStartedAt)),
	)
}

func (a *Scaler) startRetirementReaper(ctx context.Context, runnerName string, info runnerInfo, startReplacementOnExit bool) {
	a.watchdogWG.Add(1)
	go func() {
		defer a.watchdogWG.Done()

		// Leave a short window for a racing JobStarted event. markBusy moves a
		// retiring runner back to busy, in which case it must never be signalled.
		grace := time.NewTimer(retirementRaceGrace)
		defer grace.Stop()
		select {
		case <-ctx.Done():
			return
		case <-grace.C:
		}
		if !a.runners.isRetiring(runnerName) {
			a.logger.Info("Retirement cancelled because runner accepted a job", slog.String("runner", runnerName))
			return
		}

		// The official runner entrypoint traps TERM when this runner environment
		// variable is enabled. We never escalate to SIGKILL in safe mode.
		if err := a.dockerClient.ContainerKill(ctx, info.runnerID, "SIGTERM"); err != nil {
			a.logger.Warn("Failed to send graceful termination signal to retired runner; continuing to supervise it", slog.String("runner", runnerName), slog.String("error", err.Error()))
		} else {
			a.logger.Info("Sent graceful termination signal to retired runner", slog.String("runner", runnerName), slog.String("signal", "SIGTERM"))
		}

		startedAt := time.Now()
		ticker := time.NewTicker(retirementPollInterval)
		defer ticker.Stop()
		replacementStarted := !startReplacementOnExit
		degradedLogged := false
		lastInspectWarning := time.Time{}

		for {
			if !a.runners.isRetiring(runnerName) {
				a.logger.Info("Retirement cleanup skipped because runner accepted a job", slog.String("runner", runnerName))
				return
			}

			inspect, err := a.dockerClient.ContainerInspect(ctx, info.runnerID)
			if err != nil {
				if errdefs.IsNotFound(err) {
					a.logger.Info("Retired runner container no longer exists", slog.String("runner", runnerName))
					break
				}
				if lastInspectWarning.IsZero() || time.Since(lastInspectWarning) >= 30*time.Second {
					a.logger.Warn("Failed to inspect retired runner; will retry", slog.String("runner", runnerName), slog.String("error", err.Error()))
					lastInspectWarning = time.Now()
				}
			} else if inspect.State != nil && !inspect.State.Running {
				a.logger.Info("Retired runner container exited", slog.String("runner", runnerName), slog.Int("exitCode", inspect.State.ExitCode))
				break
			}

			if !replacementStarted && time.Since(startedAt) >= retirementDegradedAfter && a.jobWatchdog.hasQueuedJobs() {
				// The old runner is already deregistered, so this keeps GitHub's
				// registered runner count within maxRunners. Docker may temporarily
				// contain one extra, gracefully shutting-down container.
				a.scaleMu.Lock()
				if a.jobWatchdog.hasQueuedJobs() && a.runners.registeredCount() < a.maxRunners {
					replacementName, startErr := a.startRunner(ctx)
					if startErr != nil {
						a.logger.Error("Failed to start degraded replacement while retired runner is shutting down", slog.String("retiringRunner", runnerName), slog.String("error", startErr.Error()))
					} else {
						replacementStarted = true
						a.logger.Warn("Started replacement while deregistered runner is still shutting down", slog.String("retiringRunner", runnerName), slog.String("replacementRunner", replacementName))
					}
				}
				a.scaleMu.Unlock()
				if !degradedLogged {
					a.logger.Warn("Retired runner did not exit after graceful signal", slog.String("runner", runnerName), slog.Duration("waitingFor", time.Since(startedAt)))
					degradedLogged = true
				}
			}

			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}

		retiredInfo, ok := a.runners.finishRetirement(runnerName)
		if !ok {
			a.logger.Info("Retirement cleanup skipped because runner accepted a job", slog.String("runner", runnerName))
			return
		}

		cleanupCtx, cancel := context.WithTimeout(ctx, time.Minute)
		defer cancel()
		if err := a.removeRunnerResources(cleanupCtx, runnerName, retiredInfo); err != nil {
			a.logger.Error("Failed to clean up exited retired runner", slog.String("runner", runnerName), slog.String("error", err.Error()))
		}

		if replacementStarted || !a.jobWatchdog.hasQueuedJobs() {
			return
		}
		a.scaleMu.Lock()
		defer a.scaleMu.Unlock()
		if !a.jobWatchdog.hasQueuedJobs() || a.runners.count() >= a.maxRunners {
			return
		}
		replacementName, err := a.startRunner(ctx)
		if err != nil {
			a.logger.Error("Failed to create replacement after retired runner exited", slog.String("retiredRunner", runnerName), slog.String("error", err.Error()))
			return
		}
		a.logger.Info("Replacement runner started after safe retirement", slog.String("retiredRunner", runnerName), slog.String("replacementRunner", replacementName))
	}()
}

func (a *Scaler) startVolumeJanitor(ctx context.Context) {
	a.janitorOnce.Do(func() {
		a.logger.Info("Starting volume janitor", slog.Duration("interval", volumeJanitorInterval))
		go func() {
			ticker := time.NewTicker(volumeJanitorInterval)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					a.logger.Info("Volume janitor stopped")
					return
				case <-ticker.C:
					a.cleanupDanglingVolumes()
				}
			}
		}()
	})
}

func (a *Scaler) managedResourceLabels(resourceType string) map[string]string {
	return map[string]string{
		managedLabel:      "true",
		scaleSetNameLabel: a.scaleSetName,
		resourceTypeLabel: resourceType,
	}
}

func (a *Scaler) hasManagedLabels(labels map[string]string) bool {
	return labels[managedLabel] == "true" && labels[scaleSetNameLabel] == a.scaleSetName
}

func (a *Scaler) hasResourceName(name, resourceType string) bool {
	prefix := normalizeResourcePrefix(a.scaleSetName)
	return strings.HasPrefix(strings.TrimPrefix(name, "/"), prefix+"-"+resourceType+"-")
}

func (a *Scaler) cleanupStaleResources(ctx context.Context) error {
	containers, err := a.dockerClient.ContainerList(ctx, container.ListOptions{
		All: true,
	})
	if err != nil {
		return fmt.Errorf("failed to list stale containers: %w", err)
	}

	var cleanupErrors []string
	for _, staleContainer := range containers {
		ownedByName := false
		for _, name := range staleContainer.Names {
			if a.hasResourceName(name, "runner") || a.hasResourceName(name, "dind") {
				ownedByName = true
				break
			}
		}
		if !a.hasManagedLabels(staleContainer.Labels) && !ownedByName {
			continue
		}

		if err := a.dockerClient.ContainerRemove(ctx, staleContainer.ID, container.RemoveOptions{
			Force:         true,
			RemoveVolumes: true,
		}); err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Sprintf("remove container %s: %v", staleContainer.ID, err))
			continue
		}
		a.logger.Info("Removed stale container", slog.String("containerID", staleContainer.ID))
	}

	volumes, err := a.dockerClient.VolumeList(ctx, volume.ListOptions{Filters: filters.NewArgs()})
	if err != nil {
		cleanupErrors = append(cleanupErrors, fmt.Sprintf("list stale volumes: %v", err))
	} else {
		for _, staleVolume := range volumes.Volumes {
			if !a.hasManagedLabels(staleVolume.Labels) && !a.hasResourceName(staleVolume.Name, "workspace") {
				continue
			}
			if err := a.dockerClient.VolumeRemove(ctx, staleVolume.Name, true); err != nil {
				cleanupErrors = append(cleanupErrors, fmt.Sprintf("remove volume %s: %v", staleVolume.Name, err))
				continue
			}
			a.logger.Info("Removed stale volume", slog.String("volume", staleVolume.Name))
		}
	}

	networks, err := a.dockerClient.NetworkList(ctx, network.ListOptions{Filters: filters.NewArgs()})
	if err != nil {
		cleanupErrors = append(cleanupErrors, fmt.Sprintf("list stale networks: %v", err))
	} else {
		for _, staleNetwork := range networks {
			if !a.hasManagedLabels(staleNetwork.Labels) && !a.hasResourceName(staleNetwork.Name, "net") {
				continue
			}
			if err := a.dockerClient.NetworkRemove(ctx, staleNetwork.ID); err != nil {
				cleanupErrors = append(cleanupErrors, fmt.Sprintf("remove network %s: %v", staleNetwork.Name, err))
				continue
			}
			a.logger.Info("Removed stale network", slog.String("network", staleNetwork.Name))
		}
	}

	if len(cleanupErrors) > 0 {
		return fmt.Errorf("stale resource cleanup failed: %s", strings.Join(cleanupErrors, "; "))
	}

	a.logger.Info("Stale runner resource cleanup completed", slog.String("scaleSetName", a.scaleSetName))
	return nil
}

func (a *Scaler) cleanupDanglingVolumes() {
	runCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	trackedWorkspaceVolumes := a.trackedWorkspaceVolumes()

	list, err := a.dockerClient.VolumeList(runCtx, volume.ListOptions{
		Filters: filters.NewArgs(filters.Arg("dangling", "true")),
	})
	if err != nil {
		a.logger.Warn("Failed to list dangling volumes", slog.String("error", err.Error()))
		return
	}

	removed := 0
	for _, v := range list.Volumes {
		if !isJanitorCandidateVolume(v.Name) {
			continue
		}

		if _, tracked := trackedWorkspaceVolumes[v.Name]; tracked {
			continue
		}

		if !isVolumeOldEnough(v.CreatedAt, volumeJanitorMinAge) {
			continue
		}

		if err := a.dockerClient.VolumeRemove(runCtx, v.Name, true); err != nil {
			a.logger.Warn("Failed to remove dangling volume", slog.String("volume", v.Name), slog.String("error", err.Error()))
			continue
		}

		removed++
		a.logger.Info("Removed dangling volume", slog.String("volume", v.Name))
	}

	if removed > 0 {
		a.logger.Info("Volume janitor run completed", slog.Int("removed", removed))
	}
}

func (a *Scaler) trackedWorkspaceVolumes() map[string]struct{} {
	volumes := make(map[string]struct{})

	a.runners.mu.Lock()
	defer a.runners.mu.Unlock()

	for _, info := range a.runners.idle {
		if info.workspaceVol != "" {
			volumes[info.workspaceVol] = struct{}{}
		}
	}

	for _, info := range a.runners.busy {
		if info.workspaceVol != "" {
			volumes[info.workspaceVol] = struct{}{}
		}
	}

	for _, info := range a.runners.retiring {
		if info.workspaceVol != "" {
			volumes[info.workspaceVol] = struct{}{}
		}
	}

	return volumes
}

func isJanitorCandidateVolume(name string) bool {
	if strings.HasPrefix(name, "workspace-") || strings.Contains(name, "-workspace-") {
		return true
	}

	return isLikelyAnonymousVolumeName(name)
}

func isLikelyAnonymousVolumeName(name string) bool {
	if len(name) != 64 {
		return false
	}

	for _, ch := range name {
		switch {
		case ch >= '0' && ch <= '9':
		case ch >= 'a' && ch <= 'f':
		default:
			return false
		}
	}

	return true
}

func isVolumeOldEnough(createdAt string, minAge time.Duration) bool {
	if createdAt == "" {
		return true
	}

	parsed, err := time.Parse(time.RFC3339Nano, createdAt)
	if err != nil {
		parsed, err = time.Parse(time.RFC3339, createdAt)
		if err != nil {
			return true
		}
	}

	return time.Since(parsed) >= minAge
}

func (a *Scaler) HandleJobStarted(ctx context.Context, jobInfo *scaleset.JobStarted) error {
	a.logger.Info(
		"Job started",
		slog.Int64("runnerRequestId", jobInfo.RunnerRequestID),
		slog.String("jobId", jobInfo.JobID),
	)
	knownRunner := a.runners.markBusy(jobInfo.RunnerName)
	if !knownRunner {
		a.logger.Warn(
			"Received job-start event before runner became tracked",
			slog.String("runner", jobInfo.RunnerName),
			slog.Int64("runnerRequestId", jobInfo.RunnerRequestID),
			slog.String("jobId", jobInfo.JobID),
		)
	}
	_, busyCount := a.runners.counts()
	if !knownRunner {
		busyCount++
	}
	a.jobWatchdog.jobStarted(time.Now(), busyCount)
	return nil
}

func (a *Scaler) HandleJobCompleted(ctx context.Context, jobInfo *scaleset.JobCompleted) error {
	a.logger.Info("Job completed", slog.Int64("runnerRequestId", jobInfo.RunnerRequestID), slog.String("jobId", jobInfo.JobID))

	info, ok := a.runners.markDone(jobInfo.RunnerName)
	if !ok {
		a.logger.Warn(
			"Received job-completed event for an unknown or recycled runner",
			slog.String("runner", jobInfo.RunnerName),
			slog.Int64("runnerRequestId", jobInfo.RunnerRequestID),
			slog.String("jobId", jobInfo.JobID),
		)
		return nil
	}
	return a.removeRunnerResources(ctx, jobInfo.RunnerName, info)
}

func (a *Scaler) startRunner(ctx context.Context) (string, error) {
	names := newRunnerResourceNames(a.resourcePrefix, uuid.NewString()[:8])
	name := names.runner
	dindName := names.dind
	networkName := names.network
	workspaceVolName := names.workspace
	dockerHost := "tcp://docker:2375"

	a.logger.Info("Starting runner with image", "image", a.runnerImage)

	netResp, err := a.dockerClient.NetworkCreate(ctx, networkName, network.CreateOptions{
		Labels: a.managedResourceLabels("network"),
	})
	if err != nil {
		return "", fmt.Errorf("failed to create pair network: %w", err)
	}

	cleanup := runnerInfo{networkID: netResp.ID, networkName: networkName}
	cleanupNeeded := true
	defer func() {
		if !cleanupNeeded {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
		defer cancel()
		if cleanup.registrationID != 0 {
			if err := a.scalesetClient.RemoveRunner(cleanupCtx, cleanup.registrationID); err != nil {
				a.logger.Error("Failed to rollback runner registration", slog.String("runner", name), slog.Int64("runnerRegistrationID", cleanup.registrationID), slog.String("error", err.Error()))
			}
		}
		if err := a.removeRunnerResources(cleanupCtx, name, cleanup); err != nil {
			a.logger.Error("Failed to rollback runner resources", slog.String("runner", name), slog.String("error", err.Error()))
		}
	}()

	// Create shared workspace volume (runner checkout writes here; dind mounts it so container actions see files)
	vol, err := a.dockerClient.VolumeCreate(ctx, volume.CreateOptions{
		Name:   workspaceVolName,
		Labels: a.managedResourceLabels("workspace"),
	})
	if err != nil {
		return "", fmt.Errorf("failed to create workspace volume: %w", err)
	}
	cleanup.workspaceVol = vol.Name
	a.logger.Info("Workspace volume created", slog.String("runner", name), slog.String("volume", vol.Name))

	// Create dind
	dindC, err := a.dockerClient.ContainerCreate(
		ctx,
		&container.Config{
			Image:      a.dindImage,
			Labels:     a.managedResourceLabels("dind"),
			Env:        []string{"DOCKER_TLS_CERTDIR="},
			Entrypoint: []string{"sh", "-lc"},
			Cmd: []string{
				fmt.Sprintf("mkdir -p /home/runner/_work && chown -R %s:%s /home/runner/_work && exec dockerd-entrypoint.sh", runnerUID, runnerUID),
			},
			Healthcheck: &container.HealthConfig{
				Test:        []string{"CMD-SHELL", "docker info >/dev/null 2>&1"},
				Interval:    2 * time.Second,
				Timeout:     2 * time.Second,
				Retries:     15,
				StartPeriod: 5 * time.Second,
			},
		},
		&container.HostConfig{
			Privileged:  true,
			NetworkMode: container.NetworkMode(networkName),
			Binds:       []string{vol.Name + ":/home/runner/_work"},
		},
		&network.NetworkingConfig{
			EndpointsConfig: map[string]*network.EndpointSettings{
				networkName: {
					Aliases: []string{"docker", dindName},
				},
			},
		},
		nil,
		dindName,
	)
	if err != nil {
		return "", fmt.Errorf("failed to create dind container: %w", err)
	}
	cleanup.dindID = dindC.ID

	// Start dind
	if err := a.dockerClient.ContainerStart(ctx, dindC.ID, container.StartOptions{}); err != nil {
		return "", fmt.Errorf("failed to start dind container: %w", err)
	}

	if err := a.waitForDindReady(ctx, dindC.ID, 45*time.Second); err != nil {
		return "", fmt.Errorf("dind did not become ready: %w", err)
	}

	jit, err := a.scalesetClient.GenerateJitRunnerConfig(
		ctx,
		&scaleset.RunnerScaleSetJitRunnerSetting{
			Name: name,
		},
		a.scaleSetID,
	)
	if err != nil {
		return "", fmt.Errorf("failed to generate JIT config: %w", err)
	}
	if jit.Runner == nil || jit.Runner.ID == 0 {
		return "", fmt.Errorf("generated JIT config has no runner registration ID")
	}
	cleanup.registrationID = int64(jit.Runner.ID)

	c, err := a.dockerClient.ContainerCreate(
		ctx,
		&container.Config{
			Image:      a.runnerImage,
			Labels:     a.managedResourceLabels("runner"),
			Entrypoint: []string{"/home/runner/run.sh"},
			Healthcheck: &container.HealthConfig{
				Test:        []string{"CMD-SHELL", "pgrep -f Runner.Listener >/dev/null 2>&1"},
				Interval:    5 * time.Second,
				Timeout:     2 * time.Second,
				Retries:     12,
				StartPeriod: 20 * time.Second,
			},
			Env: []string{
				fmt.Sprintf("ACTIONS_RUNNER_INPUT_JITCONFIG=%s", jit.EncodedJITConfig),
				fmt.Sprintf("DOCKER_HOST=%s", dockerHost),
				"RUNNER_MANUALLY_TRAP_SIG=1",
			},
		},
		&container.HostConfig{
			NetworkMode: container.NetworkMode(networkName),
			Binds: []string{
				vol.Name + ":/home/runner/_work",
				"/opt/build-cache:/opt/build-cache/",
				"/opt/build-cache-npm:/opt/build-cache-npm/",
			},
		},
		&network.NetworkingConfig{
			EndpointsConfig: map[string]*network.EndpointSettings{
				networkName: {},
			},
		},
		nil,
		name,
	)
	if err != nil {
		return "", fmt.Errorf("failed to create runner container: %w", err)
	}
	cleanup.runnerID = c.ID

	if err := a.dockerClient.ContainerStart(ctx, c.ID, container.StartOptions{}); err != nil {
		return "", fmt.Errorf("failed to start runner container: %w", err)
	}

	if err := a.waitForRunnerReady(ctx, c.ID, name, runnerReadyTimeout); err != nil {
		return "", fmt.Errorf("runner did not become ready: %w", err)
	}

	completedBeforeTracking := a.runners.addIdle(name, runnerInfo{
		runnerID:       c.ID,
		dindID:         dindC.ID,
		networkID:      netResp.ID,
		networkName:    networkName,
		workspaceVol:   vol.Name,
		registrationID: int64(jit.Runner.ID),
		createdAt:      time.Now(),
	})
	if completedBeforeTracking {
		return "", fmt.Errorf("runner completed before it became ready for tracking")
	}
	cleanupNeeded = false
	return name, nil
}

type runnerResourceNames struct {
	runner    string
	dind      string
	network   string
	workspace string
}

func newRunnerResourceNames(prefix, suffix string) runnerResourceNames {
	prefix = normalizeResourcePrefix(prefix)
	return runnerResourceNames{
		runner:    fmt.Sprintf("%s-runner-%s", prefix, suffix),
		dind:      fmt.Sprintf("%s-dind-%s", prefix, suffix),
		network:   fmt.Sprintf("%s-net-%s", prefix, suffix),
		workspace: fmt.Sprintf("%s-workspace-%s", prefix, suffix),
	}
}

func normalizeResourcePrefix(value string) string {
	value = strings.TrimSpace(value)
	var result strings.Builder
	result.Grow(len(value))

	lastWasSeparator := false
	for _, ch := range value {
		valid := ch >= 'a' && ch <= 'z' ||
			ch >= 'A' && ch <= 'Z' ||
			ch >= '0' && ch <= '9' ||
			ch == '_' || ch == '.' || ch == '-'
		if valid {
			result.WriteRune(ch)
			lastWasSeparator = false
			continue
		}
		if !lastWasSeparator {
			result.WriteByte('-')
			lastWasSeparator = true
		}
	}

	prefix := strings.Trim(result.String(), "._-")
	if prefix == "" {
		return "scale-set"
	}
	if len(prefix) > 48 {
		prefix = strings.TrimRight(prefix[:48], "._-")
	}
	return prefix
}

func (a *Scaler) removeRunnerResources(ctx context.Context, runnerName string, info runnerInfo) error {
	var errs []string

	if info.runnerID != "" {
		if err := a.dockerClient.ContainerRemove(ctx, info.runnerID, container.RemoveOptions{Force: true, RemoveVolumes: true}); err != nil {
			errs = append(errs, fmt.Sprintf("remove runner container: %v", err))
		}
	}

	if info.dindID != "" {
		if err := a.dockerClient.ContainerRemove(ctx, info.dindID, container.RemoveOptions{Force: true, RemoveVolumes: true}); err != nil {
			errs = append(errs, fmt.Sprintf("remove dind container: %v", err))
		}
	}

	if info.workspaceVol != "" {
		if err := a.dockerClient.VolumeRemove(ctx, info.workspaceVol, true); err != nil {
			a.logger.Error("Failed to remove workspace volume", slog.String("runner", runnerName), slog.String("volume", info.workspaceVol), slog.String("error", err.Error()))
			errs = append(errs, fmt.Sprintf("remove workspace volume: %v", err))
		} else {
			a.logger.Info("Workspace volume removed", slog.String("runner", runnerName), slog.String("volume", info.workspaceVol))
		}
	}

	networkRef := info.networkID
	if networkRef == "" {
		networkRef = info.networkName
	}
	if networkRef != "" {
		if err := a.dockerClient.NetworkRemove(ctx, networkRef); err != nil {
			a.logger.Error("Failed to remove pair network", slog.String("runner", runnerName), slog.String("network", networkRef), slog.String("error", err.Error()))
			errs = append(errs, fmt.Sprintf("remove pair network: %v", err))
		} else {
			a.logger.Info("Pair network removed", slog.String("runner", runnerName), slog.String("network", networkRef))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("%s", strings.Join(errs, "; "))
	}
	return nil
}

func (a *Scaler) waitForRunnerReady(ctx context.Context, runnerContainerID, runnerName string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error

	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		inspect, err := a.dockerClient.ContainerInspect(ctx, runnerContainerID)
		if err != nil {
			lastErr = err
			time.Sleep(1 * time.Second)
			continue
		}

		if inspect.State == nil {
			lastErr = fmt.Errorf("runner state is nil")
			time.Sleep(1 * time.Second)
			continue
		}

		if !inspect.State.Running {
			lastErr = fmt.Errorf("runner is not running (status=%s)", inspect.State.Status)
			time.Sleep(1 * time.Second)
			continue
		}

		healthReady := false
		if inspect.State.Health == nil {
			lastErr = fmt.Errorf("runner has no health status yet")
		} else if inspect.State.Health.Status != "healthy" {
			lastErr = fmt.Errorf("runner healthcheck status is %s", inspect.State.Health.Status)
		} else {
			healthReady = true
		}

		listening, err := a.runnerIsListeningForJobs(ctx, runnerContainerID)
		if err != nil {
			lastErr = err
			time.Sleep(1 * time.Second)
			continue
		}

		if healthReady && listening {
			a.logger.Info("Runner is fully ready and waiting for jobs", slog.String("runner", runnerName), slog.String("containerID", runnerContainerID))
			return nil
		}

		if !listening {
			lastErr = fmt.Errorf("runner has not reached 'Listening for Jobs' state yet")
		}

		time.Sleep(1 * time.Second)
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("timeout exceeded")
	}
	return lastErr
}

func (a *Scaler) runnerIsListeningForJobs(ctx context.Context, runnerContainerID string) (bool, error) {
	logsReader, err := a.dockerClient.ContainerLogs(ctx, runnerContainerID, container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Tail:       "200",
	})
	if err != nil {
		return false, err
	}
	defer logsReader.Close()

	logBytes, err := io.ReadAll(logsReader)
	if err != nil {
		return false, err
	}

	logsText := string(logBytes)
	if strings.Contains(logsText, "Listening for Jobs") || strings.Contains(logsText, "Listening for jobs") {
		return true, nil
	}

	return false, nil
}

func (a *Scaler) waitForDindReady(ctx context.Context, dindContainerID string, timeout time.Duration) error {

	deadline := time.Now().Add(timeout)
	var lastErr error

	for time.Now().Before(deadline) {
		inspect, err := a.dockerClient.ContainerInspect(ctx, dindContainerID)
		if err != nil {
			lastErr = err
			time.Sleep(1 * time.Second)
			continue
		}

		if inspect.State == nil {
			lastErr = fmt.Errorf("dind state is nil")
			time.Sleep(1 * time.Second)
			continue
		}

		if !inspect.State.Running {
			lastErr = fmt.Errorf("dind is not running (status=%s)", inspect.State.Status)
			time.Sleep(1 * time.Second)
			continue
		}

		if inspect.State.Health == nil {
			lastErr = fmt.Errorf("dind has no health status yet")
			time.Sleep(1 * time.Second)
			continue
		}

		switch inspect.State.Health.Status {
		case "healthy":
			a.logger.Info("dind is ready", slog.String("containerID", dindContainerID), slog.String("health", inspect.State.Health.Status))
			return nil
		case "unhealthy":
			lastErr = fmt.Errorf("dind healthcheck is unhealthy")
		default:
			lastErr = fmt.Errorf("dind healthcheck status is %s", inspect.State.Health.Status)
		}

		time.Sleep(1 * time.Second)
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("timeout exceeded")
	}
	return lastErr
}

func (a *Scaler) shutdown(ctx context.Context) {
	a.logger.Info("Shutting down runners")
	a.runners.mu.Lock()
	defer a.runners.mu.Unlock()

	removeRunner := func(name string, info runnerInfo) {
		a.logger.Info("Removing runner", slog.String("name", name), slog.String("runnerID", info.runnerID))
		if err := a.removeRunnerResources(ctx, name, info); err != nil {
			a.logger.Error("Failed to remove runner resources", slog.String("name", name), slog.String("error", err.Error()))
		}
	}

	for name, info := range a.runners.idle {
		removeRunner(name, info)
	}
	clear(a.runners.idle)

	for name, info := range a.runners.busy {
		removeRunner(name, info)
	}
	clear(a.runners.busy)

	for name, info := range a.runners.retiring {
		removeRunner(name, info)
	}
	clear(a.runners.retiring)
}

var _ listener.Scaler = (*Scaler)(nil)

type runnerInfo struct {
	runnerID       string
	dindID         string
	networkID      string
	networkName    string
	workspaceVol   string
	registrationID int64
	createdAt      time.Time
}

type runnerState struct {
	mu          sync.Mutex
	idle        map[string]runnerInfo
	busy        map[string]runnerInfo
	retiring    map[string]runnerInfo
	pendingBusy map[string]struct{}
	pendingDone map[string]struct{}
}

func (r *runnerState) count() int {
	r.mu.Lock()
	count := len(r.idle) + len(r.busy) + len(r.retiring)
	r.mu.Unlock()
	return count
}

func (r *runnerState) counts() (idle, busy int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.idle), len(r.busy)
}

func (r *runnerState) registeredCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.idle) + len(r.busy)
}

func (r *runnerState) isRetiring(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.retiring[name]
	return ok
}

func (r *runnerState) oldestIdle() (string, runnerInfo, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.oldestIdleUnlocked()
}

func (r *runnerState) oldestIdleUnlocked() (string, runnerInfo, bool) {
	var oldestName string
	var oldestInfo runnerInfo
	for name, info := range r.idle {
		if oldestName == "" || info.createdAt.Before(oldestInfo.createdAt) {
			oldestName = name
			oldestInfo = info
		}
	}
	if oldestName == "" {
		return "", runnerInfo{}, false
	}
	return oldestName, oldestInfo, true
}

func (r *runnerState) markBusy(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	info, ok := r.idle[name]
	if !ok {
		info, ok = r.retiring[name]
		if !ok {
			if _, completed := r.pendingDone[name]; completed {
				return false
			}
			if r.pendingBusy == nil {
				r.pendingBusy = make(map[string]struct{})
			}
			r.pendingBusy[name] = struct{}{}
			return false
		}
		delete(r.retiring, name)
	} else {
		delete(r.idle, name)
	}
	r.busy[name] = info
	return true
}

func (r *runnerState) markDone(name string) (runnerInfo, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.markDoneUnlocked(name)
}

func (r *runnerState) markDoneUnlocked(name string) (runnerInfo, bool) {
	info, ok := r.busy[name]
	if ok {
		delete(r.busy, name)
		return info, true
	}
	info, ok = r.idle[name]
	if ok {
		delete(r.idle, name)
		return info, true
	}
	info, ok = r.retiring[name]
	if ok {
		delete(r.retiring, name)
		return info, true
	}
	if _, startedBeforeTracking := r.pendingBusy[name]; startedBeforeTracking {
		delete(r.pendingBusy, name)
		if r.pendingDone == nil {
			r.pendingDone = make(map[string]struct{})
		}
		r.pendingDone[name] = struct{}{}
	}
	return runnerInfo{}, false
}

// addIdle returns true when start and completion events both arrived before
// the runner finished its readiness checks. The caller must clean it up rather
// than resurrecting the completed ephemeral runner as idle.
func (r *runnerState) addIdle(name string, info runnerInfo) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, alreadyDone := r.pendingDone[name]; alreadyDone {
		delete(r.pendingDone, name)
		return true
	}
	if _, alreadyBusy := r.pendingBusy[name]; alreadyBusy {
		delete(r.pendingBusy, name)
		r.busy[name] = info
	} else {
		r.idle[name] = info
	}
	return false
}

func (r *runnerState) beginRetirement(name string) (runnerInfo, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	info, ok := r.idle[name]
	if !ok {
		return runnerInfo{}, false
	}
	delete(r.idle, name)
	r.retiring[name] = info
	return info, true
}

func (r *runnerState) restoreRetirement(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	info, ok := r.retiring[name]
	if !ok {
		return false
	}
	delete(r.retiring, name)
	r.idle[name] = info
	return true
}

func (r *runnerState) finishRetirement(name string) (runnerInfo, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	info, ok := r.retiring[name]
	if !ok {
		return runnerInfo{}, false
	}
	delete(r.retiring, name)
	return info, true
}
