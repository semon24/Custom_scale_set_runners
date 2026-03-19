package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/actions/scaleset"
	"github.com/actions/scaleset/listener"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/volume"
	dockerclient "github.com/docker/docker/client"
	"github.com/google/uuid"
)

const runnerUID = "1001"

const volumeJanitorInterval = 3 * time.Minute

type Scaler struct {
	countRunnersUp    int
	justStarted       bool
	runners           runnerState
	janitorOnce       sync.Once
	runnerImage       string
	dindImage         string
	sharedNetworkName string
	scaleSetID        int
	dockerClient       *dockerclient.Client
	scalesetClient *scaleset.Client
	minRunners     int
	maxRunners     int
	logger         *slog.Logger
}

func (a *Scaler) HandleDesiredRunnerCount(ctx context.Context, count int) (int, error) {
	a.startVolumeJanitor(ctx)

	currentCount := a.runners.count()
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

		for range scaleUp {
			if _, err := a.startRunner(ctx); err != nil {
				return 0, fmt.Errorf("failed to start runner: %w", err)
			}
		}

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

func (a *Scaler) startVolumeJanitor(ctx context.Context) {
	a.janitorOnce.Do(func() {
		a.logger.Info("Starting volume janitor", slog.Duration("interval", volumeJanitorInterval))
		go func() {
			ticker := time.NewTicker(volumeJanitorInterval)
			defer ticker.Stop()

			a.cleanupDanglingVolumes()

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

func (a *Scaler) cleanupDanglingVolumes() {
	runCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

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

func isJanitorCandidateVolume(name string) bool {
	if strings.HasPrefix(name, "workspace-") {
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

func (a *Scaler) HandleJobStarted(ctx context.Context, jobInfo *scaleset.JobStarted) error {
	a.logger.Info(
		"Job started",
		slog.Int64("runnerRequestId", jobInfo.RunnerRequestID),
		slog.String("jobId", jobInfo.JobID),
	)
	a.runners.markBusy(jobInfo.RunnerName)
	return nil
}

func (a *Scaler) HandleJobCompleted(ctx context.Context, jobInfo *scaleset.JobCompleted) error {
	a.logger.Info("Job completed", slog.Int64("runnerRequestId", jobInfo.RunnerRequestID), slog.String("jobId", jobInfo.JobID))

	info := a.runners.markDone(jobInfo.RunnerName)
	if err := a.dockerClient.ContainerRemove(ctx, info.runnerID, container.RemoveOptions{Force: true, RemoveVolumes: true}); err != nil {
		return fmt.Errorf("failed to remove runner container: %w", err)
	}
	if err := a.dockerClient.ContainerRemove(ctx, info.dindID, container.RemoveOptions{Force: true, RemoveVolumes: true}); err != nil {
		return fmt.Errorf("failed to remove dind container: %w", err)
	}
	if err := a.dockerClient.VolumeRemove(ctx, info.workspaceVol, true); err != nil {
		a.logger.Error("Failed to remove workspace volume", slog.String("volume", info.workspaceVol), slog.String("error", err.Error()))
	} else {
		a.logger.Info("Workspace volume removed", slog.String("runner", jobInfo.RunnerName), slog.String("volume", info.workspaceVol))
	}
	// Network is shared, removed on shutdown

	return nil
}

func (a *Scaler) startRunner(ctx context.Context) (string, error) {
	name := fmt.Sprintf("runner-%s", uuid.NewString()[:8])
	dindName := fmt.Sprintf("dind-%s", name)
	workspaceVolName := fmt.Sprintf("workspace-%s", name)

	a.logger.Info("Starting runner with image", "image", a.runnerImage)

	// Create shared workspace volume (runner checkout writes here; dind mounts it so container actions see files)
	vol, err := a.dockerClient.VolumeCreate(ctx, volume.CreateOptions{Name: workspaceVolName})
	if err != nil {
		return "", fmt.Errorf("failed to create workspace volume: %w", err)
	}
	a.logger.Info("Workspace volume created", slog.String("runner", name), slog.String("volume", vol.Name))

	// Create dind
	dindC, err := a.dockerClient.ContainerCreate(
		ctx,
		&container.Config{
			Image:      a.dindImage,
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
			NetworkMode: container.NetworkMode(a.sharedNetworkName),
			Binds:       []string{vol.Name + ":/home/runner/_work"},
		},
		nil, nil,
		dindName,
	)
	if err != nil {
		return "", fmt.Errorf("failed to create dind container: %w", err)
	}

	// Start dind
	if err := a.dockerClient.ContainerStart(ctx, dindC.ID, container.StartOptions{}); err != nil {
		return "", fmt.Errorf("failed to start dind container: %w", err)
	}

	if err := a.waitForDindReady(ctx, dindC.ID, 45*time.Second); err != nil {
		return "", fmt.Errorf("dind did not become ready: %w", err)
	}

	dockerHost := fmt.Sprintf("tcp://%s:2375", dindName)

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

	c, err := a.dockerClient.ContainerCreate(
		ctx,
		&container.Config{
			Image:      a.runnerImage,
			Entrypoint: []string{"/home/runner/run.sh"},
			Env: []string{
				fmt.Sprintf("ACTIONS_RUNNER_INPUT_JITCONFIG=%s", jit.EncodedJITConfig),
				fmt.Sprintf("DOCKER_HOST=%s", dockerHost),
			},
		},
		&container.HostConfig{
			NetworkMode: container.NetworkMode(a.sharedNetworkName),
			Binds: []string{
				vol.Name + ":/home/runner/_work",
				"/opt/build-cache:/opt/build-cache/",
				"/opt/build-cache-npm:/opt/build-cache-npm/",
			},
		},
		nil, nil,
		name,
	)
	if err != nil {
		return "", fmt.Errorf("failed to create runner container: %w", err)
	}

	if err := a.dockerClient.ContainerStart(ctx, c.ID, container.StartOptions{}); err != nil {
		return "", fmt.Errorf("failed to start runner container: %w", err)
	}

	a.runners.addIdle(name, runnerInfo{
		runnerID:     c.ID,
		dindID:       dindC.ID,
		workspaceVol: vol.Name,
	})
	return name, nil
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
		if err := a.dockerClient.ContainerRemove(ctx, info.runnerID, container.RemoveOptions{Force: true, RemoveVolumes: true}); err != nil {
			a.logger.Error("Failed to remove runner container", slog.String("name", name), slog.String("error", err.Error()))
		}
		if err := a.dockerClient.ContainerRemove(ctx, info.dindID, container.RemoveOptions{Force: true, RemoveVolumes: true}); err != nil {
			a.logger.Error("Failed to remove dind container", slog.String("name", name), slog.String("error", err.Error()))
		}
		if err := a.dockerClient.VolumeRemove(ctx, info.workspaceVol, true); err != nil {
			a.logger.Error("Failed to remove workspace volume", slog.String("name", name), slog.String("error", err.Error()))
		} else {
			a.logger.Info("Workspace volume removed", slog.String("runner", name), slog.String("volume", info.workspaceVol))
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
}

var _ listener.Scaler = (*Scaler)(nil)

type runnerInfo struct {
	runnerID     string
	dindID       string
	workspaceVol string
}

type runnerState struct {
	mu   sync.Mutex
	idle map[string]runnerInfo
	busy map[string]runnerInfo
}

func (r *runnerState) count() int {
	r.mu.Lock()
	count := len(r.idle) + len(r.busy)
	r.mu.Unlock()
	return count
}

func (r *runnerState) markBusy(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	info, ok := r.idle[name]
	if !ok {
		panic("marking non-existent runner busy")
	}
	delete(r.idle, name)
	r.busy[name] = info
}

func (r *runnerState) markDone(name string) runnerInfo {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.markDoneUnlocked(name)
}

func (r *runnerState) markDoneUnlocked(name string) runnerInfo {
	info, ok := r.busy[name]
	if ok {
		delete(r.busy, name)
		return info
	}
	info, ok = r.idle[name]
	if ok {
		delete(r.idle, name)
		return info
	}
	panic("marking non-existent runner done")
}

func (r *runnerState) addIdle(name string, info runnerInfo) {
	r.mu.Lock()
	r.idle[name] = info
	r.mu.Unlock()
}
