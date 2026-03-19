package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/actions/scaleset"
	"github.com/actions/scaleset/listener"
	"github.com/docker/docker/api/types/container"
	dockerclient "github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/google/uuid"
)

const runnerUID = "1001"

type Scaler struct {
	countRunnersUp    int
	justStarted       bool
	runners           runnerState
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
			_, err := a.startRunner(ctx)
			if err != nil {
				return a.runners.count(), fmt.Errorf("failed to start runner: %w", err)
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

func (a *Scaler) pruneStaleIdleRunners(ctx context.Context) {
	a.runners.mu.Lock()
	idleSnapshot := make(map[string]runnerInfo, len(a.runners.idle))
	for name, info := range a.runners.idle {
		idleSnapshot[name] = info
	}
	a.runners.mu.Unlock()

	for runnerName, info := range idleSnapshot {
		dindInspect, err := a.dockerClient.ContainerInspect(ctx, info.dindID)
		if err != nil || dindInspect.State == nil || !dindInspect.State.Running {
			a.logger.Warn("Pruning idle runner because dind is missing or not running", slog.String("runner", runnerName), slog.String("dindID", info.dindID))
			a.cleanupIdleRunnerEntry(context.WithoutCancel(ctx), runnerName, info)
			continue
		}

		state, health, err := a.inspectRunnerStateInDind(ctx, info.dindID, runnerName)
		if err != nil {
			a.logger.Warn("Pruning idle runner because inner runner is not inspectable", slog.String("runner", runnerName), slog.String("dindID", info.dindID), slog.String("error", err.Error()))
			a.cleanupIdleRunnerEntry(context.WithoutCancel(ctx), runnerName, info)
			continue
		}

		if state != "running" || health == "unhealthy" {
			a.logger.Warn("Pruning idle runner because inner runner is not healthy", slog.String("runner", runnerName), slog.String("state", state), slog.String("health", health))
			a.cleanupIdleRunnerEntry(context.WithoutCancel(ctx), runnerName, info)
		}
	}
}

func (a *Scaler) cleanupIdleRunnerEntry(ctx context.Context, runnerName string, info runnerInfo) {
	a.runners.mu.Lock()
	if cur, ok := a.runners.idle[runnerName]; ok && cur == info {
		delete(a.runners.idle, runnerName)
	}
	a.runners.mu.Unlock()

	a.dumpRunnerLogsFromDind(ctx, info.dindID, runnerName)

	if err := a.dockerClient.ContainerRemove(ctx, info.dindID, container.RemoveOptions{Force: true, RemoveVolumes: true}); err != nil {
		a.logger.Warn("Failed to remove stale dind", slog.String("runner", runnerName), slog.String("dindID", info.dindID), slog.String("error", err.Error()))
	}

	if info.workspaceVol != "" {
		if err := a.dockerClient.VolumeRemove(ctx, info.workspaceVol, true); err != nil {
			a.logger.Warn("Failed to remove stale workspace volume", slog.String("runner", runnerName), slog.String("volume", info.workspaceVol), slog.String("error", err.Error()))
		}
	}
}

func (a *Scaler) inspectRunnerStateInDind(ctx context.Context, dindContainerID, runnerName string) (string, string, error) {
	execResp, err := a.dockerClient.ContainerExecCreate(ctx, dindContainerID, container.ExecOptions{
		Cmd:          []string{"sh", "-lc", fmt.Sprintf("docker inspect --format '{{.State.Status}}|{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' %s", runnerName)},
		AttachStdout: true,
		AttachStderr: true,
	})
	if err != nil {
		return "", "", err
	}

	attach, err := a.dockerClient.ContainerExecAttach(ctx, execResp.ID, container.ExecAttachOptions{})
	if err != nil {
		return "", "", err
	}
	defer attach.Close()

	if err := a.dockerClient.ContainerExecStart(ctx, execResp.ID, container.ExecStartOptions{}); err != nil {
		return "", "", err
	}

	outBytes, err := io.ReadAll(attach.Reader)
	if err != nil {
		return "", "", err
	}
	out := decodeDockerAttachOutput(outBytes)

	execInspect, err := a.dockerClient.ContainerExecInspect(ctx, execResp.ID)
	if err != nil {
		return "", "", err
	}
	if execInspect.ExitCode != 0 {
		return "", "", fmt.Errorf("docker inspect exit code %d", execInspect.ExitCode)
	}

	parts := strings.Split(strings.TrimSpace(out), "|")
	if len(parts) != 2 {
		return "", "", fmt.Errorf("unexpected inspect output: %q", strings.TrimSpace(out))
	}

	return parts[0], parts[1], nil
}

func (a *Scaler) HandleJobStarted(ctx context.Context, jobInfo *scaleset.JobStarted) error {
	a.logger.Info(
		"Runner picked up job",
		slog.String("runner", jobInfo.RunnerName),
		slog.String("jobId", jobInfo.JobID),
		slog.Int64("runnerRequestId", jobInfo.RunnerRequestID),
	)
	a.runners.markBusy(jobInfo.RunnerName)
	return nil
}

func (a *Scaler) HandleJobCompleted(ctx context.Context, jobInfo *scaleset.JobCompleted) error {
	a.logger.Info("Job completed", slog.String("runner", jobInfo.RunnerName), slog.String("jobId", jobInfo.JobID), slog.Int64("runnerRequestId", jobInfo.RunnerRequestID))

	info := a.runners.markDone(jobInfo.RunnerName)
	if err := a.dockerClient.ContainerRemove(ctx, info.dindID, container.RemoveOptions{Force: true, RemoveVolumes: true}); err != nil {
		return fmt.Errorf("failed to remove dind container: %w", err)
	}

	if info.workspaceVol != "" {
		if err := a.dockerClient.VolumeRemove(ctx, info.workspaceVol, true); err != nil {
			a.logger.Error("Failed to remove workspace volume", slog.String("volume", info.workspaceVol), slog.String("error", err.Error()))
		} else {
			a.logger.Info("Workspace volume removed", slog.String("runner", jobInfo.RunnerName), slog.String("volume", info.workspaceVol))
		}
	}
	return nil
}

func (a *Scaler) startRunner(ctx context.Context) (string, error) {
	name := fmt.Sprintf("runner-%s", uuid.NewString()[:8])
	dindName := fmt.Sprintf("dind-%s", name)

	a.logger.Info("Starting runner with image", "image", a.runnerImage)

	dindC, err := a.dockerClient.ContainerCreate(
		ctx,
		&container.Config{
			Image:      a.dindImage,
			Env: []string{
				"DOCKER_TLS_CERTDIR=",
				"DOCKERD_ARGS=--host=unix:///var/run/docker.sock --tls=false --insecure-registry=80.87.107.65:5000",
			},
			Entrypoint: []string{"sh", "-lc"},
			Cmd: []string{
				fmt.Sprintf("mkdir -p /home/runner/_work && chown -R %s:%s /home/runner/_work && if [ -f /usr/local/bin/start-dind-with-preload.sh ]; then sh /usr/local/bin/start-dind-with-preload.sh || { echo 'preload startup failed, falling back to plain dockerd'; exec dockerd-entrypoint.sh --host=unix:///var/run/docker.sock --tls=false --insecure-registry=80.87.107.65:5000; }; else exec dockerd-entrypoint.sh --host=unix:///var/run/docker.sock --tls=false --insecure-registry=80.87.107.65:5000; fi", runnerUID, runnerUID),
			},
			Healthcheck: &container.HealthConfig{
				Test:        []string{"CMD-SHELL", "docker info >/dev/null 2>&1"},
				Interval:    3 * time.Second,
				Timeout:     2 * time.Second,
				StartPeriod: 5 * time.Second,
				Retries:     20,
			},
		},
		&container.HostConfig{
			Privileged:  true,
			NetworkMode: container.NetworkMode(a.sharedNetworkName),
		},
		nil, nil,
		dindName,
	)
	if err != nil {
		return "", fmt.Errorf("failed to create dind container: %w", err)
	}

	if err := a.dockerClient.ContainerStart(ctx, dindC.ID, container.StartOptions{}); err != nil {
		return "", fmt.Errorf("failed to start dind container: %w", err)
	}

	// Run dind wait, image wait, and JIT config in parallel to save time.
	type jitResult struct {
		jit *scaleset.RunnerScaleSetJitRunnerConfig
		err error
	}
	jitCh := make(chan jitResult, 1)
	go func() {
		jit, err := a.scalesetClient.GenerateJitRunnerConfig(
			ctx,
			&scaleset.RunnerScaleSetJitRunnerSetting{Name: name},
			a.scaleSetID,
		)
		jitCh <- jitResult{jit: jit, err: err}
	}()

	imgCh := make(chan struct{})
	go func() {
		a.waitForRunnerImageInDind(ctx, dindC.ID, 60*time.Second)
		close(imgCh)
	}()

	if err := a.waitForDindReady(ctx, dindC.ID, 90*time.Second); err != nil {
		a.dumpDindLogs(context.WithoutCancel(ctx), dindC.ID, name)
		_ = a.dockerClient.ContainerRemove(context.WithoutCancel(ctx), dindC.ID, container.RemoveOptions{Force: true, RemoveVolumes: true})
		return "", fmt.Errorf("dind did not become healthy: %w", err)
	}

	res := <-jitCh
	if res.err != nil {
		_ = a.dockerClient.ContainerRemove(context.WithoutCancel(ctx), dindC.ID, container.RemoveOptions{Force: true, RemoveVolumes: true})
		return "", fmt.Errorf("failed to generate JIT config: %w", res.err)
	}
	if res.jit == nil {
		_ = a.dockerClient.ContainerRemove(context.WithoutCancel(ctx), dindC.ID, container.RemoveOptions{Force: true, RemoveVolumes: true})
		return "", fmt.Errorf("generated JIT config is nil")
	}
	<-imgCh // wait for runner image in dind (preload) before docker run
	encodedJITConfig := strings.TrimSpace(res.jit.EncodedJITConfig)
	if encodedJITConfig == "" {
		return "", fmt.Errorf("generated JIT config is empty")
	}
	if _, err := base64.StdEncoding.DecodeString(encodedJITConfig); err != nil {
		return "", fmt.Errorf("generated JIT config is not valid base64: %w", err)
	}

	if err := a.startRunnerInsideDind(ctx, dindC.ID, name, encodedJITConfig); err != nil {
		a.dumpRunnerLogsFromDind(context.WithoutCancel(ctx), dindC.ID, name)
		_ = a.dockerClient.ContainerRemove(context.WithoutCancel(ctx), dindC.ID, container.RemoveOptions{Force: true, RemoveVolumes: true})
		return "", fmt.Errorf("failed to start runner inside dind: %w", err)
	}

	a.runners.addIdle(name, runnerInfo{
		runnerID:     name,
		dindID:       dindC.ID,
		workspaceVol: "",
	})
	return name, nil
}

func (a *Scaler) startRunnerInsideDind(ctx context.Context, dindContainerID, runnerName, encodedJITConfig string) error {
	cleanupExec, err := a.dockerClient.ContainerExecCreate(ctx, dindContainerID, container.ExecOptions{
		Cmd:          []string{"sh", "-lc", fmt.Sprintf("docker rm -f %s >/dev/null 2>&1 || true", runnerName)},
		AttachStdout: false,
		AttachStderr: false,
	})
	if err != nil {
		return fmt.Errorf("failed to create stale runner cleanup exec in dind: %w", err)
	}
	if err := a.dockerClient.ContainerExecStart(ctx, cleanupExec.ID, container.ExecStartOptions{}); err != nil {
		return fmt.Errorf("failed to start stale runner cleanup exec in dind: %w", err)
	}

	args := []string{
		"docker", "run", "-d",
		"--name", runnerName,
		"--entrypoint", "/home/runner/run.sh",
		"-e", fmt.Sprintf("ACTIONS_RUNNER_INPUT_JITCONFIG=%s", encodedJITConfig),
		"-e", "DOCKER_HOST=unix:///var/run/docker.sock",
		"-v", "/var/run/docker.sock:/var/run/docker.sock",
		"-v", "/home/runner/_work:/home/runner/_work",
		a.runnerImage,
	}

	execResp, err := a.dockerClient.ContainerExecCreate(ctx, dindContainerID, container.ExecOptions{
		Cmd:          args,
		AttachStdout: true,
		AttachStderr: true,
	})
	if err != nil {
		return fmt.Errorf("failed to create docker run exec in dind: %w", err)
	}

	attach, err := a.dockerClient.ContainerExecAttach(ctx, execResp.ID, container.ExecAttachOptions{})
	if err != nil {
		return fmt.Errorf("failed to attach docker run exec in dind: %w", err)
	}
	defer attach.Close()

	dockerRunStart := time.Now()
	if err := a.dockerClient.ContainerExecStart(ctx, execResp.ID, container.ExecStartOptions{}); err != nil {
		return fmt.Errorf("failed to start docker run exec in dind: %w", err)
	}

	outBytes, readErr := io.ReadAll(attach.Reader)
	out := strings.TrimSpace(decodeDockerAttachOutput(outBytes))
	if readErr != nil {
		return fmt.Errorf("failed to read docker run exec output in dind: %w", readErr)
	}

	for {
		inspect, err := a.dockerClient.ContainerExecInspect(ctx, execResp.ID)
		if err != nil {
			return fmt.Errorf("failed to inspect docker run exec in dind: %w", err)
		}
		if inspect.Running {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		if inspect.ExitCode != 0 {
			if out != "" {
				return fmt.Errorf("docker run exec in dind exited with code %d: %s", inspect.ExitCode, out)
			}
			return fmt.Errorf("docker run exec in dind exited with code %d", inspect.ExitCode)
		}
		break
	}

	a.logger.Info("Runner started inside dind", slog.String("runner", runnerName), slog.String("dindContainerID", dindContainerID), slog.Duration("elapsed", time.Since(dockerRunStart)))
	return nil
}

func (a *Scaler) dumpRunnerLogsFromDind(ctx context.Context, dindContainerID, runnerName string) {
	execResp, err := a.dockerClient.ContainerExecCreate(ctx, dindContainerID, container.ExecOptions{
		Cmd:          []string{"sh", "-lc", fmt.Sprintf("docker logs --tail 200 %s 2>&1", runnerName)},
		AttachStdout: true,
		AttachStderr: true,
	})
	if err != nil {
		a.logger.Warn("Failed to create runner logs exec in dind", slog.String("runner", runnerName), slog.String("dindID", dindContainerID), slog.String("error", err.Error()))
		return
	}

	attach, err := a.dockerClient.ContainerExecAttach(ctx, execResp.ID, container.ExecAttachOptions{})
	if err != nil {
		a.logger.Warn("Failed to attach runner logs exec in dind", slog.String("runner", runnerName), slog.String("dindID", dindContainerID), slog.String("error", err.Error()))
		return
	}
	defer attach.Close()

	if err := a.dockerClient.ContainerExecStart(ctx, execResp.ID, container.ExecStartOptions{}); err != nil {
		a.logger.Warn("Failed to start runner logs exec in dind", slog.String("runner", runnerName), slog.String("dindID", dindContainerID), slog.String("error", err.Error()))
		return
	}

	out, readErr := io.ReadAll(attach.Reader)
	if readErr != nil {
		a.logger.Warn("Failed to read runner logs from dind", slog.String("runner", runnerName), slog.String("dindID", dindContainerID), slog.String("error", readErr.Error()))
		return
	}

	execInspect, err := a.dockerClient.ContainerExecInspect(ctx, execResp.ID)
	if err != nil {
		a.logger.Warn("Failed to inspect runner logs exec in dind", slog.String("runner", runnerName), slog.String("dindID", dindContainerID), slog.String("error", err.Error()))
		return
	}

	logs := strings.TrimSpace(decodeDockerAttachOutput(out))
	if logs == "" {
		return
	}

	if execInspect.ExitCode != 0 {
		a.logger.Warn("Collected runner logs from dind with non-zero exit code", slog.String("runner", runnerName), slog.String("dindID", dindContainerID), slog.Int("exitCode", execInspect.ExitCode), slog.String("logs", logs))
		return
	}

	a.logger.Info("Collected runner logs from dind", slog.String("runner", runnerName), slog.String("dindID", dindContainerID), slog.String("logs", logs))
}

func (a *Scaler) dumpDindLogs(ctx context.Context, dindContainerID, runnerName string) {
	reader, err := a.dockerClient.ContainerLogs(ctx, dindContainerID, container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Tail:       "200",
	})
	if err != nil {
		a.logger.Warn("Failed to get dind logs", slog.String("runner", runnerName), slog.String("dindID", dindContainerID), slog.String("error", err.Error()))
		return
	}
	defer reader.Close()

	raw, err := io.ReadAll(reader)
	if err != nil {
		a.logger.Warn("Failed to read dind logs", slog.String("runner", runnerName), slog.String("dindID", dindContainerID), slog.String("error", err.Error()))
		return
	}

	logs := strings.TrimSpace(decodeDockerAttachOutput(raw))
	if logs == "" {
		return
	}

	a.logger.Warn("Collected dind logs", slog.String("runner", runnerName), slog.String("dindID", dindContainerID), slog.String("logs", logs))
}

	func (a *Scaler) waitForRunnerHealthyInDind(ctx context.Context, dindContainerID, runnerName string, timeout time.Duration) error {
		deadline := time.Now().Add(timeout)
		var lastErr error

		for time.Now().Before(deadline) {
			execResp, err := a.dockerClient.ContainerExecCreate(ctx, dindContainerID, container.ExecOptions{
				Cmd:          []string{"sh", "-lc", fmt.Sprintf("docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}|{{.State.Status}}|{{.State.Running}}|{{.State.ExitCode}}' %s", runnerName)},
				AttachStdout: true,
				AttachStderr: true,
			})
			if err != nil {
				lastErr = err
				time.Sleep(1 * time.Second)
				continue
			}

			attach, err := a.dockerClient.ContainerExecAttach(ctx, execResp.ID, container.ExecAttachOptions{})
			if err != nil {
				lastErr = err
				time.Sleep(1 * time.Second)
				continue
			}

			if err := a.dockerClient.ContainerExecStart(ctx, execResp.ID, container.ExecStartOptions{}); err != nil {
				attach.Close()
				lastErr = err
				time.Sleep(1 * time.Second)
				continue
			}

			resultBytes, readErr := io.ReadAll(attach.Reader)
			attach.Close()
			if readErr != nil {
				lastErr = readErr
				time.Sleep(1 * time.Second)
				continue
			}
			result := decodeDockerAttachOutput(resultBytes)

			execInspect, err := a.dockerClient.ContainerExecInspect(ctx, execResp.ID)
			if err != nil {
				lastErr = err
				time.Sleep(1 * time.Second)
				continue
			}
			if execInspect.Running {
				time.Sleep(500 * time.Millisecond)
				continue
			}
			if execInspect.ExitCode != 0 {
				lastErr = fmt.Errorf("docker inspect in dind exited with code %d", execInspect.ExitCode)
				time.Sleep(1 * time.Second)
				continue
			}

			parts := strings.Split(strings.TrimSpace(result), "|")
			if len(parts) != 4 {
				lastErr = fmt.Errorf("unexpected runner inspect output: %q", strings.TrimSpace(result))
				time.Sleep(1 * time.Second)
				continue
			}

			healthStatus := parts[0]
			runnerStatus := parts[1]
			runningFlag := parts[2]
			exitCode := parts[3]

			if healthStatus == "healthy" {
				a.logger.Info("Runner is healthy inside dind", slog.String("runner", runnerName), slog.String("dindContainerID", dindContainerID), slog.String("health", healthStatus))
				return nil
			}

			lastErr = fmt.Errorf("runner not healthy yet (health=%s status=%s running=%s exitCode=%s)", healthStatus, runnerStatus, runningFlag, exitCode)
			time.Sleep(1 * time.Second)
		}

		if lastErr == nil {
			lastErr = fmt.Errorf("timeout exceeded")
		}
		return lastErr
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

func (a *Scaler) waitForRunnerImageInDind(ctx context.Context, dindContainerID string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return
		}
		execResp, err := a.dockerClient.ContainerExecCreate(ctx, dindContainerID, container.ExecOptions{
			Cmd:          []string{"sh", "-lc", fmt.Sprintf("docker image inspect %q >/dev/null 2>&1", a.runnerImage)},
			AttachStdout: false,
			AttachStderr: false,
		})
		if err != nil {
			time.Sleep(2 * time.Second)
			continue
		}
		if err := a.dockerClient.ContainerExecStart(ctx, execResp.ID, container.ExecStartOptions{}); err != nil {
			time.Sleep(2 * time.Second)
			continue
		}
		for {
			inspect, err := a.dockerClient.ContainerExecInspect(ctx, execResp.ID)
			if err != nil || inspect.Running {
				time.Sleep(200 * time.Millisecond)
				continue
			}
			if inspect.ExitCode == 0 {
				a.logger.Info("Runner image ready in dind", slog.String("containerID", dindContainerID), slog.String("image", a.runnerImage))
				return
			}
			break
		}
		time.Sleep(2 * time.Second)
	}
	a.logger.Info("Runner image not found in dind within timeout, proceeding (docker run may pull)", slog.String("containerID", dindContainerID), slog.String("image", a.runnerImage), slog.Duration("timeout", timeout))
}

func decodeDockerAttachOutput(raw []byte) string {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	if _, err := stdcopy.StdCopy(&stdout, &stderr, bytes.NewReader(raw)); err == nil {
		return stdout.String() + stderr.String()
	}

	return string(raw)
}

func (a *Scaler) shutdown(ctx context.Context) {
	a.logger.Info("Shutting down runners")
	a.runners.mu.Lock()
	defer a.runners.mu.Unlock()

	removeRunner := func(name string, info runnerInfo) {
		a.logger.Info("Removing runner", slog.String("name", name), slog.String("runnerID", info.runnerID))
		if err := a.dockerClient.ContainerRemove(ctx, info.dindID, container.RemoveOptions{Force: true, RemoveVolumes: true}); err != nil {
			a.logger.Error("Failed to remove dind container", slog.String("name", name), slog.String("error", err.Error()))
		}
		if info.workspaceVol != "" {
			if err := a.dockerClient.VolumeRemove(ctx, info.workspaceVol, true); err != nil {
				a.logger.Error("Failed to remove workspace volume", slog.String("name", name), slog.String("error", err.Error()))
			} else {
				a.logger.Info("Workspace volume removed", slog.String("runner", name), slog.String("volume", info.workspaceVol))
			}
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
