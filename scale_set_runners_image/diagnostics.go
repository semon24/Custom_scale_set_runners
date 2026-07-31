package main

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/pkg/stdcopy"
)

const diagnosticLogLimit = 16 * 1024

var githubDiagnosticEndpoints = []string{
	"https://github.com/",
	"https://api.github.com/",
}

type httpProbeResult struct {
	URL        string
	Reachable  bool
	StatusCode int
	RequestID  string
	Duration   time.Duration
	Error      string
}

func probeHTTP(ctx context.Context, client *http.Client, endpoint string) httpProbeResult {
	result := httpProbeResult{URL: endpoint}
	startedAt := time.Now()

	request, err := http.NewRequestWithContext(ctx, http.MethodHead, endpoint, nil)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	request.Header.Set("User-Agent", "dockerscaleset-watchdog/0.1")

	response, err := client.Do(request)
	result.Duration = time.Since(startedAt)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	defer response.Body.Close()

	result.Reachable = true
	result.StatusCode = response.StatusCode
	result.RequestID = response.Header.Get("X-GitHub-Request-Id")
	return result
}

func (a *Scaler) logRunnerDiagnostics(ctx context.Context, runnerName string, info runnerInfo) {
	a.logger.Warn(
		"Collecting stale runner diagnostics",
		slog.String("runner", runnerName),
		slog.String("runnerContainerID", info.runnerID),
		slog.String("dindContainerID", info.dindID),
		slog.String("network", info.networkName),
		slog.String("workspaceVolume", info.workspaceVol),
		slog.Time("runnerCreatedAt", info.createdAt),
	)

	a.logContainerDiagnostics(ctx, "runner", runnerName, info.runnerID)
	a.logContainerDiagnostics(ctx, "dind", runnerName, info.dindID)
	a.logRunnerNetworkProbe(ctx, runnerName, info.runnerID)
}

func (a *Scaler) logContainerDiagnostics(ctx context.Context, role, runnerName, containerID string) {
	if containerID == "" {
		a.logger.Warn("Container diagnostics skipped because the container ID is empty", slog.String("role", role), slog.String("runner", runnerName))
		return
	}

	inspect, err := a.dockerClient.ContainerInspect(ctx, containerID)
	if err != nil {
		a.logger.Warn(
			"Failed to inspect runner resource",
			slog.String("role", role),
			slog.String("runner", runnerName),
			slog.String("containerID", containerID),
			slog.String("error", err.Error()),
		)
	} else if inspect.State == nil {
		a.logger.Warn("Runner resource has no container state", slog.String("role", role), slog.String("runner", runnerName), slog.String("containerID", containerID))
	} else {
		health := "not-configured"
		if inspect.State.Health != nil {
			health = inspect.State.Health.Status
		}
		a.logger.Warn(
			"Runner resource state",
			slog.String("role", role),
			slog.String("runner", runnerName),
			slog.String("containerID", containerID),
			slog.String("status", inspect.State.Status),
			slog.Bool("running", inspect.State.Running),
			slog.Bool("restarting", inspect.State.Restarting),
			slog.Int("exitCode", inspect.State.ExitCode),
			slog.String("health", health),
			slog.String("stateError", inspect.State.Error),
			slog.Time("startedAt", parseDockerTime(inspect.State.StartedAt)),
		)
	}

	logs, err := a.containerLogTail(ctx, containerID)
	if err != nil {
		a.logger.Warn(
			"Failed to read runner resource logs",
			slog.String("role", role),
			slog.String("runner", runnerName),
			slog.String("containerID", containerID),
			slog.String("error", err.Error()),
		)
		return
	}
	a.logger.Warn(
		"Recent runner resource logs",
		slog.String("role", role),
		slog.String("runner", runnerName),
		slog.String("containerID", containerID),
		slog.String("logs", logs),
	)
}

func (a *Scaler) containerLogTail(ctx context.Context, containerID string) (string, error) {
	reader, err := a.dockerClient.ContainerLogs(ctx, containerID, container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Timestamps: true,
		Tail:       "200",
	})
	if err != nil {
		return "", err
	}
	defer reader.Close()

	raw, err := io.ReadAll(reader)
	if err != nil {
		return "", err
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if _, err := stdcopy.StdCopy(&stdout, &stderr, bytes.NewReader(raw)); err != nil {
		stdout.Reset()
		stdout.Write(raw)
	}
	if stderr.Len() > 0 {
		stdout.WriteString("\n[stderr]\n")
		stdout.Write(stderr.Bytes())
	}
	return truncateDiagnosticText(stdout.String()), nil
}

func (a *Scaler) logRunnerNetworkProbe(ctx context.Context, runnerName, containerID string) {
	if containerID == "" {
		return
	}

	command := strings.Join([]string{
		"set +e",
		"probe_status=0",
		"echo '[dns]'",
		"if command -v getent >/dev/null 2>&1; then getent hosts github.com api.github.com || probe_status=1; else echo 'getent is unavailable'; fi",
		"echo '[https]'",
		"if command -v curl >/dev/null 2>&1; then",
		"  curl -sS -o /dev/null -w 'github.com status=%{http_code} remote=%{remote_ip} time=%{time_total}\\n' --connect-timeout 5 --max-time 10 https://github.com/ || probe_status=1",
		"  curl -sS -o /dev/null -w 'api.github.com status=%{http_code} remote=%{remote_ip} time=%{time_total}\\n' --connect-timeout 5 --max-time 10 https://api.github.com/ || probe_status=1",
		"elif command -v wget >/dev/null 2>&1; then",
		"  wget --spider -T 10 https://github.com/ || probe_status=1",
		"  wget --spider -T 10 https://api.github.com/ || probe_status=1",
		"else echo 'curl and wget are unavailable'; probe_status=127; fi",
		"exit \"$probe_status\"",
	}, "\n")

	execCreate, err := a.dockerClient.ContainerExecCreate(ctx, containerID, container.ExecOptions{
		AttachStdout: true,
		AttachStderr: true,
		Cmd:          []string{"sh", "-lc", command},
	})
	if err != nil {
		a.logger.Warn("Failed to create in-runner network probe", slog.String("runner", runnerName), slog.String("error", err.Error()))
		return
	}

	attached, err := a.dockerClient.ContainerExecAttach(ctx, execCreate.ID, container.ExecAttachOptions{})
	if err != nil {
		a.logger.Warn("Failed to start in-runner network probe", slog.String("runner", runnerName), slog.String("error", err.Error()))
		return
	}
	defer attached.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	_, copyErr := stdcopy.StdCopy(&stdout, &stderr, attached.Reader)
	inspect, inspectErr := a.dockerClient.ContainerExecInspect(ctx, execCreate.ID)
	if copyErr != nil {
		a.logger.Warn("Failed to read in-runner network probe", slog.String("runner", runnerName), slog.String("error", copyErr.Error()))
	}
	if stderr.Len() > 0 {
		stdout.WriteString("\n[stderr]\n")
		stdout.Write(stderr.Bytes())
	}
	attrs := []any{
		slog.String("runner", runnerName),
		slog.String("output", truncateDiagnosticText(stdout.String())),
	}
	if inspectErr != nil {
		attrs = append(attrs, slog.String("inspectError", inspectErr.Error()))
	} else {
		attrs = append(attrs, slog.Int("exitCode", inspect.ExitCode))
	}
	a.logger.Warn("In-runner GitHub network probe completed", attrs...)
}

func (a *Scaler) logGitHubConnectivity(ctx context.Context) {
	for _, host := range []string{"github.com", "api.github.com"} {
		lookupCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		startedAt := time.Now()
		addresses, err := net.DefaultResolver.LookupHost(lookupCtx, host)
		cancel()
		if err != nil {
			a.logger.Warn("GitHub DNS probe failed", slog.String("host", host), slog.Duration("duration", time.Since(startedAt)), slog.String("error", err.Error()))
			continue
		}
		a.logger.Info("GitHub DNS probe succeeded", slog.String("host", host), slog.Any("addresses", addresses), slog.Duration("duration", time.Since(startedAt)))
	}

	client := &http.Client{Timeout: 10 * time.Second}
	for _, endpoint := range githubDiagnosticEndpoints {
		result := probeHTTP(ctx, client, endpoint)
		if !result.Reachable {
			a.logger.Warn("GitHub HTTPS probe failed", slog.String("url", result.URL), slog.Duration("duration", result.Duration), slog.String("error", result.Error))
			continue
		}
		a.logger.Info(
			"GitHub HTTPS probe succeeded",
			slog.String("url", result.URL),
			slog.Int("statusCode", result.StatusCode),
			slog.String("githubRequestId", result.RequestID),
			slog.Duration("duration", result.Duration),
		)
	}
}

func truncateDiagnosticText(value string) string {
	value = strings.TrimSpace(value)
	if len(value) <= diagnosticLogLimit {
		return value
	}
	return "[truncated]\n" + value[len(value)-diagnosticLogLimit:]
}

func parseDockerTime(value string) time.Time {
	parsed, _ := time.Parse(time.RFC3339Nano, value)
	return parsed
}
