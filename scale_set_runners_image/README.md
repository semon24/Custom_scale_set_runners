# Docker Runner Scale Set Example

This example showcases a Docker implementation of GitHub Actions runner scale sets, using the `github.com/actions/scaleset` client to provision ephemeral GitHub Actions runners as Docker containers.

The goal of this example is to show how simple and powerful it is when you only need to focus on the core logic of scaling runners up and down, while the client handles all the API interactions.

> [!WARNING]
> This is a simplified example meant for demonstration and learning purposes. It is not intended for production use.

> [!NOTE]
> When exiting normally all runners and the scale set itself are cleaned up automatically.

## Getting started

You can install the example with:

```bash
go install github.com/actions/scaleset/examples/dockerscaleset@latest
```

If this fails you should also try running the command with

```bash
GONOSUMDB=github.com/actions/scaleset GOPRIVATE=github.com/actions/scaleset go
install github.com/actions/scaleset/examples/dockerscaleset@latest
```

You'll then need:

- Docker installed and running on your machine.
- A URL for the target repository, organization, or enterprise where you want to register your scale set.
- [Credentials that have access to the above target](https://docs.github.com/en/actions/tutorials/use-actions-runner-controller/authenticate-to-the-api): you can use either a GitHub App (recommended) or a Personal Access Token (PAT).
- A name for your scale set (this must be unique within the runner group the scale set is created in).

---

## Flags

| Flag | Required | Description |
|------|----------|-------------|
| `--url` | Yes | Registration target (org, repo, or enterprise URL, e.g. `https://github.com/org/repo`). |
| `--name` | Yes | Runner scale set name (must be unique within the runner group). |
| `--labels` | No | Labels for workflow targeting (comma-separated or repeated). Defaults to `--name` if not provided. |
| `--max-runners` | No | Upper bound of concurrently provisioned runners (default 10). |
| `--min-runners` | No | Lower bound to maintain (default 0). |
| `--runner-group` | No | Runner group name (default `default`). |
| `--app-client-id` | Cond.* | GitHub App Client (App) ID. |
| `--app-installation-id` | Cond.* | GitHub App Installation ID. |
| `--app-private-key` | Cond.* | GitHub App private key PEM contents. |
| `--token` | Cond.* | Personal Access Token (alternative to App). |
| `--log-level` | No | `debug`, `info`, `warn`, `error` (default `info`). |
| `--log-format` | No | `text`, `json`, or `none` (any invalid → no logs). |
| `--runner-image` | No | Override container image (defaults to latest official). |
| `--job-start-timeout` | No | Replace one oldest idle runner when an assigned job has not started within this duration (default `5m`). |

*Provide either App credentials (all three) OR a PAT.*

## Private registry auth

If `--runner-image` points to a private registry, pass Docker registry credentials via environment variables:

```bash
REGISTRY_USER=your_login
REGISTRY_PASSWORD=your_password
```

For Docker Compose, place them in `.env` and expose them through `environment:` for the scale set container.

## Assigned-job watchdog

The scaler starts a watchdog only when GitHub reports more assigned jobs than busy runners. If no job starts before `--job-start-timeout`, the controller collects diagnostics and safely retires one oldest idle runner: it first deregisters that runner from GitHub, allows a short race window for `JobStarted`, and then sends only graceful `SIGTERM`. It never escalates to `SIGKILL`. The old container is removed only after Docker confirms that it exited. If it accepts a job during the race window, it is moved to `busy` and allowed to finish. When the old deregistered container does not exit within two minutes, a replacement may be started while shutdown supervision continues; the number of GitHub-registered runners still stays within `--max-runners`.

The watchdog does not run a periodic restart when no jobs are queued. Empty GitHub long-poll responses are treated as "no new information" and do not arm or disarm it. After a recovery, another replacement requires fresh authoritative positive statistics followed by another complete timeout, so one stale signal cannot create an endless restart loop.

Timeout diagnostics include runner and DinD state, their recent logs, DNS and HTTPS probes for `github.com` and `api.github.com` from the controller, and a DNS/HTTPS probe executed inside the runner container. ICMP ping is not used because GitHub or an intermediate firewall can block it while HTTPS remains available.
