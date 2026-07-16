package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/actions/scaleset"
	"github.com/actions/scaleset/listener"
	"github.com/docker/docker/api/types/image"
	dockerclient "github.com/docker/docker/client"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
)

func init() {
	flags := cmd.Flags()
	flags.StringVar(&cfg.RegistrationURL, "url", "", "REQUIRED: URL where to register your scale set (e.g. https://github.com/org/repo)")
	flags.IntVar(&cfg.MaxRunners, "max-runners", 10, "Maximum number of runners")
	flags.IntVar(&cfg.MinRunners, "min-runners", 0, "Minimum number of runners")
	flags.StringVar(&cfg.ScaleSetName, "name", "", "REQUIRED: Name of your scale set")
	flags.StringSliceVar(&cfg.Labels, "labels", nil, "Labels for workflow targeting (comma-separated or repeated). Defaults to --name if not provided.")
	flags.StringVar(&cfg.RunnerGroup, "runner-group", scaleset.DefaultRunnerGroup, "Name of the runner group your scale set should belong to")
	flags.StringVar(&cfg.GitHubApp.ClientID, "app-client-id", "", "GitHub App client id")
	flags.Int64Var(&cfg.GitHubApp.InstallationID, "app-installation-id", 0, "GitHub App installation ID")
	flags.StringVar(&cfg.GitHubApp.PrivateKey, "app-private-key", "", "GitHub App private key")
	flags.StringVar(&cfg.GitHubAppPrivateKeyFile, "app-private-key-file", "", "Path to GitHub App private key PEM file")
	flags.StringVar(&cfg.Token, "token", "", "Personal access token (can be used in place of a GitHub App, although not recommended)")
	flags.StringVar(&cfg.LogLevel, "log-level", "info", "Logging level (debug, info, warn, error)")
	flags.StringVar(&cfg.LogFormat, "log-format", "text", "Logging format (text, json). If invalid value is provided, defaults to no logs.")
	flags.StringVar(&cfg.RunnerImage, "runner-image", "", "Docker image for GitHub Actions runner")
	flags.StringVar(&cfg.DindImage, "dind-image", "docker:dind", "Docker-in-Docker image for runner isolation")
	if err := cmd.MarkFlagRequired("url"); err != nil {
		panic(err)
	}
	if err := cmd.MarkFlagRequired("name"); err != nil {
		panic(err)
	}
}

func main() {
	if err := cmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, c Config) error {
	// Ensure that the config is valid
	if err := c.Validate(); err != nil {
		return fmt.Errorf("configuration validation failed: %w", err)
	}

	logger := c.Logger()

	// Create a new scaleset scalesetClient
	scalesetClient, err := c.ScalesetClient()
	if err != nil {
		return fmt.Errorf("failed to create scaleset client: %w", err)
	}

	// Get the runner group ID of the chosen runner group
	var runnerGroupID int
	switch c.RunnerGroup {
	case scaleset.DefaultRunnerGroup:
		runnerGroupID = 1
	default:
		runnerGroup, err := scalesetClient.GetRunnerGroupByName(ctx, c.RunnerGroup)
		if err != nil {
			return fmt.Errorf("failed to get runner group ID: %w", err)
		}
		runnerGroupID = runnerGroup.ID
	}

	// Reuse the runner scale set after restarts. Create it only on the first run.
	scaleSet, err := scalesetClient.GetRunnerScaleSet(ctx, runnerGroupID, c.ScaleSetName)
	if err != nil {
		return fmt.Errorf("failed to get runner scale set: %w", err)
	}
	if scaleSet == nil {
		scaleSet, err = scalesetClient.CreateRunnerScaleSet(ctx, &scaleset.RunnerScaleSet{
			Name:          c.ScaleSetName,
			RunnerGroupID: runnerGroupID,
			Labels:        c.BuildLabels(),
			RunnerSetting: scaleset.RunnerSetting{
				DisableUpdate: true,
			},
		})
		if err != nil {
			return fmt.Errorf("failed to create runner scale set: %w", err)
		}
		logger.Info(
			"Created runner scale set",
			slog.Int("scaleSetID", scaleSet.ID),
			slog.String("scaleSetName", scaleSet.Name),
		)
	} else {
		logger.Info(
			"Reusing existing runner scale set",
			slog.Int("scaleSetID", scaleSet.ID),
			slog.String("scaleSetName", scaleSet.Name),
		)
	}

	// Set the user agent for the scaleset client now that we have the scale set ID
	scalesetClient.SetSystemInfo(systemInfo(scaleSet.ID))

	dockerClient, err := dockerclient.NewClientWithOpts(dockerclient.FromEnv, dockerclient.WithAPIVersionNegotiation())
	if err != nil {
		return fmt.Errorf("failed to create docker client: %w", err)
	}

	logger.Info(
		"Pulling runner image",
		slog.String("image", c.RunnerImage),
	)

	runnerRegistryAuth, err := c.RegistryAuth(c.RunnerImage)
	if err != nil {
		return fmt.Errorf("failed to prepare runner image registry auth: %w", err)
	}
	// Pull the runner image
	pull, err := dockerClient.ImagePull(ctx, c.RunnerImage, image.PullOptions{
		RegistryAuth: runnerRegistryAuth,
	})
	if err != nil {
		return fmt.Errorf("failed to pull runner image: %w", err)
	}

	if _, err := io.ReadAll(pull); err != nil {
		return fmt.Errorf("failed to read image pull response: %w", err)
	}

	if err := pull.Close(); err != nil {
		return fmt.Errorf("failed to close image pull: %w", err)
	}

	logger.Info("Pulling dind image", slog.String("image", c.DindImage))
	dindRegistryAuth, err := c.RegistryAuth(c.DindImage)
	if err != nil {
		return fmt.Errorf("failed to prepare dind image registry auth: %w", err)
	}
	dindPull, err := dockerClient.ImagePull(ctx, c.DindImage, image.PullOptions{
		RegistryAuth: dindRegistryAuth,
	})
	if err != nil {
		return fmt.Errorf("failed to pull dind image: %w", err)
	}
	if _, err := io.ReadAll(dindPull); err != nil {
		return fmt.Errorf("failed to read dind image pull response: %w", err)
	}
	if err := dindPull.Close(); err != nil {
		return fmt.Errorf("failed to close dind image pull: %w", err)
	}

	// Get the name of the client which will be used as the owner
	hostname, err := os.Hostname()
	if err != nil {
		hostname = uuid.NewString()
		logger.Info("Failed to get hostname, fallback to uuid", "uuid", hostname, "error", err)
	}

	sessionClient, err := scalesetClient.MessageSessionClient(ctx, scaleSet.ID, hostname)
	if err != nil {
		return fmt.Errorf("failed to create message session client: %w", err)
	}
	defer sessionClient.Close(context.Background())

	logger.Info("Initializing listener")
	listener, err := listener.New(sessionClient, listener.Config{
		ScaleSetID: scaleSet.ID,
		MaxRunners: c.MaxRunners,
		Logger:     logger.WithGroup("listener"),
	})
	if err != nil {
		return fmt.Errorf("failed to create listener: %w", err)
	}

	resourcePrefix := c.ScaleSetName

	scaler := &Scaler{
		logger: logger.WithGroup("scaler"),
		runners: runnerState{
			idle: make(map[string]runnerInfo),
			busy: make(map[string]runnerInfo),
		},
		justStarted:    true,
		runnerImage:    c.RunnerImage,
		dindImage:      c.DindImage,
		scaleSetName:   c.ScaleSetName,
		resourcePrefix: normalizeResourcePrefix(resourcePrefix),
		minRunners:     c.MinRunners,
		maxRunners:     c.MaxRunners,
		dockerClient:   dockerClient,
		scalesetClient: scalesetClient,
		scaleSetID:     scaleSet.ID,
	}

	if err := scaler.cleanupStaleResources(ctx); err != nil {
		return fmt.Errorf("failed to clean up stale runner resources: %w", err)
	}

	defer scaler.shutdown(context.WithoutCancel(ctx))

	logger.Info("Starting listener")
	if err := listener.Run(ctx, scaler); !errors.Is(err, context.Canceled) {
		return fmt.Errorf("listener run failed: %w", err)
	}
	return nil
}

var cfg Config

var cmd = &cobra.Command{
	Use:   "dockerscaleset",
	Short: "Example CLI application scaling runners using Docker",
	Long: `This is an example CLI application that demonstrates how to scale
runners using Docker.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, cancel := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
		defer cancel()

		if err := cfg.Validate(); err != nil {
			return fmt.Errorf("invalid configuration: %w", err)
		}

		return run(ctx, cfg)
	},
}
