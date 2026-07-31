package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/actions/scaleset"
	"github.com/actions/scaleset/listener"
	"github.com/google/uuid"
)

// scaleSetMessageClient is deliberately small so the polling semantics can be
// tested independently from the GitHub client. A nil long-poll response means
// "no new information", not an authoritative desired count of zero.
type scaleSetMessageClient interface {
	Session() scaleset.RunnerScaleSetSession
	GetMessage(ctx context.Context, lastMessageID, maxCapacity int) (*scaleset.RunnerScaleSetMessage, error)
	DeleteMessage(ctx context.Context, messageID int) error
}

func runScaleSetListener(ctx context.Context, client scaleSetMessageClient, maxRunners int, scaler listener.Scaler, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}

	session := client.Session()
	if session.SessionID == uuid.Nil {
		return fmt.Errorf("initial session is nil")
	}
	if session.Statistics == nil {
		return fmt.Errorf("session statistics is nil")
	}

	logger.Info("Handling initial session statistics", slog.Int("totalAssignedJobs", session.Statistics.TotalAssignedJobs))
	if _, err := scaler.HandleDesiredRunnerCount(ctx, session.Statistics.TotalAssignedJobs); err != nil {
		return fmt.Errorf("handling initial statistics failed: %w", err)
	}

	lastMessageID := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		logger.Debug("Getting next scale-set message", slog.Int("lastMessageID", lastMessageID))
		message, err := client.GetMessage(ctx, lastMessageID, maxRunners)
		if err != nil {
			return fmt.Errorf("failed to get message: %w", err)
		}
		if message == nil {
			// GitHub long polling uses nil for an empty poll. Preserve the last
			// authoritative statistics so a queued job cannot be accidentally
			// disarmed by the absence of a new message.
			continue
		}

		lastMessageID = message.MessageID
		handlerCtx := context.WithoutCancel(ctx)
		if err := client.DeleteMessage(handlerCtx, message.MessageID); err != nil {
			return fmt.Errorf("failed to delete message %d: %w", message.MessageID, err)
		}
		if message.Statistics == nil {
			return fmt.Errorf("message %d statistics is nil", message.MessageID)
		}

		for _, job := range message.JobStartedMessages {
			if err := scaler.HandleJobStarted(handlerCtx, job); err != nil {
				return fmt.Errorf("failed to handle job started: %w", err)
			}
		}
		for _, job := range message.JobCompletedMessages {
			if err := scaler.HandleJobCompleted(handlerCtx, job); err != nil {
				return fmt.Errorf("failed to handle job completed: %w", err)
			}
		}
		if _, err := scaler.HandleDesiredRunnerCount(handlerCtx, message.Statistics.TotalAssignedJobs); err != nil {
			return fmt.Errorf("failed to handle desired runner count: %w", err)
		}
	}
}
