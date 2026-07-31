package main

import (
	"context"
	"testing"

	"github.com/actions/scaleset"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestRunScaleSetListenerIgnoresEmptyPollZero(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	client := &fakeListenerClient{
		session: scaleset.RunnerScaleSetSession{
			SessionID:  uuid.New(),
			Statistics: &scaleset.RunnerScaleSetStatistic{TotalAssignedJobs: 1},
		},
		responses: []listenerResponse{{message: nil}, {cancel: cancel}},
	}
	scaler := &recordingScaler{}

	err := runScaleSetListener(ctx, client, 4, scaler, nil)

	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, []int{1}, scaler.desiredCounts)
}

func TestRunScaleSetListenerForwardsAuthoritativeZeroStatistics(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	client := &fakeListenerClient{
		session: scaleset.RunnerScaleSetSession{
			SessionID:  uuid.New(),
			Statistics: &scaleset.RunnerScaleSetStatistic{TotalAssignedJobs: 1},
		},
		responses: []listenerResponse{
			{message: &scaleset.RunnerScaleSetMessage{MessageID: 7, Statistics: &scaleset.RunnerScaleSetStatistic{TotalAssignedJobs: 0}}},
			{cancel: cancel},
		},
	}
	scaler := &recordingScaler{}

	err := runScaleSetListener(ctx, client, 4, scaler, nil)

	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, []int{1, 0}, scaler.desiredCounts)
	require.Equal(t, []int{7}, client.deletedMessageIDs)
}

type listenerResponse struct {
	message *scaleset.RunnerScaleSetMessage
	err     error
	cancel  context.CancelFunc
}

type fakeListenerClient struct {
	session           scaleset.RunnerScaleSetSession
	responses         []listenerResponse
	responseIndex     int
	deletedMessageIDs []int
}

func (f *fakeListenerClient) Session() scaleset.RunnerScaleSetSession {
	return f.session
}

func (f *fakeListenerClient) GetMessage(context.Context, int, int) (*scaleset.RunnerScaleSetMessage, error) {
	response := f.responses[f.responseIndex]
	f.responseIndex++
	if response.cancel != nil {
		response.cancel()
	}
	return response.message, response.err
}

func (f *fakeListenerClient) DeleteMessage(_ context.Context, messageID int) error {
	f.deletedMessageIDs = append(f.deletedMessageIDs, messageID)
	return nil
}

type recordingScaler struct {
	desiredCounts []int
}

func (r *recordingScaler) HandleDesiredRunnerCount(_ context.Context, count int) (int, error) {
	r.desiredCounts = append(r.desiredCounts, count)
	return count, nil
}

func (r *recordingScaler) HandleJobStarted(context.Context, *scaleset.JobStarted) error {
	return nil
}

func (r *recordingScaler) HandleJobCompleted(context.Context, *scaleset.JobCompleted) error {
	return nil
}
