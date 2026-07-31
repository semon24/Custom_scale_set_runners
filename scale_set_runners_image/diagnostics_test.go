package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProbeHTTPReportsReachableEndpoint(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-GitHub-Request-Id", "test-request")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	result := probeHTTP(context.Background(), server.Client(), server.URL)

	require.True(t, result.Reachable)
	require.Equal(t, http.StatusNoContent, result.StatusCode)
	require.Equal(t, "test-request", result.RequestID)
	require.Empty(t, result.Error)
}

func TestProbeHTTPReportsTransportFailure(t *testing.T) {
	client := &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return nil, context.DeadlineExceeded
	})}

	result := probeHTTP(context.Background(), client, "https://api.github.com/")

	require.False(t, result.Reachable)
	require.Contains(t, result.Error, context.DeadlineExceeded.Error())
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}
