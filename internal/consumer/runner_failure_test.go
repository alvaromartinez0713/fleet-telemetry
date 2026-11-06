package consumer_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"fleet-telemetry/internal/bus"
	"fleet-telemetry/internal/consumer"
)

// failingGroup is a group that never works: the broker rejects the consumer
// (bad authorization, unknown topic, unreachable cluster).
type failingGroup struct{ polls int }

func (g *failingGroup) Poll(context.Context) ([]bus.Message, error) {
	g.polls++
	return nil, errors.New("authorization failed")
}

func (g *failingGroup) Commit(context.Context, []bus.Message) error { return nil }

// TestRunReturnsErrorWhenTheGroupKeepsFailing pins that a permanently broken
// group ends Run with an error instead of retrying forever: the process must
// exit so orchestration restarts it, not spin consuming nothing.
func TestRunReturnsErrorWhenTheGroupKeepsFailing(t *testing.T) {
	group := &failingGroup{}
	runner := consumer.NewRunner(group, &fakeWriter{}, consumer.Options{
		FlushEvery: time.Millisecond,
		Backoff:    time.Millisecond,
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	err := runner.Run(context.Background())
	if err == nil {
		t.Fatal("Run returned nil, want an error once the group keeps failing")
	}
	if !strings.Contains(err.Error(), "consecutive poll failures") {
		t.Fatalf("err = %q, want it to name the consecutive poll failures", err)
	}
	if group.polls < 2 {
		t.Fatalf("polls = %d, want the failure to be retried before giving up", group.polls)
	}
}

// TestRunStopsCleanlyOnCancellation keeps the graceful path honest: a cancelled
// context is a stop, never an error.
func TestRunStopsCleanlyOnCancellation(t *testing.T) {
	group := newFakeGroup(&recorder{})
	runner := consumer.NewRunner(group, &fakeWriter{}, consumer.Options{
		FlushEvery: time.Millisecond,
		Backoff:    time.Millisecond,
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v after cancellation, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
}
