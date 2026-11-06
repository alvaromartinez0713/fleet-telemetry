package consumer_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"fleet-telemetry/internal/bus"
	"fleet-telemetry/internal/consumer"
	"fleet-telemetry/internal/contracts"
)

func testEnvelope() contracts.Envelope {
	now := time.Now().UTC()
	return contracts.Envelope{
		Event: contracts.Event{
			DeviceID:      "dev-1",
			Seq:           1,
			CapturedAt:    now.Add(-time.Minute),
			Lat:           4.65,
			Lon:           -74.05,
			Mode:          contracts.ModeMoving,
			SchemaVersion: contracts.SchemaVersion,
		},
		ReceivedAt: now,
	}
}

// failingGroup is a group that never works: the broker rejects the consumer
// (bad authorization, unknown topic, unreachable cluster).
type failingGroup struct{ polls int }

func (g *failingGroup) Poll(context.Context) ([]bus.Message, error) {
	g.polls++
	return nil, errors.New("authorization failed")
}

func (g *failingGroup) Commit(context.Context, []bus.Message) error { return nil }

// partialGroup reproduces a fetch that carries both records and an error: the
// broker has already advanced past the delivered record.
type partialGroup struct {
	mu        sync.Mutex
	delivered bool
	noError   bool
	committed [][]bus.Message
	entry     bus.Message
}

func (g *partialGroup) Poll(ctx context.Context) ([]bus.Message, error) {
	g.mu.Lock()
	if !g.delivered {
		g.delivered = true
		entry := g.entry
		noErr := g.noError
		g.mu.Unlock()
		if noErr {
			return []bus.Message{entry}, nil
		}
		return []bus.Message{entry}, errors.New("fetch error after the position advanced")
	}
	g.mu.Unlock()
	<-ctx.Done()
	return nil, nil
}

func (g *partialGroup) Commit(_ context.Context, msgs []bus.Message) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.committed = append(g.committed, append([]bus.Message(nil), msgs...))
	return nil
}

func (g *partialGroup) committedCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	total := 0
	for _, batch := range g.committed {
		total += len(batch)
	}
	return total
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestRunReturnsErrorWhenTheGroupKeepsFailing pins that a permanently broken
// group ends Run with an error instead of retrying forever: the process must
// exit so orchestration restarts it, not spin consuming nothing.
func TestRunReturnsErrorWhenTheGroupKeepsFailing(t *testing.T) {
	group := &failingGroup{}
	runner := consumer.NewRunner(group, &fakeWriter{rec: &recorder{}}, consumer.Options{
		FlushEvery: time.Millisecond,
		Backoff:    time.Millisecond,
		Logger:     quietLogger(),
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

// TestFetchedRecordsArePersistedEvenWhenThePollAlsoFails pins the at-least-once
// guarantee: a fetch that carries records AND an error must still have those
// records persisted and committed, because the broker already advanced past
// them. Dropping them would commit over events that were never stored.
func TestFetchedRecordsArePersistedEvenWhenThePollAlsoFails(t *testing.T) {
	group := &partialGroup{entry: bus.Message{Envelope: testEnvelope()}}
	writer := &fakeWriter{rec: &recorder{}}
	runner := consumer.NewRunner(group, writer, consumer.Options{
		MaxBatch:   1,
		FlushEvery: time.Millisecond,
		Backoff:    time.Millisecond,
		Logger:     quietLogger(),
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()

	deadline := time.Now().Add(3 * time.Second)
	for len(writer.persisted()) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	persisted := len(writer.persisted())
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run returned %v after cancellation, want nil", err)
	}

	if persisted != 1 {
		t.Fatalf("persisted batches = %d, want the record delivered with the fetch error to be persisted", persisted)
	}
	if got := group.committedCount(); got != 1 {
		t.Fatalf("committed records = %d, want 1", got)
	}
}

// TestRunReturnsErrorWhenStorageKeepsFailing pins the storage side: a database
// that stays down must end the process too. Retrying the same batch forever
// also loses the group membership, so the failure has to be bounded.
func TestRunReturnsErrorWhenStorageKeepsFailing(t *testing.T) {
	group := &partialGroup{entry: bus.Message{Envelope: testEnvelope()}, noError: true}
	writer := &fakeWriter{rec: &recorder{}, failures: 1000}
	runner := consumer.NewRunner(group, writer, consumer.Options{
		MaxBatch:   1,
		FlushEvery: time.Millisecond,
		Backoff:    time.Millisecond,
		Logger:     quietLogger(),
	})

	err := runner.Run(context.Background())
	if err == nil {
		t.Fatal("Run returned nil, want an error once storage keeps failing")
	}
	if !strings.Contains(err.Error(), "consecutive flush failures") {
		t.Fatalf("err = %q, want it to name the consecutive flush failures", err)
	}
}

// TestRunStopsCleanlyOnCancellation keeps the graceful path honest: a cancelled
// context is a stop, never an error.
func TestRunStopsCleanlyOnCancellation(t *testing.T) {
	group := newFakeGroup(&recorder{})
	runner := consumer.NewRunner(group, &fakeWriter{rec: &recorder{}}, consumer.Options{
		FlushEvery: time.Millisecond,
		Backoff:    time.Millisecond,
		Logger:     quietLogger(),
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
