package consumer_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"fleet-telemetry/internal/bus"
	"fleet-telemetry/internal/consumer"
	"fleet-telemetry/internal/contracts"
	"fleet-telemetry/internal/store"
)

// recorder collects the observable order of writer and group operations so a
// test can prove a commit never happens before a successful persist.
type recorder struct {
	mu     sync.Mutex
	events []string
}

func (r *recorder) add(e string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
}

func (r *recorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.events...)
}

func (r *recorder) count(e string) int {
	n := 0
	for _, got := range r.snapshot() {
		if got == e {
			n++
		}
	}
	return n
}

type fakeWriter struct {
	mu       sync.Mutex
	rec      *recorder
	batches  [][]contracts.Envelope
	failures int
	ctxErrs  []error
}

func (w *fakeWriter) PersistBatch(ctx context.Context, envs []contracts.Envelope) (store.PersistResult, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.ctxErrs = append(w.ctxErrs, ctx.Err())
	if w.failures > 0 {
		w.failures--
		w.rec.add("persist-error")
		return store.PersistResult{}, errors.New("timescale: connection reset")
	}
	w.rec.add("persist-ok")
	w.batches = append(w.batches, append([]contracts.Envelope(nil), envs...))
	return store.PersistResult{Inserted: int64(len(envs)), Duplicates: 1}, nil
}

func (w *fakeWriter) persisted() [][]contracts.Envelope {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.batches
}

type fakeGroup struct {
	mu        sync.Mutex
	rec       *recorder
	feeds     [][]bus.Message
	committed [][]bus.Message
	idle      chan struct{}
	idleOnce  sync.Once
}

func newFakeGroup(rec *recorder, feeds ...[]bus.Message) *fakeGroup {
	return &fakeGroup{rec: rec, feeds: feeds, idle: make(chan struct{})}
}

func (g *fakeGroup) Poll(ctx context.Context) ([]bus.Message, error) {
	g.mu.Lock()
	if len(g.feeds) > 0 {
		msgs := g.feeds[0]
		g.feeds = g.feeds[1:]
		g.mu.Unlock()
		return msgs, nil
	}
	g.mu.Unlock()
	g.idleOnce.Do(func() { close(g.idle) })
	<-ctx.Done()
	return nil, nil
}

func (g *fakeGroup) Commit(_ context.Context, msgs []bus.Message) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.rec.add("commit")
	g.committed = append(g.committed, append([]bus.Message(nil), msgs...))
	return nil
}

func (g *fakeGroup) committedBatches() [][]bus.Message {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.committed
}

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func message(device string, seq uint64) bus.Message {
	env := contracts.Envelope{
		Event: contracts.Event{
			DeviceID:      device,
			Seq:           seq,
			CapturedAt:    time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC).Add(time.Duration(seq) * time.Second),
			Lat:           4.65,
			Lon:           -74.05,
			Mode:          contracts.ModeMoving,
			SchemaVersion: contracts.SchemaVersion,
		},
		ReceivedAt: time.Date(2026, 11, 5, 12, 0, 1, 0, time.UTC),
	}
	return bus.Message{
		Envelope: env,
		Record:   &kgo.Record{Topic: contracts.TopicTelemetry, Partition: 0, Offset: int64(seq)},
	}
}

func decodeFailedMessage() bus.Message {
	return bus.Message{
		Record:    &kgo.Record{Topic: contracts.TopicTelemetry, Partition: 0, Offset: 99},
		DecodeErr: errors.New("invalid character 'x' looking for beginning of value"),
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// runUntil starts the runner and returns a cancel func plus a channel carrying
// the value Run eventually returns.
func runUntil(t *testing.T, r *consumer.Runner) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	return cancel, done
}

func mustFinish(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() = %v, want nil after cancellation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run() did not return after cancellation")
	}
}

func TestRunCommitsDuplicatesInsteadOfStopping(t *testing.T) {
	rec := &recorder{}
	group := newFakeGroup(rec, []bus.Message{message("dev-1", 1), message("dev-1", 2)})
	writer := &fakeWriter{rec: rec}
	runner := consumer.NewRunner(group, writer, consumer.Options{
		MaxBatch:   10,
		FlushEvery: 10 * time.Millisecond,
		Backoff:    time.Millisecond,
		Logger:     testLogger(),
	})

	cancel, done := runUntil(t, runner)
	waitFor(t, "the batch to be committed", func() bool { return len(group.committedBatches()) == 1 })
	mustFinish(t, cancel, done)

	committed := group.committedBatches()
	if len(committed) != 1 {
		t.Fatalf("committed %d batches, want 1", len(committed))
	}
	if len(committed[0]) != 2 {
		t.Fatalf("committed %d messages, want 2", len(committed[0]))
	}
	if got := len(writer.persisted()); got != 1 {
		t.Fatalf("persisted %d batches, want 1", got)
	}
	if got := len(writer.persisted()[0]); got != 2 {
		t.Fatalf("persisted %d envelopes, want 2", got)
	}
}

func TestRunKeepsMessagesUncommittedWhenPersistFails(t *testing.T) {
	rec := &recorder{}
	group := newFakeGroup(rec, []bus.Message{message("dev-1", 1), message("dev-1", 2)})
	writer := &fakeWriter{rec: rec, failures: 1}
	runner := consumer.NewRunner(group, writer, consumer.Options{
		MaxBatch:   10,
		FlushEvery: 10 * time.Millisecond,
		Backoff:    time.Millisecond,
		Logger:     testLogger(),
	})

	cancel, done := runUntil(t, runner)
	waitFor(t, "the retried batch to be committed", func() bool {
		return len(group.committedBatches()) == 1
	})
	mustFinish(t, cancel, done)

	order := rec.snapshot()
	if len(order) < 3 {
		t.Fatalf("recorded events = %v, want a failed persist, a retry and a commit", order)
	}
	if order[0] != "persist-error" || order[1] != "persist-ok" || order[2] != "commit" {
		t.Fatalf("order = %v, want persist-error, persist-ok, commit: nothing may be committed before the persist succeeds", order)
	}
	if got := len(group.committedBatches()); got != 1 {
		t.Fatalf("committed %d times, want 1 (the failed attempt must not commit)", got)
	}
	if got := len(group.committedBatches()[0]); got != 2 {
		t.Fatalf("committed %d messages after the retry, want the whole 2-message slice", got)
	}
}

func TestRunSkipsDecodeFailuresButCommitsThem(t *testing.T) {
	rec := &recorder{}
	group := newFakeGroup(rec, []bus.Message{decodeFailedMessage(), message("dev-1", 1)})
	writer := &fakeWriter{rec: rec}
	runner := consumer.NewRunner(group, writer, consumer.Options{
		MaxBatch:   10,
		FlushEvery: 10 * time.Millisecond,
		Backoff:    time.Millisecond,
		Logger:     testLogger(),
	})

	cancel, done := runUntil(t, runner)
	waitFor(t, "the mixed batch to be committed", func() bool { return len(group.committedBatches()) == 1 })
	mustFinish(t, cancel, done)

	persisted := writer.persisted()
	if len(persisted) != 1 || len(persisted[0]) != 1 {
		t.Fatalf("persisted batches = %v, want one batch holding only the valid envelope", persisted)
	}
	if persisted[0][0].DeviceID != "dev-1" || persisted[0][0].Seq != 1 {
		t.Fatalf("persisted the wrong envelope: %+v", persisted[0][0])
	}
	committed := group.committedBatches()[0]
	if len(committed) != 2 {
		t.Fatalf("committed %d records, want 2 (poison record included)", len(committed))
	}
	if committed[0].DecodeErr == nil {
		t.Fatalf("first committed record should be the decode failure with its offset")
	}
}

func TestRunFlushesAtMaxBatchBeforeTheTick(t *testing.T) {
	rec := &recorder{}
	group := newFakeGroup(rec, []bus.Message{message("dev-1", 1), message("dev-1", 2)})
	writer := &fakeWriter{rec: rec}
	runner := consumer.NewRunner(group, writer, consumer.Options{
		MaxBatch:   2,
		FlushEvery: time.Hour,
		Backoff:    time.Millisecond,
		Logger:     testLogger(),
	})

	cancel, done := runUntil(t, runner)
	waitFor(t, "the maxBatch flush", func() bool { return len(group.committedBatches()) == 1 })
	mustFinish(t, cancel, done)

	if got := len(group.committedBatches()[0]); got != 2 {
		t.Fatalf("committed %d messages, want 2", got)
	}
}

func TestRunDrainsPendingOnCancellation(t *testing.T) {
	rec := &recorder{}
	group := newFakeGroup(rec, []bus.Message{message("dev-1", 1)})
	writer := &fakeWriter{rec: rec}
	runner := consumer.NewRunner(group, writer, consumer.Options{
		MaxBatch:   100,
		FlushEvery: time.Hour, // the tick never elapses: only shutdown can flush
		Backoff:    time.Millisecond,
		Logger:     testLogger(),
	})

	cancel, done := runUntil(t, runner)
	select {
	case <-group.idle:
	case <-time.After(5 * time.Second):
		t.Fatal("runner never reached an empty poll")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() = %v, want nil after cancellation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run() did not return after cancellation")
	}

	if got := len(group.committedBatches()); got != 1 {
		t.Fatalf("committed %d batches, want 1 (the pending slice must be drained)", got)
	}
	if got := len(writer.persisted()); got != 1 {
		t.Fatalf("persisted %d batches, want 1", got)
	}
	for i, err := range writer.ctxErrs {
		if err != nil {
			t.Fatalf("persist call %d ran with a cancelled context: %v; the final flush needs a fresh context", i, err)
		}
	}
}
