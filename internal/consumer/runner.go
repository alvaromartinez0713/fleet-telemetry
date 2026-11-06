// Package consumer is the bus-to-storage use case: it accumulates consumed
// messages into a micro-batch, persists them idempotently and only then commits
// the offsets, so a crash replays a batch instead of losing it (ADR-002).
package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"fleet-telemetry/internal/bus"
	"fleet-telemetry/internal/contracts"
	"fleet-telemetry/internal/store"
)

// Writer persists one micro-batch and advances the watermarks of the devices it
// contains in the same transaction. *store.Store implements it.
type Writer interface {
	PersistBatch(context.Context, []contracts.Envelope) (store.PersistResult, error)
}

// Source is the consumer group the runner drains. *bus.Group implements it.
type Source interface {
	Poll(context.Context) ([]bus.Message, error)
	Commit(context.Context, []bus.Message) error
}

// Options configures the flush policy. Zero values fall back to the defaults,
// which are what the binary uses in production.
type Options struct {
	// MaxBatch flushes as soon as this many messages are pending.
	MaxBatch int
	// FlushEvery flushes whatever is pending when the tick elapses; it is also
	// the poll deadline, so a deadline is a tick and never an error.
	FlushEvery time.Duration
	// Backoff is how long the runner waits before retrying a failed poll or a
	// failed persist.
	Backoff time.Duration
	// DrainTimeout bounds the final flush done during shutdown.
	DrainTimeout time.Duration
	Logger       *slog.Logger
}

const (
	defaultMaxBatch     = 500
	defaultFlushEvery   = 2 * time.Second
	defaultBackoff      = time.Second
	defaultDrainTimeout = 10 * time.Second
	// maxPollFailures bounds how long a permanently broken group is retried.
	// A transient poll error is expected and retried; an error that keeps
	// coming back is not, and the process must fail so orchestration restarts
	// it instead of spinning forever without consuming.
	maxPollFailures = 5
	// maxFlushFailures bounds a storage outage. A flush failure is retried with
	// the batch kept, but a database that stays down long enough also gets the
	// consumer evicted from the group: after this many consecutive failures the
	// process must exit instead of re-persisting the same batch forever.
	maxFlushFailures = 5
)

// Runner joins the bus group and moves telemetry from the bus to storage.
type Runner struct {
	group  Source
	writer Writer
	opts   Options
	log    *slog.Logger

	pending     []bus.Message
	flushFailed bool
	lastFlush   time.Time
}

// NewRunner builds a runner. The group and the writer stay adapters: every
// decision about batching, retrying and committing lives here.
func NewRunner(group Source, writer Writer, opts Options) *Runner {
	if opts.MaxBatch <= 0 {
		opts.MaxBatch = defaultMaxBatch
	}
	if opts.FlushEvery <= 0 {
		opts.FlushEvery = defaultFlushEvery
	}
	if opts.Backoff <= 0 {
		opts.Backoff = defaultBackoff
	}
	if opts.DrainTimeout <= 0 {
		opts.DrainTimeout = defaultDrainTimeout
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Runner{
		group:     group,
		writer:    writer,
		opts:      opts,
		log:       opts.Logger,
		lastFlush: time.Now(),
	}
}

// Run consumes until ctx is cancelled. A cancelled context is a graceful stop:
// whatever is pending is flushed with a fresh bounded context and Run returns
// nil. A poll failure is retried with backoff, but a group that keeps failing
// for maxPollFailures consecutive attempts is broken (bad authorization,
// unknown topic, unreachable cluster): Run drains and returns the error so the
// orchestrator restarts the process instead of leaving it consuming nothing.
func (r *Runner) Run(ctx context.Context) error {
	pollFailures := 0
	flushFailures := 0
	for {
		if ctx.Err() != nil {
			r.drain(ctx)
			return nil
		}

		if r.flushDue() {
			if err := r.flush(ctx); err != nil {
				r.flushFailed = true
				flushFailures++
				r.log.Error("flush failed, keeping the batch for a retry",
					"error", err, "pending", len(r.pending), "consecutive_failures", flushFailures)
				if flushFailures >= maxFlushFailures {
					r.drain(ctx)
					return fmt.Errorf("storage is not writable after %d consecutive flush failures: %w", flushFailures, err)
				}
				if !r.sleep(ctx) {
					r.drain(ctx)
					return nil
				}
				continue
			}
			r.flushFailed = false
			flushFailures = 0
			r.lastFlush = time.Now()
			continue
		}

		pollCtx, cancel := context.WithTimeout(ctx, r.pollWait())
		msgs, err := r.group.Poll(pollCtx)
		cancel()
		// Whatever the fetch delivered is kept even when it also carries an
		// error: the broker already advanced past those records, so dropping
		// them would commit past events that were never persisted (ADR-002).
		r.pending = append(r.pending, msgs...)
		if err != nil {
			pollFailures++
			r.log.Error("poll failed", "error", err, "consecutive_failures", pollFailures, "kept", len(msgs))
			if pollFailures >= maxPollFailures {
				r.drain(ctx)
				return fmt.Errorf("consumer group is not usable after %d consecutive poll failures: %w", pollFailures, err)
			}
			if !r.sleep(ctx) {
				r.drain(ctx)
				return nil
			}
			continue
		}
		pollFailures = 0
	}
}

// flushDue reports whether the pending slice must be persisted now: the batch
// is full, the tick elapsed, or the previous attempt failed and is retried.
func (r *Runner) flushDue() bool {
	if len(r.pending) == 0 {
		return false
	}
	if r.flushFailed {
		return true
	}
	return len(r.pending) >= r.opts.MaxBatch || time.Since(r.lastFlush) >= r.opts.FlushEvery
}

// pollWait is the time left until the next tick, so the tick stays accurate
// even when messages keep arriving in small poll results.
func (r *Runner) pollWait() time.Duration {
	if len(r.pending) == 0 {
		return r.opts.FlushEvery
	}
	if remaining := r.opts.FlushEvery - time.Since(r.lastFlush); remaining > 0 {
		return remaining
	}
	return time.Millisecond
}

// flush persists the pending slice and then commits every offset of it,
// including the records that could not be decoded: a poison record is skipped
// but never blocks its partition (ADR-002).
func (r *Runner) flush(ctx context.Context) error {
	if len(r.pending) == 0 {
		return nil
	}

	envs := make([]contracts.Envelope, 0, len(r.pending))
	failed := 0
	for _, msg := range r.pending {
		if msg.DecodeErr != nil {
			failed++
			attrs := []any{"error", msg.DecodeErr}
			if msg.Record != nil {
				attrs = append(attrs, "partition", msg.Record.Partition, "offset", msg.Record.Offset)
			}
			r.log.Warn("skipping undecodable record", attrs...)
			continue
		}
		envs = append(envs, msg.Envelope)
	}

	if len(envs) > 0 {
		res, err := r.writer.PersistBatch(ctx, envs)
		if err != nil {
			return fmt.Errorf("persist %d events: %w", len(envs), err)
		}
		r.log.Info("batch persisted",
			"inserted", res.Inserted,
			"duplicates", res.Duplicates,
			"advanced", res.Advanced,
			"undecodable", failed)
	}

	if err := r.group.Commit(ctx, r.pending); err != nil {
		return fmt.Errorf("commit %d offsets: %w", len(r.pending), err)
	}

	r.pending = nil
	return nil
}

// drain flushes what is pending during shutdown with a fresh bounded context,
// because the cancelled one would abort the write the consumer exists to keep.
func (r *Runner) drain(ctx context.Context) {
	if len(r.pending) == 0 {
		return
	}
	pending := len(r.pending)
	flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.opts.DrainTimeout)
	defer cancel()

	if err := r.flush(flushCtx); err != nil {
		r.log.Error("final flush failed, offsets left uncommitted for redelivery",
			"error", err, "pending", pending)
		return
	}
	r.log.Info("final flush done", "records", pending)
}

// sleep waits for the backoff delay and reports whether ctx is still alive.
func (r *Runner) sleep(ctx context.Context) bool {
	timer := time.NewTimer(r.opts.Backoff)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
