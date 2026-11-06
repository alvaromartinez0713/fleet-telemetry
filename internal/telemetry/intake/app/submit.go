package app

import (
	"context"
	"fmt"
	"time"

	"fleet-telemetry/internal/contracts"
)

// Options are the use case's boundaries. Zero values fall back to the defaults
// the binary runs with.
type Options struct {
	// Now stamps received_at and bounds the accepted device clock skew.
	Now func() time.Time
	// MaxBatch is the configured batch limit. Zero, or a value above the wire
	// contract, falls back to contracts.MaxBatchSize: an operator-configured
	// limit lowers the contract, never raises it.
	MaxBatch int
}

// Submit is the intake use case. It owns the rules the intake promises: the
// batch limit, the contract, the device match, and the received_at stamp.
// Everything it cannot do alone (the bus, the clock) arrives through Options
// and the Producer port, so it needs no logger, no writer and no knowledge of
// the middleware around it.
type Submit struct {
	producer Producer
	now      func() time.Time
	maxBatch int
}

// NewSubmit builds the use case.
func NewSubmit(producer Producer, opts Options) *Submit {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.MaxBatch <= 0 || opts.MaxBatch > contracts.MaxBatchSize {
		opts.MaxBatch = contracts.MaxBatchSize
	}
	return &Submit{producer: producer, now: opts.Now, maxBatch: opts.MaxBatch}
}

// Submit accepts one batch for the authenticated device and returns how many
// envelopes were queued. deviceID is the identity the token authenticated; a
// batch that carries another device's events is refused as a whole, because the
// device cannot know when its own events were accepted otherwise.
//
// received_at is intake's stamp, the same for every envelope of the batch: a
// retry re-stamps it, which is why it is not part of the dedup identity
// (ADR-002).
func (s *Submit) Submit(ctx context.Context, deviceID string, batch contracts.Batch) (int, error) {
	now := s.now()

	if len(batch.Events) > s.maxBatch {
		return 0, &Rejection{
			Kind:   KindBatchTooLarge,
			Index:  NoIndex,
			Detail: fmt.Sprintf("the batch has %d events and the configured limit is %d", len(batch.Events), s.maxBatch),
		}
	}

	idx, err := batch.Validate(now)
	if err != nil {
		if idx < 0 {
			return 0, &Rejection{Kind: KindInvalidBatch, Index: NoIndex, Detail: err.Error(), Cause: err}
		}
		return 0, &Rejection{
			Kind:   KindInvalidBatch,
			Index:  idx,
			Detail: fmt.Sprintf("event %d: %s", idx, err),
			Cause:  err,
		}
	}

	for i, e := range batch.Events {
		if e.DeviceID == deviceID {
			continue
		}
		return 0, &Rejection{
			Kind:   KindDeviceMismatch,
			Index:  i,
			Detail: fmt.Sprintf("event %d belongs to another device; a device may only submit its own events", i),
			Cause:  fmt.Errorf("event %d of %d submitted by device %q", i, len(batch.Events), e.DeviceID),
		}
	}

	envs := make([]contracts.Envelope, len(batch.Events))
	for i, e := range batch.Events {
		envs[i] = contracts.Envelope{Event: e, ReceivedAt: now}
	}

	if err := s.producer.Produce(ctx, envs); err != nil {
		return 0, &Rejection{
			Kind:   KindBusUnavailable,
			Index:  NoIndex,
			Detail: "the batch could not be queued; retry later",
			Cause:  err,
		}
	}

	return len(envs), nil
}
