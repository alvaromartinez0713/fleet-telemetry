// Package app is the intake module's core: the use case that accepts a
// telemetry batch and queues it. Every external interaction crosses a port this
// package declares, so the core imports neither a driver nor the HTTP layer.
package app

import (
	"context"

	"fleet-telemetry/internal/contracts"
)

// Producer queues an accepted batch on the bus. The Kafka adapter of this
// module implements it; the use case only knows that a nil error means the
// events are durably queued, which is what a 202 to the device promises
// (ADR-002).
type Producer interface {
	Produce(context.Context, []contracts.Envelope) error
}
