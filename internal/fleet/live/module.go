// Package live is the skeleton of the live module (T4): the public API the
// portal and the mobile app read, REST for the first paint and SSE for the
// deltas (ADR-001), plus the synchronous call to the assistant guarded by a
// circuit breaker (ADR-004).
//
// Nothing is implemented yet. This package is the module's whole public surface:
// New builds it and Run starts it. internal/archtest already holds the module to
// the dependency rules every other module follows, so the first implementation
// lands inside the boundary instead of beside it.
package live

import (
	"context"
	"errors"
)

// ErrNotImplemented is what Run returns until T4 lands. The binary reports it on
// stderr and exits zero, exactly as the five-binary skeleton did.
var ErrNotImplemented = errors.New("live: skeleton — not implemented yet (see odd/tasks/fleet-telemetry.md T4)")

// Module is the module handle. It carries no state until T4 needs some.
type Module struct{}

// New builds the module.
func New() (*Module, error) { return &Module{}, nil }

// Run starts the module and returns when ctx is cancelled.
func (m *Module) Run(context.Context) error { return ErrNotImplemented }
