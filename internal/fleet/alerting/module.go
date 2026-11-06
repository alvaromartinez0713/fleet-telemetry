// Package alerting is the skeleton of the alerting module (T7): rules over
// vehicle state, e.g. a vehicle stopped more than 20 minutes in a critical zone
// (ADR-003).
//
// Nothing is implemented yet. This package is the module's whole public surface:
// New builds it and Run starts it. internal/archtest already holds the module to
// the dependency rules every other module follows, so the first implementation
// lands inside the boundary instead of beside it.
package alerting

import (
	"context"
	"errors"
)

// ErrNotImplemented is what Run returns until T7 lands. The binary reports it on
// stderr and exits zero, exactly as the five-binary skeleton did.
var ErrNotImplemented = errors.New("alerting: skeleton — not implemented yet (see odd/tasks/fleet-telemetry.md T7)")

// Module is the module handle. It carries no state until T7 needs some.
type Module struct{}

// New builds the module.
func New() (*Module, error) { return &Module{}, nil }

// Run starts the module and returns when ctx is cancelled.
func (m *Module) Run(context.Context) error { return ErrNotImplemented }
