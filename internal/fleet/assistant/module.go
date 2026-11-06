// Package assistant is the skeleton of the assistant module (T6): the AI
// operator, a direct function-calling loop in Go against the LLM API (ADR-005,
// no LangChain) with 8-10 tools including salud_pipeline().
//
// Nothing is implemented yet. This package is the module's whole public surface:
// New builds it and Run starts it. internal/archtest already holds the module to
// the dependency rules every other module follows, so the first implementation
// lands inside the boundary instead of beside it.
package assistant

import (
	"context"
	"errors"
)

// ErrNotImplemented is what Run returns until T6 lands. The binary reports it on
// stderr and exits zero, exactly as the five-binary skeleton did.
var ErrNotImplemented = errors.New("assistant: skeleton — not implemented yet (see odd/tasks/fleet-telemetry.md T6)")

// Module is the module handle. It carries no state until T6 needs some.
type Module struct{}

// New builds the module.
func New() (*Module, error) { return &Module{}, nil }

// Run starts the module and returns when ctx is cancelled.
func (m *Module) Run(context.Context) error { return ErrNotImplemented }
