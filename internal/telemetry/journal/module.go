// Package journal is the telemetry journal module: it records accepted reports.
// It micro-batches consumed records, persists them idempotently with
// UNIQUE(device_id, captured_at, seq) and advances the cumulative watermark in
// the same transaction (ADR-002, ADR-003). It commits offsets only after the
// write, so a crash replays instead of losing events, and it never drops events
// by content.
//
// This package is the module's whole public surface: New builds the module and
// Run records until the context is cancelled. Everything under
// internal/telemetry/journal/app, .../journal/adapter and .../journal/watermark is
// private to the module; internal/archtest fails the build when another module or
// a binary imports one of them.
package journal

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"fleet-telemetry/internal/telemetry/journal/adapter"
	"fleet-telemetry/internal/telemetry/journal/app"
)

const (
	serviceName    = "journal"
	startupTimeout = 30 * time.Second
)

// Config is everything the module needs from the deployment. It carries plain
// values only: loading and defaulting them belongs to the binary.
type Config struct {
	KafkaBrokers []string
	Topic        string
	DatabaseURL  string
	Group        string
	FlushEvery   time.Duration
	MaxBatch     int
}

// Options are the module's boundaries. Zero values fall back to the defaults the
// binary runs with.
type Options struct {
	// Logger receives the startup line, the per-batch lines and the failures.
	Logger *slog.Logger
}

// Module is the journal service: it owns its store, its consumer group and the
// runner, so the binary only has to build it and run it.
type Module struct {
	cfg    Config
	log    *slog.Logger
	store  *adapter.Store
	group  *adapter.Group
	runner *app.Runner
}

// New builds the module: it opens and migrates the store, joins the consumer
// group and wires the runner. ctx bounds the startup work only.
func New(ctx context.Context, cfg Config, opts Options) (*Module, error) {
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}

	startupCtx, cancel := context.WithTimeout(ctx, startupTimeout)
	defer cancel()

	store, err := adapter.Open(startupCtx, cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("open store: %w", err)
	}

	// The schema is idempotent, so the journal owns it: it is the only binary
	// that cannot work without the tables.
	if err := store.Migrate(startupCtx); err != nil {
		store.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}

	group, err := adapter.NewGroup(cfg.KafkaBrokers, cfg.Group, cfg.Topic)
	if err != nil {
		store.Close()
		return nil, fmt.Errorf("join group: %w", err)
	}

	return &Module{
		cfg:   cfg,
		log:   log,
		store: store,
		group: group,
		runner: app.NewRunner(group, store, app.Options{
			MaxBatch:   cfg.MaxBatch,
			FlushEvery: cfg.FlushEvery,
			Logger:     log,
		}),
	}, nil
}

// Run records until ctx is cancelled, then drains what is pending and returns
// nil: a cancelled context is a graceful stop, not an error.
func (m *Module) Run(ctx context.Context) error {
	defer m.group.Close()
	defer m.store.Close()

	m.log.Info("journal running",
		"service", serviceName,
		"topic", m.cfg.Topic,
		"group", m.cfg.Group,
		"brokers", m.cfg.KafkaBrokers,
		"flush_every", m.cfg.FlushEvery,
		"max_batch", m.cfg.MaxBatch)

	if err := m.runner.Run(ctx); err != nil {
		return fmt.Errorf("run: %w", err)
	}

	m.log.Info("journal stopped", "service", serviceName)
	return nil
}
