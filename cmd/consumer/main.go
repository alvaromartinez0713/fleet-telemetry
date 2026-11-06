// Command consumer moves telemetry from the bus to TimescaleDB: it micro-batches
// consumed records, persists them idempotently with UNIQUE(device_id,
// captured_at, seq) and advances the cumulative watermark in the same
// transaction (ADR-002, ADR-003). It commits offsets only after the write, so a
// crash replays instead of losing events. It never drops events by content.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"fleet-telemetry/internal/bus"
	"fleet-telemetry/internal/config"
	"fleet-telemetry/internal/consumer"
	"fleet-telemetry/internal/store"
)

const (
	serviceName    = "consumer"
	startupTimeout = 30 * time.Second
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(log)

	if err := run(log); err != nil {
		log.Error("consumer failed", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := config.LoadConsumer()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	startupCtx, cancel := context.WithTimeout(ctx, startupTimeout)
	st, err := store.Open(startupCtx, cfg.DatabaseURL)
	if err != nil {
		cancel()
		return fmt.Errorf("open store: %w", err)
	}
	defer st.Close()

	// The schema is idempotent, so the consumer owns it: it is the only binary
	// that cannot work without the tables.
	if err := st.Migrate(startupCtx); err != nil {
		cancel()
		return fmt.Errorf("migrate: %w", err)
	}
	cancel()

	group, err := bus.NewGroup(cfg.KafkaBrokers, cfg.Group, cfg.Topic)
	if err != nil {
		return fmt.Errorf("join group: %w", err)
	}
	defer group.Close()

	runner := consumer.NewRunner(group, st, consumer.Options{
		MaxBatch:   cfg.MaxBatchSize,
		FlushEvery: cfg.FlushEvery,
		Logger:     log,
	})

	log.Info("consumer running",
		"service", serviceName,
		"topic", cfg.Topic,
		"group", cfg.Group,
		"brokers", cfg.KafkaBrokers,
		"flush_every", cfg.FlushEvery,
		"max_batch", cfg.MaxBatchSize)

	if err := runner.Run(ctx); err != nil {
		return fmt.Errorf("run: %w", err)
	}

	log.Info("consumer stopped", "service", serviceName)
	return nil
}
