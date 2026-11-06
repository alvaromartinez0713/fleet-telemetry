// Command journal is the composition root of the telemetry journal module: it
// loads the deployment configuration, builds the module and records until the
// process is signalled. Wiring only: no business logic, no HTTP, no SQL.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"fleet-telemetry/internal/platform/config"
	"fleet-telemetry/internal/telemetry/journal"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(log)

	if err := run(log); err != nil {
		log.Error("journal failed", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := config.LoadJournal()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	module, err := journal.New(ctx, journal.Config{
		KafkaBrokers: cfg.KafkaBrokers,
		Topic:        cfg.Topic,
		DatabaseURL:  cfg.DatabaseURL,
		Group:        cfg.Group,
		FlushEvery:   cfg.FlushEvery,
		MaxBatch:     cfg.MaxBatchSize,
	}, journal.Options{Logger: log})
	if err != nil {
		return err
	}

	return module.Run(ctx)
}
