// Command intake is the composition root of the telemetry intake module: it
// loads the deployment configuration, builds the module and runs it until the
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
	"fleet-telemetry/internal/telemetry/intake"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(log)

	if err := run(log); err != nil {
		log.Error("intake failed", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := config.LoadIntake()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	module, err := intake.New(intake.Config{
		Addr:         cfg.Addr,
		KafkaBrokers: cfg.KafkaBrokers,
		Topic:        cfg.Topic,
		JWTSecret:    cfg.JWTSecret,
		MaxBatch:     cfg.MaxBatch,
	}, intake.Options{Logger: log})
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	return module.Run(ctx)
}
