// Command ingest is the HTTP intake: it authenticates devices (ADR-003),
// validates the batch contract and queues it on the bus, answering 202 as soon
// as the events are durably queued (ADR-002). It never touches the database.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"

	"fleet-telemetry/internal/bus"
	"fleet-telemetry/internal/config"
	"fleet-telemetry/internal/ingest"
)

const (
	serviceName     = "ingest"
	shutdownTimeout = 10 * time.Second
	startupTimeout  = 10 * time.Second
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(log)

	// gin's debug mode prints a banner and the route table to stdout, behind the
	// structured logger this service owns (ADR-006). Routing, logging and recovery
	// are explicit in the intake, so release mode is the honest one.
	gin.SetMode(gin.ReleaseMode)

	if err := run(log); err != nil {
		log.Error("ingest failed", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := config.LoadIngest()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	producer, err := bus.NewProducer(cfg.KafkaBrokers, cfg.Topic)
	if err != nil {
		return fmt.Errorf("create producer: %w", err)
	}
	defer producer.Close()

	handler := ingest.NewServer(
		ingest.NewAuthenticator(cfg.JWTSecret, time.Now),
		producer,
		ingest.Options{Logger: log, MaxBatch: cfg.MaxBatch},
	)

	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           handler,
		ReadHeaderTimeout: startupTimeout,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serveErr := make(chan error, 1)
	go func() {
		log.Info("ingest listening",
			"service", serviceName, "addr", cfg.Addr, "topic", cfg.Topic, "brokers", cfg.KafkaBrokers)
		serveErr <- srv.ListenAndServe()
	}()

	select {
	case err := <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve: %w", err)
		}
		return nil
	case <-ctx.Done():
		log.Info("ingest shutting down", "service", serviceName)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	log.Info("ingest stopped", "service", serviceName)
	return nil
}
