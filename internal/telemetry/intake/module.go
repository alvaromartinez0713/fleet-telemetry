// Package intake is the telemetry intake module: the HTTP edge that
// authenticates a device (ADR-003), validates the batch contract and queues it
// on the bus, answering 202 as soon as the events are durably queued (ADR-002).
// It never touches the database and it knows no other module.
//
// This package is the module's whole public surface: New builds the module and
// Run serves it. Everything under internal/telemetry/intake/http,
// .../intake/app and .../intake/adapter is private to the module; internal/archtest
// fails the build when another module or a binary imports one of them.
package intake

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"fleet-telemetry/internal/telemetry/intake/adapter"
	intakehttp "fleet-telemetry/internal/telemetry/intake/http"
)

const (
	serviceName       = "intake"
	shutdownTimeout   = 10 * time.Second
	readHeaderTimeout = 10 * time.Second
)

// Config is everything the module needs from the deployment. It carries plain
// values only: loading and defaulting them belongs to the binary.
type Config struct {
	Addr         string
	KafkaBrokers []string
	Topic        string
	JWTSecret    []byte
	MaxBatch     int
}

// Options are the module's boundaries. Zero values fall back to the defaults the
// binary runs with.
type Options struct {
	// Now stamps received_at and bounds the accepted device clock skew.
	Now func() time.Time
	// Logger receives the listening and failure lines of the module.
	Logger *slog.Logger
}

// Module is the intake service: it owns its producer and its HTTP server, so the
// binary only has to build it and run it.
type Module struct {
	cfg      Config
	log      *slog.Logger
	producer *adapter.Producer
	server   *http.Server
}

// New builds the module: the Kafka producer, the use case and the HTTP chain.
func New(cfg Config, opts Options) (*Module, error) {
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}

	// gin's debug mode prints a banner and the route table to stdout, behind the
	// structured logger this service owns (ADR-006). Routing, logging and recovery
	// are explicit in the intake, so release mode is the honest one.
	gin.SetMode(gin.ReleaseMode)

	producer, err := adapter.NewProducer(cfg.KafkaBrokers, cfg.Topic)
	if err != nil {
		return nil, fmt.Errorf("create producer: %w", err)
	}

	handler := intakehttp.NewServer(
		intakehttp.NewAuthenticator(cfg.JWTSecret, opts.Now),
		producer,
		intakehttp.Options{Now: opts.Now, MaxBatch: cfg.MaxBatch, Logger: log},
	)

	return &Module{
		cfg:      cfg,
		log:      log,
		producer: producer,
		server: &http.Server{
			Addr:              cfg.Addr,
			Handler:           handler,
			ReadHeaderTimeout: readHeaderTimeout,
		},
	}, nil
}

// Run serves until ctx is cancelled, then shuts down within shutdownTimeout. A
// cancelled context is a graceful stop and returns nil.
func (m *Module) Run(ctx context.Context) error {
	defer m.producer.Close()

	serveErr := make(chan error, 1)
	go func() {
		m.log.Info("intake listening",
			"service", serviceName, "addr", m.cfg.Addr, "topic", m.cfg.Topic, "brokers", m.cfg.KafkaBrokers)
		serveErr <- m.server.ListenAndServe()
	}()

	select {
	case err := <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve: %w", err)
		}
		return nil
	case <-ctx.Done():
		m.log.Info("intake shutting down", "service", serviceName)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := m.server.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	m.log.Info("intake stopped", "service", serviceName)
	return nil
}
