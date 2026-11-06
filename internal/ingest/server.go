// Package ingest is the HTTP intake adapter: it authenticates the device,
// validates the batch against the shared contract, stamps received_at and hands
// the batch to the bus. Contract rules live in internal/contracts; this package
// only translates HTTP into that contract and back.
//
// Each cross-cutting concern is its own middleware (auth.go, middleware.go) and
// the use case (telemetry.go) contains no plumbing. This file only builds the
// handler.
package ingest

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"fleet-telemetry/internal/contracts"
	"fleet-telemetry/internal/platform/httpx"
)

// maxBodyBytes bounds one request body. A batch of MaxBatchSize events fits
// comfortably; anything larger is a client bug, not telemetry we owe storage.
const maxBodyBytes = 1 << 20 // 1 MiB

// Problem type URIs. They identify the failure class, so a client can branch on
// the type instead of parsing prose (RFC 9457).
const (
	problemUnauthorized   = "urn:fleet:ingest:unauthorized"
	problemInvalidBatch   = "urn:fleet:ingest:invalid-batch"
	problemDeviceMismatch = "urn:fleet:ingest:device-mismatch"
	problemBodyTooLarge   = "urn:fleet:ingest:body-too-large"
	problemBusUnavailable = "urn:fleet:ingest:bus-unavailable"
	problemInternal       = "urn:fleet:ingest:internal"
)

// Producer queues the accepted batch on the bus. *bus.Producer implements it.
type Producer interface {
	Produce(context.Context, []contracts.Envelope) error
}

// Options are the intake's boundaries. Zero values fall back to the defaults.
type Options struct {
	// Now stamps received_at and bounds the accepted device clock skew.
	Now func() time.Time
	// MaxBodyBytes bounds the request body.
	MaxBodyBytes int64
	// MaxBatch bounds how many events one request may carry. Zero falls back to
	// contracts.MaxBatchSize; an operator-configured limit lowers it, never
	// raises it above the wire contract.
	MaxBatch int
	// Logger receives the request line, the failure lines and the panic stacks.
	Logger *slog.Logger
}

// Server holds what the use case needs and nothing else: no logger, no writer,
// no knowledge of the middleware around it.
type Server struct {
	producer Producer
	now      func() time.Time
	maxBytes int64
	maxBatch int
}

// NewServer builds the handler. The mux owns routing, so method and path
// mismatches are answered by net/http (405/404) and never by hand-rolled code.
func NewServer(auth *Authenticator, producer Producer, opts Options) http.Handler {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.MaxBodyBytes <= 0 {
		opts.MaxBodyBytes = maxBodyBytes
	}
	if opts.MaxBatch <= 0 || opts.MaxBatch > contracts.MaxBatchSize {
		if opts.MaxBatch > contracts.MaxBatchSize {
			opts.Logger.Warn("configured batch limit exceeds the wire contract, using the contract limit",
				"configured", opts.MaxBatch, "contract", contracts.MaxBatchSize)
		}
		opts.MaxBatch = contracts.MaxBatchSize
	}

	s := &Server{
		producer: producer,
		now:      opts.Now,
		maxBytes: opts.MaxBodyBytes,
		maxBatch: opts.MaxBatch,
	}

	// Per-route chain, applied innermost first: the choke point, then the body
	// limit, then authentication. The choke point is applied first because the use
	// case returns errors instead of writing problems, and it is the only place
	// that renders one.
	//
	// authenticate sits outside limitBody so an unauthenticated caller is rejected
	// before its body is read or counted. A middleware that has a single failure of
	// its own (the 401 here, the 500 of a recovered panic) writes that one problem
	// directly; it has nothing to delegate.
	var route http.Handler = httpx.Errors(opts.Logger, s.handleTelemetry)
	route = limitBody(s.maxBytes, route)
	route = authenticate(auth, opts.Logger, route)

	// The mux owns routing and therefore sits outside the route chain: a method or
	// path mismatch is answered by net/http (405/404) before any identity check,
	// which is the only way that answer stays a routing answer.
	mux := http.NewServeMux()
	mux.Handle("POST "+contracts.RouteTelemetry, route)

	// Global chain, applied innermost first, which together with the route chain
	// gives this order from the outside: recoverer → requestLogger → mux →
	// authenticate → limitBody → choke point → use case.
	//
	//   - recoverer is outermost: a panic has to be contained before anything else
	//     looks at the request, and before the response is touched.
	//   - requestLogger is just inside it, so it observes the status every layer
	//     below produces (405, 401, 202, 500) and the device id that authenticate
	//     resolved.
	var handler http.Handler = mux
	handler = requestLogger(opts.Logger, handler)
	handler = recoverer(opts.Logger, handler)
	return handler
}
