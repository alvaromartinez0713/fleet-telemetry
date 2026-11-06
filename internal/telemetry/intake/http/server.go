// Package intakehttp is the intake module's HTTP edge: it authenticates the
// device, decodes the batch against the shared contract and hands it to the use
// case in internal/telemetry/intake/app. Contract rules live in
// internal/contracts; this package only translates HTTP into that contract and
// back.
//
// The router is gin, the one HTTP idiom of the repository. Each cross-cutting
// concern is its own middleware (auth.go, middleware.go), the use case is
// reached through app.Submit (telemetry.go), and the package only builds the
// chain and maps a use-case rejection to a problem document (problems.go).
package intakehttp

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"fleet-telemetry/internal/contracts"
	"fleet-telemetry/internal/platform/httpx"
	"fleet-telemetry/internal/telemetry/intake/app"
)

// maxBodyBytes bounds one request body. A batch of MaxBatchSize events fits
// comfortably; anything larger is a client bug, not telemetry we owe storage.
const maxBodyBytes = 1 << 20 // 1 MiB

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

// Server holds what the HTTP edge needs and nothing else: no logger, no writer,
// and no business rule, which lives in the use case.
type Server struct {
	submit   *app.Submit
	maxBytes int64
}

// NewServer builds the handler. The engine owns routing, so method and path
// mismatches are answered by gin (405/404) and never by hand-rolled code.
func NewServer(auth *Authenticator, producer app.Producer, opts Options) http.Handler {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.MaxBodyBytes <= 0 {
		opts.MaxBodyBytes = maxBodyBytes
	}
	if opts.MaxBatch > contracts.MaxBatchSize {
		opts.Logger.Warn("configured batch limit exceeds the wire contract, using the contract limit",
			"configured", opts.MaxBatch, "contract", contracts.MaxBatchSize)
	}

	s := &Server{
		submit:   app.NewSubmit(producer, app.Options{Now: opts.Now, MaxBatch: opts.MaxBatch}),
		maxBytes: opts.MaxBodyBytes,
	}

	// gin.New, never gin.Default: logging and recovery belong to the two
	// single-responsibility middlewares below (ADR-006), and gin.Default would
	// install a second copy of both.
	engine := gin.New()

	// Routing stays a routing answer. HandleMethodNotAllowed makes the engine answer
	// a wrong method itself, through the NoMethod chain, which is the one chain that
	// runs without the route middlewares: the 405 needs no token, exactly as it did
	// when the mux owned routing. NoMethod and NoRoute restore net/http's plain text
	// answer, which is what this endpoint returned before the migration.
	engine.HandleMethodNotAllowed = true
	engine.NoMethod(methodNotAllowed)
	engine.NoRoute(notFound)

	// Global chain, in this order:
	//
	//   - recoverer is outermost: a panic has to be contained before anything else
	//     looks at the request, and before the response is touched.
	//   - requestLogger is just inside it, so it observes the status every layer
	//     below produces (405, 401, 202, 500) and the device id that authenticate
	//     resolved.
	engine.Use(recoverer(opts.Logger), requestLogger(opts.Logger))

	// Route chain, in this order:
	//
	//   - authenticate sits outside limitBody so an unauthenticated caller is
	//     rejected before its body is read or counted.
	//   - the choke point is innermost, wrapping the handler: the use case reports
	//     failures with c.Error, and this is the only place that renders one.
	engine.POST(contracts.RouteTelemetry,
		authenticate(auth, opts.Logger),
		limitBody(s.maxBytes),
		httpx.GinErrors(opts.Logger),
		s.handleTelemetry,
	)

	return engine
}

// methodNotAllowed keeps the routing answer net/http gave before the migration: a
// wrong method is answered without a token, with the standard text body and the
// Allow header the engine sets. It is a routing answer, not a problem document,
// so no adapter rewrites it.
func methodNotAllowed(c *gin.Context) {
	http.Error(c.Writer, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
}

// notFound does the same for an unknown path: net/http's plain 404 body instead
// of gin's, which has no trailing newline.
func notFound(c *gin.Context) {
	http.NotFound(c.Writer, c.Request)
}
