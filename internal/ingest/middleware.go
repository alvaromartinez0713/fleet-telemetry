package ingest

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/gin-gonic/gin"

	"fleet-telemetry/internal/platform/httpx"
)

// Each middleware is a gin.HandlerFunc that owns exactly one cross-cutting
// concern and none of them knows what telemetry is. The order they run in is
// decided and documented in NewServer.

// deviceContextKey is the gin context key of the authenticated device id. The
// gin context is per request and shared by the whole chain, so the request
// logger, which runs outside the authenticator, reads the id the authenticator
// stored.
const deviceContextKey = "fleet.device_id"

// authenticate validates the device bearer token (ADR-003) and puts the device
// id in the request context, or answers 401 with a problem document. It never
// looks at the body: an unauthenticated caller is rejected before its bytes are
// read or counted.
func authenticate(auth *Authenticator, log *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		deviceID, err := auth.DeviceID(c.GetHeader("Authorization"))
		if err != nil {
			if errors.Is(err, ErrMissingToken) {
				abortProblem(c, httpx.Problem{
					Type:   problemUnauthorized,
					Title:  "Missing bearer token",
					Status: http.StatusUnauthorized,
					Detail: "an Authorization: Bearer <token> header is required",
				})
				return
			}
			log.WarnContext(c.Request.Context(), "rejected device token", "error", err)
			abortProblem(c, httpx.Problem{
				Type:   problemUnauthorized,
				Title:  "Invalid bearer token",
				Status: http.StatusUnauthorized,
				Detail: "the device token is invalid or expired",
			})
			return
		}
		c.Set(deviceContextKey, deviceID)
		c.Next()
	}
}

// limitBody bounds how many bytes a request may present. The overflow is not
// classified here: it surfaces as *http.MaxBytesError when the body is read, and
// the reader of the body is the one that knows which problem it means.
func limitBody(maxBytes int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		// The reader needs the original writer underneath gin's, not gin's
		// wrapper, or an oversized body loses the close-connection signal.
		c.Request.Body = http.MaxBytesReader(httpx.Raw(c.Writer), c.Request.Body, maxBytes)
		c.Next()
	}
}

// recoverer turns a panic into a 500 problem plus one log line with the stack,
// and re-panics http.ErrAbortHandler because net/http uses it to abort silently.
func recoverer(log *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			recovered := recover()
			if recovered == nil {
				return
			}
			if recovered == http.ErrAbortHandler {
				panic(recovered)
			}
			log.ErrorContext(c.Request.Context(), "panic serving request",
				"method", c.Request.Method,
				"path", c.Request.URL.Path,
				"panic", fmt.Sprint(recovered),
				"stack", string(debug.Stack()),
			)
			if !c.Writer.Written() {
				httpx.WriteProblem(c.Writer, httpx.Problem{
					Type:   problemInternal,
					Title:  "Internal Server Error",
					Status: http.StatusInternalServerError,
					Detail: "the request could not be completed",
				})
			}
			// The panic unwound gin's Next loop; without an abort the engine
			// would keep running the handlers after the one that panicked.
			c.Abort()
		}()
		c.Next()
	}
}

// requestLogger writes exactly one INFO line per request with the facts that
// identify it: method, path, status, latency and the authenticated device (empty
// when the request never got that far).
func requestLogger(log *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()

		c.Next()

		log.LogAttrs(c.Request.Context(), slog.LevelInfo, "http request",
			slog.String("method", c.Request.Method),
			slog.String("path", c.Request.URL.Path),
			slog.Int("status", c.Writer.Status()),
			slog.Duration("latency", time.Since(start)),
			slog.String("device", deviceIDFrom(c)),
		)
	}
}

// abortProblem renders p and stops the chain. A middleware that owns a single
// failure of its own (the 401 here, the 500 of a recovered panic) has nothing to
// delegate to the choke point, so it answers that one problem itself.
func abortProblem(c *gin.Context, p httpx.Problem) {
	httpx.WriteProblem(c.Writer, p)
	c.Abort()
}

// deviceIDFrom reads the authenticated device id the authenticate middleware
// stored, or "" when the request never got that far.
func deviceIDFrom(c *gin.Context) string {
	value, ok := c.Get(deviceContextKey)
	if !ok {
		return ""
	}
	id, _ := value.(string)
	return id
}
