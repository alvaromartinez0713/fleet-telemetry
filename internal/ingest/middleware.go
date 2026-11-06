package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"

	"fleet-telemetry/internal/platform/httpx"
)

// Middleware wraps http.Handler and returns http.Handler: each one owns exactly
// one cross-cutting concern and none of them knows what telemetry is. The order
// they are applied in is decided and documented in NewServer.

// deviceScopeKey is the request context key of the *deviceScope holder.
type deviceScopeKey struct{}

// deviceScope is the per-request holder of the authenticated device id. A
// pointer in the context is what lets the request logger, which wraps the
// authenticator, report the device the inner middleware resolved.
type deviceScope struct{ id string }

// withDeviceScope installs an empty holder for the authenticator to fill.
func withDeviceScope(ctx context.Context) context.Context {
	return context.WithValue(ctx, deviceScopeKey{}, &deviceScope{})
}

// withDeviceID records the authenticated device, updating the holder the request
// logger installed. Without one it stores a fresh holder, so the chain still
// works when a middleware is used on its own.
func withDeviceID(ctx context.Context, id string) context.Context {
	if scope, ok := ctx.Value(deviceScopeKey{}).(*deviceScope); ok {
		scope.id = id
		return ctx
	}
	return context.WithValue(ctx, deviceScopeKey{}, &deviceScope{id: id})
}

// deviceIDFrom reads the authenticated device id of the request.
func deviceIDFrom(ctx context.Context) string {
	if scope, ok := ctx.Value(deviceScopeKey{}).(*deviceScope); ok {
		return scope.id
	}
	return ""
}

// authenticate validates the device bearer token (ADR-003) and puts the device
// id in the request context, or answers 401 with a problem document. It never
// looks at the body: an unauthenticated caller is rejected before its bytes are
// read or counted.
func authenticate(auth *Authenticator, log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		deviceID, err := auth.DeviceID(r.Header.Get("Authorization"))
		if err != nil {
			if errors.Is(err, ErrMissingToken) {
				httpx.WriteProblem(w, httpx.Problem{
					Type:   problemUnauthorized,
					Title:  "Missing bearer token",
					Status: http.StatusUnauthorized,
					Detail: "an Authorization: Bearer <token> header is required",
				})
				return
			}
			log.WarnContext(r.Context(), "rejected device token", "error", err)
			httpx.WriteProblem(w, httpx.Problem{
				Type:   problemUnauthorized,
				Title:  "Invalid bearer token",
				Status: http.StatusUnauthorized,
				Detail: "the device token is invalid or expired",
			})
			return
		}
		next.ServeHTTP(w, r.WithContext(withDeviceID(r.Context(), deviceID)))
	})
}

// limitBody bounds how many bytes a request may present. The overflow is not
// classified here: it surfaces as *http.MaxBytesError when the body is read, and
// the reader of the body is the one that knows which problem it means.
func limitBody(maxBytes int64, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(httpx.Raw(w), r.Body, maxBytes)
		next.ServeHTTP(w, r)
	})
}

// recoverer turns a panic into a 500 problem plus one log line with the stack,
// and re-panics http.ErrAbortHandler because net/http uses it to abort silently.
func recoverer(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := httpx.NewRecorder(w)
		defer func() {
			recovered := recover()
			if recovered == nil {
				return
			}
			if recovered == http.ErrAbortHandler {
				panic(recovered)
			}
			log.ErrorContext(r.Context(), "panic serving request",
				"method", r.Method,
				"path", r.URL.Path,
				"panic", fmt.Sprint(recovered),
				"stack", string(debug.Stack()),
			)
			if !rec.Written() {
				httpx.WriteProblem(rec, httpx.Problem{
					Type:   problemInternal,
					Title:  "Internal Server Error",
					Status: http.StatusInternalServerError,
					Detail: "the request could not be completed",
				})
			}
		}()
		next.ServeHTTP(rec, r)
	})
}

// requestLogger writes exactly one INFO line per request with the facts that
// identify it: method, path, status, latency and the authenticated device (empty
// when the request never got that far).
func requestLogger(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ctx := withDeviceScope(r.Context())
		rec := httpx.NewRecorder(w)

		next.ServeHTTP(rec, r.WithContext(ctx))

		log.LogAttrs(ctx, slog.LevelInfo, "http request",
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.Int("status", rec.Status()),
			slog.Duration("latency", time.Since(start)),
			slog.String("device", deviceIDFrom(ctx)),
		)
	})
}
