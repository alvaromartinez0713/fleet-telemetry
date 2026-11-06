package httpx

import (
	"errors"
	"log/slog"
	"net/http"
	"strings"
)

// HandlerFunc is a handler that reports a failure instead of rendering it.
type HandlerFunc func(http.ResponseWriter, *http.Request) error

// internalProblem is what an unclassified error renders: a generic 500 that can
// never carry internal text.
var internalProblem = Problem{
	Type:   "about:blank",
	Title:  "Internal Server Error",
	Status: http.StatusInternalServerError,
	Detail: "the request could not be completed",
}

// Errors is the single choke point for handler failures. It calls next, and if
// next returns an error it logs it and renders the problem exactly once.
//
// Two audiences, two levels: a 4xx is expected traffic and logs at WARN with the
// mapped detail, a 5xx (and any error that is not a ProblemError) logs at ERROR
// with the full unwrapped chain. An unclassified error always renders the
// generic 500; internal text never reaches the client. A response that was
// already written is logged and left alone, because a second write would corrupt
// it.
func Errors(log *slog.Logger, next HandlerFunc) http.Handler {
	if log == nil {
		log = slog.Default()
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := NewRecorder(w)
		err := next(rec, r)
		if err == nil {
			return
		}

		var problemErr *ProblemError
		if errors.As(err, &problemErr) {
			p := problemErr.Problem()
			attrs := requestAttrs(r, p)
			if cause := problemErr.Unwrap(); cause != nil {
				attrs = append(attrs, slog.String("cause", Chain(cause)))
			}
			level := slog.LevelWarn
			if p.Status >= http.StatusInternalServerError {
				level = slog.LevelError
			}
			log.LogAttrs(r.Context(), level, "request failed", attrs...)
			if !rec.Written() {
				WriteProblem(rec, p)
			}
			return
		}

		log.LogAttrs(r.Context(), slog.LevelError, "request failed",
			append(requestAttrs(r, internalProblem), slog.String("cause", Chain(err)))...)
		if !rec.Written() {
			WriteProblem(rec, internalProblem)
		}
	})
}

// requestAttrs are the facts every failure log line carries.
func requestAttrs(r *http.Request, p Problem) []slog.Attr {
	return []slog.Attr{
		slog.String("method", r.Method),
		slog.String("path", r.URL.Path),
		slog.Int("status", p.Status),
		slog.String("type", p.Type),
		slog.String("detail", p.Detail),
	}
}

// Chain joins every message of the unwrapped error chain so a log shows the
// whole cause while the response only ever carries the mapped detail. The depth
// is bounded so a self-wrapping error cannot loop.
func Chain(err error) string {
	if err == nil {
		return ""
	}
	const maxDepth = 16
	parts := make([]string, 0, 4)
	for e := err; e != nil && len(parts) < maxDepth; e = errors.Unwrap(e) {
		parts = append(parts, e.Error())
	}
	return strings.Join(parts, ": ")
}

// Recorder remembers whether the response was written and with which status, so
// a failure that happens after the headers went out is logged without being
// rendered a second time, and a middleware outside the handler can log the real
// status instead of assuming 200.
type Recorder struct {
	http.ResponseWriter
	status  int
	written bool
}

// NewRecorder wraps w. The returned Recorder forwards every write unchanged.
func NewRecorder(w http.ResponseWriter) *Recorder {
	return &Recorder{ResponseWriter: w}
}

// WriteHeader records the first status only; a later call cannot change a
// response that is already on the wire.
func (rec *Recorder) WriteHeader(status int) {
	if rec.written {
		return
	}
	rec.status, rec.written = status, true
	rec.ResponseWriter.WriteHeader(status)
}

// Write defaults the status to 200, exactly like net/http does.
func (rec *Recorder) Write(b []byte) (int, error) {
	if !rec.written {
		rec.WriteHeader(http.StatusOK)
	}
	return rec.ResponseWriter.Write(b)
}

// Status is the status that went out, or 200 when nothing was written: a handler
// that returns without writing sends an empty 200.
func (rec *Recorder) Status() int {
	if !rec.written {
		return http.StatusOK
	}
	return rec.status
}

// Written reports whether the response headers were already sent.
func (rec *Recorder) Written() bool { return rec.written }

// Unwrap exposes the wrapped writer to http.ResponseController.
func (rec *Recorder) Unwrap() http.ResponseWriter { return rec.ResponseWriter }

// Raw returns the writer underneath every Recorder in the chain. net/http's
// helpers type-assert on the original writer, and http.MaxBytesReader uses that
// assertion to ask the server to close the connection after an oversized body;
// handing it a wrapper would silently drop that signal.
func Raw(w http.ResponseWriter) http.ResponseWriter {
	for {
		unwrapper, ok := w.(interface{ Unwrap() http.ResponseWriter })
		if !ok {
			return w
		}
		w = unwrapper.Unwrap()
	}
}
