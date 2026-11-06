package httpx

import (
	"errors"
	"log/slog"
	"net/http"
	"strings"
)

// internalProblem is what an unclassified error renders: a generic 500 that can
// never carry internal text.
var internalProblem = Problem{
	Type:   "about:blank",
	Title:  "Internal Server Error",
	Status: http.StatusInternalServerError,
	Detail: "the request could not be completed",
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

// Raw returns the writer underneath every wrapper in the chain. net/http's
// helpers type-assert on the original writer, and http.MaxBytesReader uses that
// assertion to ask the server to close the connection after an oversized body;
// handing it a wrapper such as gin's would silently drop that signal.
func Raw(w http.ResponseWriter) http.ResponseWriter {
	for {
		unwrapper, ok := w.(interface{ Unwrap() http.ResponseWriter })
		if !ok {
			return w
		}
		w = unwrapper.Unwrap()
	}
}
