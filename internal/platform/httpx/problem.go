// Package httpx holds the HTTP concerns every service shares: the RFC 9457
// problem document, the error that carries one, and the single choke point that
// turns a returned error into exactly one rendered response.
package httpx

import (
	"encoding/json"
	"net/http"
)

// Problem is an RFC 9457 problem document. The type URI identifies the failure
// class, so a client can branch on it instead of parsing prose. EventIndex is an
// extension member: it points at the offending event of a batch so a client can
// drop just that one.
type Problem struct {
	Type       string `json:"type"`
	Title      string `json:"title"`
	Status     int    `json:"status"`
	Detail     string `json:"detail"`
	EventIndex *int   `json:"event_index,omitempty"`
}

// WriteProblem renders p as application/problem+json. It is the only writer of
// that content type, so every failure looks the same to a client.
func WriteProblem(w http.ResponseWriter, p Problem) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(p.Status)
	_ = json.NewEncoder(w).Encode(p)
}

// ProblemError is an error that already knows which problem the client should
// see. It has two audiences on purpose: Error() is the user-safe, already mapped
// detail and the only text a response may carry, while the internal cause
// travels through Unwrap() and reaches only the logs.
type ProblemError struct {
	problem Problem
	cause   error
}

// NewProblemError pairs a ready-to-render problem with the cause that explains
// it internally. A nil cause means the failure needs no further evidence.
func NewProblemError(p Problem, cause error) *ProblemError {
	return &ProblemError{problem: p, cause: cause}
}

// Problem returns the document to render. It is safe for the client: every
// field was written by a mapper, never copied from an internal error.
func (e *ProblemError) Problem() Problem { return e.problem }

// Error is the user-safe detail, so a ProblemError can be logged on its own
// without leaking internals. The cause is deliberately not included.
func (e *ProblemError) Error() string { return e.problem.Detail }

// Unwrap exposes the internal cause for the logs. Rendering never calls it.
func (e *ProblemError) Unwrap() error { return e.cause }
