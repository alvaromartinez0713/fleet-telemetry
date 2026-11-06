package intakehttp

import (
	"errors"
	"fmt"
	"net/http"

	"fleet-telemetry/internal/platform/httpx"
	"fleet-telemetry/internal/telemetry/intake/app"
)

// Problem type URIs of this module. They identify the failure class, so a client
// can branch on the type instead of parsing prose (RFC 9457).
const (
	problemUnauthorized   = "urn:fleet:ingest:unauthorized"
	problemInvalidBatch   = "urn:fleet:ingest:invalid-batch"
	problemDeviceMismatch = "urn:fleet:ingest:device-mismatch"
	problemBodyTooLarge   = "urn:fleet:ingest:body-too-large"
	problemBusUnavailable = "urn:fleet:ingest:bus-unavailable"
	problemInternal       = "urn:fleet:ingest:internal"
)

// problemFor translates a use-case rejection into the problem document the
// client sees. The class chooses type, title and status; the detail and the
// cause come from the rejection, so the text the client reads is written once,
// where the rule that produced it lives.
//
// The batch-level and event-level contract failures differ only by the index,
// which is also the RFC 9457 event_index extension member: a client can drop
// just that one event.
func problemFor(err error) error {
	var rejection *app.Rejection
	if !errors.As(err, &rejection) {
		// Nothing known produced this: the choke point renders the generic 500,
		// which is the safe answer for text we cannot vouch for.
		return err
	}

	p := httpx.Problem{Detail: rejection.Detail}
	switch rejection.Kind {
	case app.KindBatchTooLarge:
		p.Type, p.Title, p.Status = problemInvalidBatch, "Telemetry batch too large", http.StatusBadRequest
	case app.KindInvalidBatch:
		p.Type, p.Status = problemInvalidBatch, http.StatusBadRequest
		p.Title = "Invalid telemetry batch"
		if rejection.Index >= 0 {
			p.Title = "Invalid telemetry event"
		}
	case app.KindDeviceMismatch:
		p.Type, p.Title, p.Status = problemDeviceMismatch, "Device mismatch", http.StatusForbidden
	case app.KindBusUnavailable:
		p.Type, p.Title, p.Status = problemBusUnavailable, "Event bus unavailable", http.StatusBadGateway
	default:
		return fmt.Errorf("unmapped rejection kind %d: %w", rejection.Kind, err)
	}

	if rejection.Index >= 0 {
		index := rejection.Index
		p.EventIndex = &index
	}
	return httpx.NewProblemError(p, rejection.Cause)
}

// failure builds a ProblemError for one of the failure classes the HTTP edge
// owns: the body limit and the request body it reads. The use case's classes go
// through problemFor instead.
func failure(kind, title string, status int, detail string, cause error) *httpx.ProblemError {
	return httpx.NewProblemError(httpx.Problem{Type: kind, Title: title, Status: status, Detail: detail}, cause)
}
