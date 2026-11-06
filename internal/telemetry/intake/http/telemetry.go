package intakehttp

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"

	"fleet-telemetry/internal/contracts"
)

// accepted is the 202 body: it counts what was durably queued, never what was
// persisted (ADR-002).
type accepted struct {
	Accepted int `json:"accepted"`
}

// handleTelemetry is the HTTP edge of the intake use case. The device id arrives
// from the authentication middleware and the bounded body from the body-limit
// middleware; what is left here is the transport half: decode the body, call the
// use case, and answer with what it queued.
//
// Every failure is reported with c.Error and the handler returns: the choke
// point is the only place that renders a problem, so this reads as a handler
// instead of as plumbing.
func (s *Server) handleTelemetry(c *gin.Context) {
	batch, err := decodeBatch(c.Request.Body, s.maxBytes)
	if err != nil {
		c.Error(err)
		return
	}

	queued, err := s.submit.Submit(c.Request.Context(), deviceIDFrom(c), batch)
	if err != nil {
		c.Error(problemFor(err))
		return
	}

	c.Header("Content-Type", "application/json")
	c.Writer.WriteHeader(http.StatusAccepted)
	if err := json.NewEncoder(c.Writer).Encode(accepted{Accepted: queued}); err != nil {
		// The 202 is already on the wire: the choke point logs this and does not
		// try to render a problem over it.
		c.Error(fmt.Errorf("write accepted response: %w", err))
	}
}

// decodeBatch reads the request body as a telemetry batch and translates the two
// ways a body can fail into the problem the client should see: an overflow of the
// body limit becomes a 413, anything else a 400. Both travel as ProblemError, so
// the shared helper keeps the rendering out of the handler.
func decodeBatch(body io.Reader, maxBytes int64) (contracts.Batch, error) {
	var batch contracts.Batch
	err := json.NewDecoder(body).Decode(&batch)
	if err == nil {
		return batch, nil
	}

	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		return contracts.Batch{}, failure(problemBodyTooLarge, "Request body too large",
			http.StatusRequestEntityTooLarge, fmt.Sprintf("the body exceeds the %d byte limit", maxBytes), err)
	}
	return contracts.Batch{}, failure(problemInvalidBatch, "Invalid request body",
		http.StatusBadRequest, "the body is not a valid telemetry batch", err)
}
