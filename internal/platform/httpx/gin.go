package httpx

import (
	"errors"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"
)

// GinErrors is the single choke point for handler failures in a gin chain. It
// runs the rest of the chain and then renders the first error a handler reported
// with c.Error, exactly once.
//
// Two audiences, two levels: a 4xx is expected traffic and logs at WARN with the
// mapped detail, a 5xx (and any error that is not a ProblemError) logs at ERROR
// with the full unwrapped chain. An unclassified error always renders the
// generic 500; internal text never reaches the client. A response that was
// already written is logged and left alone, because a second write would corrupt
// it.
func GinErrors(log *slog.Logger) gin.HandlerFunc {
	if log == nil {
		log = slog.Default()
	}
	return func(c *gin.Context) {
		c.Next()
		if len(c.Errors) == 0 {
			return
		}

		r := c.Request
		err := c.Errors[0].Err

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
			if !c.Writer.Written() {
				WriteProblem(c.Writer, p)
			}
			return
		}

		log.LogAttrs(r.Context(), slog.LevelError, "request failed",
			append(requestAttrs(r, internalProblem), slog.String("cause", Chain(err)))...)
		if !c.Writer.Written() {
			WriteProblem(c.Writer, internalProblem)
		}
	}
}
