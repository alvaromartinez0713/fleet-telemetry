package app

// NoIndex is the Rejection index of a failure that belongs to the batch itself
// rather than to one of its events.
const NoIndex = -1

// Kind is the failure class of a rejected batch.
type Kind int

const (
	// KindBatchTooLarge is a batch above the configured limit.
	KindBatchTooLarge Kind = iota + 1
	// KindInvalidBatch is a batch, or one of its events, that violates the wire
	// contract.
	KindInvalidBatch
	// KindDeviceMismatch is an event that belongs to a device other than the
	// authenticated one.
	KindDeviceMismatch
	// KindBusUnavailable is a batch that could not be queued.
	KindBusUnavailable
)

// Rejection is the use case's answer when it refuses a batch. It carries the
// failure class, the index of the offending event, the detail a client may see
// and the internal cause that belongs in the logs.
//
// It deliberately is not a problem document: the HTTP layer maps the class to
// one (RFC 9457), so the core never depends on the transport. Two audiences stay
// separated the same way they are in the transport: Error() and Detail are the
// user-safe text, Cause travels only through Unwrap into the logs.
type Rejection struct {
	Kind   Kind
	Index  int
	Detail string
	Cause  error
}

// Error is the user-safe detail, so a Rejection can be logged on its own without
// leaking internals.
func (r *Rejection) Error() string { return r.Detail }

// Unwrap exposes the internal cause for the logs. Rendering never calls it.
func (r *Rejection) Unwrap() error { return r.Cause }
