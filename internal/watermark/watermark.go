// Package watermark implements the cumulative FIFO watermark that tells a
// device which of its local events are already durable (ADR-003).
//
// The watermark W for a device is the largest seq such that every event with
// seq in 1..W is persisted. "Highest seq received" is deliberately not
// implemented: with out-of-order arrival or a lost batch it would delete local
// events the server never stored.
package watermark

// Advance returns the new watermark for a device.
//
// current is the stored watermark, maxSeq is the highest seq persisted for the
// device, and present is how many events with seq > current exist. The run is
// contiguous exactly when the count matches the span maxSeq-current, and only
// then may the watermark move. The value never decreases.
func Advance(current, maxSeq uint64, present int64) uint64 {
	if present <= 0 || maxSeq <= current {
		return current
	}
	span := maxSeq - current
	if uint64(present) != span {
		return current
	}
	return maxSeq
}
