package adapter

import (
	"time"

	"fleet-telemetry/internal/contracts"
)

// batchColumns turns a batch of envelopes into the parallel arrays the UNNEST
// insert consumes. Keeping it in one place makes the column order of the SQL
// and the Go side impossible to drift apart.
type batchColumns struct {
	deviceIDs      []string
	seqs           []int64
	capturedAts    []time.Time
	receivedAts    []time.Time
	lats           []float64
	lons           []float64
	speeds         []*float64
	headings       []*float64
	accuracies     []*float64
	modes          []string
	traceparents   []*string
	schemaVersions []int32

	order []string
	seen  map[string]struct{}
}

func newBatchColumns(size int) *batchColumns {
	return &batchColumns{
		deviceIDs:      make([]string, 0, size),
		seqs:           make([]int64, 0, size),
		capturedAts:    make([]time.Time, 0, size),
		receivedAts:    make([]time.Time, 0, size),
		lats:           make([]float64, 0, size),
		lons:           make([]float64, 0, size),
		speeds:         make([]*float64, 0, size),
		headings:       make([]*float64, 0, size),
		accuracies:     make([]*float64, 0, size),
		modes:          make([]string, 0, size),
		traceparents:   make([]*string, 0, size),
		schemaVersions: make([]int32, 0, size),
		seen:           make(map[string]struct{}, size),
	}
}

func (c *batchColumns) add(e contracts.Envelope) {
	c.deviceIDs = append(c.deviceIDs, e.DeviceID)
	c.seqs = append(c.seqs, int64(e.Seq))
	c.capturedAts = append(c.capturedAts, e.CapturedAt.UTC())
	c.receivedAts = append(c.receivedAts, e.ReceivedAt.UTC())
	c.lats = append(c.lats, e.Lat)
	c.lons = append(c.lons, e.Lon)
	c.speeds = append(c.speeds, e.SpeedKmh)
	c.headings = append(c.headings, e.HeadingDeg)
	c.accuracies = append(c.accuracies, e.AccuracyM)
	c.modes = append(c.modes, string(e.Mode))
	c.traceparents = append(c.traceparents, nullable(e.Traceparent))
	c.schemaVersions = append(c.schemaVersions, int32(e.SchemaVersion))

	if _, ok := c.seen[e.DeviceID]; !ok {
		c.seen[e.DeviceID] = struct{}{}
		c.order = append(c.order, e.DeviceID)
	}
}

// devices lists the devices of the batch once, in first-seen order.
func (c *batchColumns) devices() []string { return c.order }

func nullable(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
