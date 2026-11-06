package store_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"fleet-telemetry/internal/contracts"
	"fleet-telemetry/internal/store"
)

// timescaleImage matches deploy/docker-compose.yml so the integration test runs
// against the same engine as the local stack.
const timescaleImage = "timescale/timescaledb:latest-pg16"

// requireIntegration keeps the container test out of the default `go test ./...`
// run: it needs Docker and a pullable image.
func requireIntegration(t *testing.T) {
	t.Helper()
	if os.Getenv("RUN_INTEGRATION") != "1" {
		t.Skip("set RUN_INTEGRATION=1 to run against a real TimescaleDB container")
	}
}

func startStore(t *testing.T) *store.Store {
	t.Helper()

	// The reaper (ryuk) reads the Docker socket, which is root-owned on most
	// sandboxes, so it needs DAC override to start. It only watches container
	// lifecycle; the test's own cleanup still terminates the container below.
	t.Setenv("TESTCONTAINERS_RYUK_CONTAINER_PRIVILEGED", "true")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: timescaleImage,
			Env: map[string]string{
				"POSTGRES_USER":     "fleet",
				"POSTGRES_PASSWORD": "fleet",
				"POSTGRES_DB":       "fleet",
			},
			ExposedPorts: []string{"5432/tcp"},
			WaitingFor: wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(4 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start timescaledb container: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Logf("terminate timescaledb container: %v", err)
		}
	})

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("container host: %v", err)
	}
	port, err := container.MappedPort(ctx, "5432/tcp")
	if err != nil {
		t.Fatalf("container port: %v", err)
	}
	dsn := fmt.Sprintf("postgres://fleet:fleet@%s:%s/fleet?sslmode=disable", host, port.Port())

	st, err := store.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(st.Close)

	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return st
}

var base = time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)

// envelope builds a device event with a deterministic captured_at so the same
// seq can be replayed byte-identically, exactly like a mobile client retry.
func envelope(device string, seq uint64) contracts.Envelope {
	return contracts.Envelope{
		Event: contracts.Event{
			DeviceID:      device,
			Seq:           seq,
			CapturedAt:    base.Add(time.Duration(seq) * time.Second),
			Lat:           4.65,
			Lon:           -74.05,
			Mode:          contracts.ModeMoving,
			Traceparent:   "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
			SchemaVersion: contracts.SchemaVersion,
		},
		// A retry re-stamps received_at; captured_at stays immutable.
		ReceivedAt: base.Add(time.Hour),
	}
}

func TestPersistBatchIntegration(t *testing.T) {
	requireIntegration(t)

	st := startStore(t)
	ctx := context.Background()

	t.Run("replayed event is stored once and counted as a duplicate", func(t *testing.T) {
		device := "dev-dup"
		ev := envelope(device, 1)

		first, err := st.PersistBatch(ctx, []contracts.Envelope{ev})
		if err != nil {
			t.Fatalf("first persist: %v", err)
		}
		if first.Inserted != 1 || first.Duplicates != 0 {
			t.Fatalf("first persist = inserted %d, duplicates %d; want 1 and 0", first.Inserted, first.Duplicates)
		}

		second, err := st.PersistBatch(ctx, []contracts.Envelope{ev})
		if err != nil {
			t.Fatalf("replay persist: %v", err)
		}
		if second.Inserted != 0 || second.Duplicates != 1 {
			t.Fatalf("replay persist = inserted %d, duplicates %d; want 0 and 1", second.Inserted, second.Duplicates)
		}

		count, err := st.Count(ctx, device)
		if err != nil {
			t.Fatalf("count: %v", err)
		}
		if count != 1 {
			t.Fatalf("stored %d rows for %s, want 1", count, device)
		}
	})

	t.Run("watermark advances over a contiguous run", func(t *testing.T) {
		device := "dev-fifo"

		res, err := st.PersistBatch(ctx, []contracts.Envelope{envelope(device, 1), envelope(device, 2)})
		if err != nil {
			t.Fatalf("persist: %v", err)
		}
		if got, ok := res.Advanced[device]; !ok || got != 2 {
			t.Fatalf("advanced = %v, want %s -> 2", res.Advanced, device)
		}

		wm, err := st.Watermark(ctx, device)
		if err != nil {
			t.Fatalf("watermark: %v", err)
		}
		if wm != 2 {
			t.Fatalf("watermark = %d, want 2", wm)
		}
	})

	t.Run("gap blocks the watermark until the missing seq arrives", func(t *testing.T) {
		device := "dev-gap"

		res, err := st.PersistBatch(ctx, []contracts.Envelope{envelope(device, 1), envelope(device, 3)})
		if err != nil {
			t.Fatalf("persist: %v", err)
		}
		// Advance only moves on a complete run, so a gap freezes the watermark
		// at 0 instead of claiming seq 1..1 is drained: conservative on
		// purpose, the client never deletes a row the server cannot prove.
		if next, moved := res.Advanced[device]; moved {
			t.Fatalf("watermark moved to %d while seq 2 is missing, want no advance", next)
		}
		wm, err := st.Watermark(ctx, device)
		if err != nil {
			t.Fatalf("watermark: %v", err)
		}
		if wm != 0 {
			t.Fatalf("watermark = %d, want 0 while seq 2 is missing", wm)
		}

		res, err = st.PersistBatch(ctx, []contracts.Envelope{envelope(device, 2)})
		if err != nil {
			t.Fatalf("persist missing seq: %v", err)
		}
		if got, ok := res.Advanced[device]; !ok || got != 3 {
			t.Fatalf("advanced = %v, want %s -> 3 once the gap closes", res.Advanced, device)
		}
		if wm, err = st.Watermark(ctx, device); err != nil {
			t.Fatalf("watermark: %v", err)
		}
		if wm != 3 {
			t.Fatalf("watermark = %d, want 3", wm)
		}
	})

	t.Run("watermark never regresses", func(t *testing.T) {
		device := "dev-mono"

		if _, err := st.PersistBatch(ctx, []contracts.Envelope{
			envelope(device, 1), envelope(device, 2), envelope(device, 3),
		}); err != nil {
			t.Fatalf("persist: %v", err)
		}

		res, err := st.PersistBatch(ctx, []contracts.Envelope{envelope(device, 2), envelope(device, 1)})
		if err != nil {
			t.Fatalf("replay persist: %v", err)
		}
		if next, moved := res.Advanced[device]; moved {
			t.Fatalf("watermark moved to %d on a replay, want no advance", next)
		}
		wm, err := st.Watermark(ctx, device)
		if err != nil {
			t.Fatalf("watermark: %v", err)
		}
		if wm != 3 {
			t.Fatalf("watermark = %d after replay, want 3", wm)
		}
	})

	t.Run("watermark of an unknown device is zero", func(t *testing.T) {
		wm, err := st.Watermark(ctx, "dev-unknown")
		if err != nil {
			t.Fatalf("watermark: %v", err)
		}
		if wm != 0 {
			t.Fatalf("watermark = %d, want 0 for a device with no row", wm)
		}
	})
}
