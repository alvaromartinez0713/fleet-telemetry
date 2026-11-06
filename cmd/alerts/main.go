// Command alerts evaluates rules over vehicle state, e.g. "vehicle stopped
// more than 20 minutes in a critical zone" (feature doc S4/ADR-011: movement /
// stopped-with-heartbeat / no-signal states).
package main

import (
	"fmt"
	"os"
)

const serviceName = "alerts"

func main() {
	fmt.Fprintf(os.Stderr, "%s: skeleton — not implemented yet (see odd/tasks/fleet-telemetry.md T7)\n", serviceName)
	os.Exit(0)
}
