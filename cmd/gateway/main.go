// Command gateway is the public API: REST for portal queries, SSE for
// real-time streaming (ADR-001), and the synchronous call to the agent
// guarded by a circuit breaker (ADR-004).
package main

import (
	"fmt"
	"os"
)

const serviceName = "gateway"

func main() {
	fmt.Fprintf(os.Stderr, "%s: skeleton — not implemented yet (see odd/tasks/fleet-telemetry.md T4)\n", serviceName)
	os.Exit(0)
}
