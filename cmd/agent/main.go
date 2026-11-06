// Command agent implements the AI operator: a direct function-calling loop in
// Go against the LLM API (ADR-014, no LangChain), with 8-10 tools including
// salud_pipeline() to answer natural-language queries about fleet state.
package main

import (
	"fmt"
	"os"
)

const serviceName = "agent"

func main() {
	fmt.Fprintf(os.Stderr, "%s: skeleton — not implemented yet (see odd/tasks/fleet-telemetry.md T6)\n", serviceName)
	os.Exit(0)
}
