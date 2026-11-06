// Command live is the composition root of the live module (T4): it
// builds the module and starts it. Wiring only: no business logic.
package main

import (
	"context"
	"fmt"
	"os"

	"fleet-telemetry/internal/fleet/live"
)

func main() {
	module, err := live.New()
	if err != nil {
		fmt.Fprintf(os.Stderr, "live: %v\n", err)
		os.Exit(1)
	}

	if err := module.Run(context.Background()); err != nil {
		// The module is a skeleton: it reports what is still missing on stderr and
		// the process exits zero, like the placeholder binary it replaces.
		fmt.Fprintln(os.Stderr, err)
	}
}
