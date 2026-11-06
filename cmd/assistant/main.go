// Command assistant is the composition root of the assistant module (T6): it
// builds the module and starts it. Wiring only: no business logic.
package main

import (
	"context"
	"fmt"
	"os"

	"fleet-telemetry/internal/fleet/assistant"
)

func main() {
	module, err := assistant.New()
	if err != nil {
		fmt.Fprintf(os.Stderr, "assistant: %v\n", err)
		os.Exit(1)
	}

	if err := module.Run(context.Background()); err != nil {
		// The module is a skeleton: it reports what is still missing on stderr and
		// the process exits zero, like the placeholder binary it replaces.
		fmt.Fprintln(os.Stderr, err)
	}
}
