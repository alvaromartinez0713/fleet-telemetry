package intakehttp_test

import (
	"os"
	"testing"

	"github.com/gin-gonic/gin"
)

// TestMain puts the router in test mode for the whole package: gin's debug mode
// prints a banner and the route table to stdout, which would sit between the
// test output and the log lines the tests assert on.
func TestMain(m *testing.M) {
	gin.SetMode(gin.TestMode)
	os.Exit(m.Run())
}
