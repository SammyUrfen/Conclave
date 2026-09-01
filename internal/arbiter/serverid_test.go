package arbiter_test

import (
	"testing"

	"github.com/SammyUrfen/conclave/internal/arbiter"
	"github.com/SammyUrfen/conclave/internal/signaling"
)

// TestDefaultArbiterIDMatchesServerID enforces the one duplication PLAN §2.4 rules is
// worth keeping: `arbiter` may not import `signaling` (the DAG forbids it), and
// `signaling.ServerID` is a WIRE constant with no correct shared home, so the literal
// is written twice. A duplication kept on purpose still needs something that fails when
// the two stop agreeing, and this is it.
//
// The import here is legal and creates no production edge: this is the EXTERNAL test
// package `arbiter_test`, a separate compilation unit that nothing links into the
// binary. §2.4 additionally requires cmd/server to hold the same assertion, since
// cmd/server is the place that must pass Config.ArbiterID explicitly; that one is
// WI-9's to write and does not replace this one.
func TestDefaultArbiterIDMatchesServerID(t *testing.T) {
	if arbiter.DefaultArbiterID != signaling.ServerID {
		t.Fatalf("arbiter.DefaultArbiterID = %q but signaling.ServerID = %q: a peer would "+
			"fence against an id the server never stamps, and every push would be rejected",
			arbiter.DefaultArbiterID, signaling.ServerID)
	}
}
