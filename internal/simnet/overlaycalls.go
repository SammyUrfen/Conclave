package simnet

import "github.com/SammyUrfen/conclave/internal/overlay"

// This file is the SINGLE point at which simnet calls into internal/overlay.
//
// It exists because the overlay contract has moved twice under this package already:
// PickRoot gained a stream-cost parameter, and ValidateLocalRepair gained the fleet
// projection, the Constraints, and a Churn struct in place of three string slices.
// Both amendments landed here as a few lines instead of a twenty-site edit across the
// harness and its tests, which is the whole point of the indirection — the caller's
// shape stays stable while the callee's moves.

// pickRoot chooses the tree's root. cons is taken whole rather than as a bare stream
// cost because PickRoot's eligibility rule now spans three Constraints-adjacent facts
// (the per-stream price, and via the Node, impairment and provisionality) and will
// plausibly span more.
func pickRoot(nodes []overlay.Node, prev *overlay.Topology, cons overlay.Constraints) string {
	return overlay.PickRoot(nodes, prev, cons.StreamKbps)
}

// buildTree computes the next tree from the fleet, the previous tree, and the
// constraints.
func buildTree(nodes []overlay.Node, prev *overlay.Topology, cons overlay.Constraints) (*overlay.Topology, error) {
	return overlay.BuildTree(nodes, prev, cons)
}

// validateTopology asks the independent oracle whether a tree is legal.
func validateTopology(t *overlay.Topology, nodes []overlay.Node, cons overlay.Constraints) error {
	return overlay.Validate(t, nodes, cons)
}

// validateRepair asks the independent oracle whether a TRANSITION was bounded by the
// churn that caused it — the headline stability property.
//
// prev MUST be the last PUBLISHED tree, never the coordinator's working copy. Two
// reasons, the second decisive: an oracle fed the patched copy asserts against the
// very belief it exists to check, and the published tree is the ONLY artifact still
// carrying the Backups assignment, so it is the only one against which "the promoted
// node landed on the backup it was actually ASSIGNED" can be checked at all. Callers
// in this package keep `published` and `working` in separate variables for exactly
// this reason; conflating them was a critical defect in an earlier contract.
func validateRepair(prev, next *overlay.Topology, nodes []overlay.Node, cons overlay.Constraints, ch overlay.Churn) error {
	return overlay.ValidateLocalRepair(prev, next, nodes, cons, ch)
}

// nextConstraints fills in the fields a scenario should never have to hand-crank:
// the root (picked, unless the caller pinned one) and the fencing counters.
//
// Epoch defaults to 1 — the harness models a single coordinator term unless a
// scenario is specifically exercising a handover — and Rev advances off prev, which
// is what BuildTree demands and what a scenario would otherwise get wrong on every
// rebuild, turning a real assertion into a stream of "rev does not advance" errors.
//
// StickinessMs is deliberately NOT defaulted: zero stickiness is a meaningful
// setting (it is the memoryless Phase 4 behaviour), so silently substituting the
// production default would hide the very knob a stability test is varying.
//
// It reports false when no node can root the tree, which the caller treats as "not
// ready" rather than as an error — a fleet of only provisional or TURN-bound members
// is a normal early state, not a fault.
func nextConstraints(cons overlay.Constraints, nodes []overlay.Node, prev *overlay.Topology) (overlay.Constraints, bool) {
	if cons.Root == "" {
		cons.Root = pickRoot(nodes, prev, cons)
		if cons.Root == "" {
			return cons, false
		}
	}
	if cons.Epoch == 0 {
		cons.Epoch = 1
	}
	if cons.Rev == 0 {
		cons.Rev = 1
		if prev != nil && prev.Epoch == cons.Epoch {
			cons.Rev = prev.Rev + 1
		}
	}
	return cons, true
}
