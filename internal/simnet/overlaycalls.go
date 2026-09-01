package simnet

import "github.com/SammyUrfen/conclave/internal/overlay"

// This file is the SINGLE point at which simnet calls into internal/overlay.
//
// It exists because the overlay contract is still moving: PickRoot, BuildTree, and
// ValidateLocalRepair have all changed shape once already and are scheduled to change
// again (a stream-cost parameter on PickRoot; a Churn struct and the fleet projection
// on ValidateLocalRepair). Scattering those calls across the harness and its tests
// would turn each amendment into a twenty-site edit; funnelled here it is a few
// lines, and the wrappers already take every argument the next signature wants even
// where today's does not use it. That is deliberate: the caller's shape is stable
// even while the callee's is not.

// pickRoot chooses the tree's root. cons is taken whole (rather than just the fields
// used today) so the stream cost is already at hand when PickRoot starts judging
// "can this node parent at the real per-stream price" rather than "upload > 0".
func pickRoot(nodes []overlay.Node, prev *overlay.Topology, cons overlay.Constraints) string {
	_ = cons
	return overlay.PickRoot(nodes, prev)
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
// churn that caused it — the headline stability property. nodes and cons are taken
// even though today's ValidateLocalRepair is a pure function of the two trees,
// because the impairment signal it is about to gain lives on the fleet, not on the
// trees.
func validateRepair(prev, next *overlay.Topology, nodes []overlay.Node, cons overlay.Constraints, gone, joined, promoted []string) error {
	_, _ = nodes, cons
	return overlay.ValidateLocalRepair(prev, next, gone, joined, promoted)
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
