// Package overlay models the forwarding graph and builds it.
//
// Its centerpiece is BuildTree: a pure function that, given the set of nodes and
// their constraints (spare upload, RTTs, max depth, TURN-bound → forced leaf),
// returns a degree-bounded, depth-limited, min-latency relay tree via a greedy
// heuristic that PREFERS THE PREVIOUS TREE, so a rebuild after one departure moves
// the orphans and nobody else. Alongside it live two independent oracles —
// Validate ("is this tree legal?") and ValidateLocalRepair ("was this change
// minimal?") — which the tests and the simnet harness assert against instead of
// re-deriving the expected tree.
//
// Keeping the package pure — no sockets, no clock, no pion, no randomness, and no
// imports from anywhere else under internal/ — is what makes the topology logic
// unit- and simulation-testable in milliseconds, and it is a hard contract
// requirement rather than a stylistic preference. Everything temporal or random is
// passed in as a value.
//
// One consequence deserves stating up front: with a previous tree supplied, the
// result is a function of HISTORY, not just of the fleet. Two meets with identical
// members can hold different, equally valid trees. That is the price of
// minimal-disruption rebuilds, and it is deliberate.
//
// Populated in Phase 4; epoch/rev stamping, backup parents, and the
// stability-preserving rebuild landed in Phase 5.
package overlay
