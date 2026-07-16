// Package overlay models the forwarding graph and builds it.
//
// Its centerpiece is BuildTree: a pure function that, given the set of nodes and
// their constraints (spare upload, RTTs, max depth, TURN-bound → forced leaf),
// returns a degree-bounded, depth-limited, min-latency relay tree via a greedy
// heuristic. Keeping it pure — no sockets, no clock — is what makes the topology
// logic unit- and simulation-testable in milliseconds.
//
// Populated in Phase 4.
package overlay
