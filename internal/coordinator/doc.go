// Package coordinator is the control-plane brain that runs on the elected peer.
//
// It ingests the metrics view, drives overlay.BuildTree, and issues signaling
// instructions ("connect to parent P", "re-parent this subtree"). It also owns
// the hard parts that arrive later: hysteresis so it re-optimizes only on real
// threshold events, warm backup parents for fast failover (Phase 5), and — the
// budget-eater — handing the coordinator role itself over under central-server
// arbitration with epoch/term fencing (Phase 6).
//
// Populated in Phase 4 onward.
package coordinator
