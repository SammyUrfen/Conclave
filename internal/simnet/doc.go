// Package simnet is an in-memory simulated network for deterministic testing.
//
// It fakes a set of nodes with injectable latencies, upload caps, and churn
// events (join/leave/failure) and drives the *real* overlay and coordinator
// logic against them — no pion, no cameras, no wall-clock flakiness. This is the
// harness that keeps the genuinely hard phases honest: failover (Phase 5) and
// coordinator election/handover (Phase 6) are only testable because their
// decisions run behind an interface this package can substitute.
//
// Populated in Phase 4.
package simnet
