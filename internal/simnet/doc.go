// Package simnet is an in-memory simulated network for deterministic testing.
//
// It fakes a set of nodes with injectable latencies, upload caps, and churn
// events (join/leave/failure) and drives the *real* overlay and coordinator
// logic against them — no pion, no cameras, no wall-clock flakiness. This is the
// harness that keeps the genuinely hard phases honest: failover (Phase 5) and
// coordinator election/handover (Phase 6) are only testable because their
// decisions run behind an interface this package can substitute.
//
// Network is the harness: Add/Remove/SetRTT model a fleet and its churn, and Build
// drives the real overlay.BuildTree so tests assert overlay.Validate holds.
package simnet
