// Package simnet is an in-memory simulated network for deterministic testing.
//
// It fakes a set of nodes with injectable latencies, upload caps, link degradation,
// partitions, and churn, and drives the *real* overlay and coordinator logic against
// them — no pion, no cameras, no wall clock. This is the harness that keeps the
// genuinely hard phases honest: failover (Phase 5) and coordinator election/handover
// (Phase 6) are only testable because their decisions run behind interfaces this
// package can substitute.
//
// The three pieces:
//
//   - Network models a room's participants. Add/Remove/SetRTT shape a fleet;
//     Kill/Leave/Partition/Heal/Degrade/Restore/Isolate/Rejoin break it on purpose;
//     Build drives the real overlay.BuildTree so a test asserts overlay.Validate.
//   - VirtualClock is a clock.Clock where time moves only when a test says so, which
//     is what makes dwell, hysteresis, and heartbeat timeouts assertable at all.
//   - Scenario scripts steps at virtual offsets, settles the loops under test between
//     them, and records a trace.
//
// Scenarios drive the REAL internal/coordinator (see control.go's Recorder, Capture,
// and ReportOf), which is the edge docs/PLAN.md §2.3 grants and which adds no
// production dependency because production never imports simnet. A lighter model of
// the loop is kept for the fast property sweeps only; harness_test.go states which
// scenarios use which, and a differential test pins the two together.
//
// # The two non-negotiable rules
//
//  1. A scenario replays identically from a seed. Enforced by the clock's
//     (deadline, sequence) firing order, the Network's insertion-order iteration
//     (never a map range), BuildTree's purity, and a single seeded *rand.Rand.
//  2. Nothing here reads the wall clock. The single real-time reference is
//     SettleTimeout, a deadlock guard that never affects a trace — only whether a
//     wedged test hangs forever or fails. `make check-determinism` enforces it.
//
// # What this harness deliberately does NOT assert
//
// That the same fleet always converges to the same tree. It does not, and cannot:
// stickiness makes BuildTree a function of history, which is exactly the price paid
// for minimal-disruption rebuilds — the two properties cannot both hold. What holds,
// and is what these tests assert, is that every tree is legal, that the edge delta
// between consecutive trees is bounded by the churn that caused the rebuild, and
// that the same event SEQUENCE always replays byte-identically.
package simnet
