// Package policy holds the rules this process applies to UNTRUSTED INPUT AT ITS
// BOUNDARY: which browser origins may open a connection, and what a meet id may
// look like. Every externally-facing surface — the peer WebSocket, the dashboard
// REST API, the dashboard event stream, meet creation — must apply the SAME
// rules. The failure mode of them diverging is silent and security-relevant: two
// copies of a matcher WILL drift, and the drift shows up as a silently permissive
// CORS policy that no test notices, because each surface's tests pass against its
// own copy. So there is exactly one implementation.
//
// # Why this is a shared package and not a consumer-defined interface
//
// The rest of this codebase declares interfaces at the point of use, which is
// what keeps the dependency graph acyclic (see docs/PLAN.md §2.3). policy is a
// deliberate exception, the same kind as internal/clock: a piece of shared
// VOCABULARY multiple packages must agree on EXACTLY, where the agreement is the
// point and divergence is silent, rather than a service seam where each
// consumer's needs legitimately differ. See docs/PLAN.md §2.4 for the rejected
// alternatives (letting dashboard import signaling directly; duplicating the
// matcher behind a cross-checking test) and why each loses.
//
// Leaf: imports nothing internal, ever. Same standing as internal/clock and
// internal/logging — an ambient boundary rule, not a service seam.
package policy
