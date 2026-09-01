// Package coordinator is the control-plane brain: it ingests the metrics and
// liveness view of a meet, decides when the forwarding tree must change, computes
// the new one with overlay.BuildTree, and pushes it to the peers.
//
// It imports NEITHER signaling NOR media. Inbound it satisfies signaling.Observer
// structurally (Go's implicit interfaces, so no import is needed); outbound it
// DECLARES what it needs — Sender and Publisher — and takes values of them.
// cmd/server wires the concrete ends. That is the whole mechanism keeping the
// dependency graph acyclic, and it is why every seam here is defined at the point of
// use rather than next to its implementation.
//
// # What Phase 5 added
//
//   - A three-state health FSM per (meet, node) — healthy → degraded → gone —
//     driven by the injected clock and by the cadence each peer DECLARES, with the
//     resurrection rule that makes a returning peer's first frame re-admit it.
//   - Hysteresis: a degradation dwell that fires only on an unbroken run of bad
//     telemetry, and a recompute cooldown that coalesces bursts. Neither is allowed
//     to delay the two cases that must act now — an unplaced joiner and a stranded
//     peer.
//   - Local subtree repair, which is EMERGENT rather than bespoke: excluding a gone
//     node from the projection and letting the stability-preserving builder run is
//     what moves exactly its orphans. There is deliberately no second repair
//     algorithm to drift from the builder and its oracle.
//   - Backup ratification: a peer that promoted its own backup has its choice
//     patched into the working copy BEFORE the rebuild, so stickiness defends the
//     decision instead of fighting it.
//   - The two-attempt build, so "unbuildable" means genuinely over-constrained and
//     never "stickiness painted us into a corner".
//   - The Publisher seam: every decision is observable as an ordered event stream,
//     which is what the dashboard renders and what the tests assert against.
//
// # Concurrency
//
// One goroutine owns all state (Run); every input is funnelled to it as an event.
// One further goroutine drains the outbound push queue, so a Sender that blocks
// cannot stall the control plane. Every deadline in the process is multiplexed onto
// a single clock.Timer, which is what lets Sync be a COMPLETE quiescence barrier:
// draining one wake channel drains every pending reaction.
//
// # What Phase 6 added
//
//   - Term boundaries. SetEpoch adopts a strictly higher arbiter-minted epoch and Yield
//     surrenders the role; between them they are the only things that move authority,
//     and neither can move it backwards. A term boundary discards what this node
//     BELIEVED (the tree) and keeps what it OBSERVED (telemetry, health, membership).
//   - Rebuild-from-peers, and only that. A new term starts from the realized parent
//     each peer reports in its heartbeat — never from a predecessor's snapshot — so the
//     handover path after a crash is byte-for-byte the path after a graceful handover,
//     and is therefore exercised by every test rather than only by the emergency.
//   - The outbound term gate: a push computed under a term that has ended is dropped
//     rather than sent. The fence on the peer would refuse it anyway; that is a reason
//     it is SAFE, not a reason to send it.
//
// This package still never learns who the coordinator is by itself. Authority arrives
// only through SetEpoch and Yield, which cmd/server and cmd/peer drive from the
// arbiter's announcements — the same single-writer discipline that makes the epoch a
// meaningful fencing token in the first place.
//
// # Observability
//
// Every decision this package makes is emitted through the Publisher seam as an ordered
// event stream, and one rule governs what goes in it: if the coordinator KNOWS a value,
// it emits that value rather than letting a consumer reconstruct it. Event.NodeID,
// Event.PrevHealth, and the StaleRejected totals in both snapshots all exist because a
// consumer was otherwise remembering or inferring something this package already held —
// and a derived value that only USUALLY matches is the failure mode this build has hit
// most often. Counting events (EventStale) follow the same transition-not-sample
// discipline as the health FSM: they fire on a change, never once per frame.
//
// Phases 5 and 6 of the roadmap.
package coordinator
