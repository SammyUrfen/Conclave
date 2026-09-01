// Package dashboard is the browser-facing control-plane surface: a small REST API for
// commands and point-in-time reads, plus one WebSocket per open meet view carrying the
// live event stream (docs/PLAN.md §9).
//
// # What it is not
//
// THE BROWSER NEVER CARRIES MEDIA. Nothing in this package touches WebRTC, and nothing
// it serves is meant to. The page it feeds creates and lists meets, hands a participant
// the command to join, renders the subnet, and streams the event log — control plane
// only.
//
// # Where it sits in the DAG
//
// dashboard imports arbiter, coordinator, overlay, metrics, policy and clock; nothing
// imports dashboard. The two publisher seams it satisfies (coordinator.Publisher and
// arbiter.Publisher) are declared by their EMITTERS precisely so that stays true, and
// the three consumer seams it declares (MeetSource, SubnetSource, DemoControl) name
// only producer-owned types so *arbiter.Arbiter and *coordinator.Coordinator satisfy
// them without importing anything new. cmd/server wires all five.
//
// It may NOT import signaling. That forbidden edge is what stops the next convenience
// ("dashboard already imports signaling — just call hub.SendRoom from the demo
// endpoint") from putting control-plane authority behind an unauthenticated HTTP
// handler; the shared boundary rules it needs live in internal/policy instead (§2.4).
//
// # The two rules everything here is shaped around
//
// FIRST: never block a control-plane goroutine. Publish, PublishElection and
// PublishRepair run on the coordinator's and arbiter's single Run goroutines. They fan
// out under a short mutex and DROP for a consumer that cannot keep up, rather than
// waiting — one stalled browser tab must never stall every meet in the process. The
// dual of that rule is that this package must never hold its own mutex across a call
// INTO the control plane (Snapshot, ListMeets, Evict, …): doing so would let a
// publisher block behind a goroutine that is itself waiting on the Run loop, which is
// a deadlock that only appears under load.
//
// SECOND: the origin allow-list is enforced HERE, not by the WebSocket library.
// coder/websocket's own check allows any request whose Host equals its Origin's host,
// before consulting the configured patterns and ignoring the scheme — which a DNS
// rebinding attacker satisfies by construction, reaching a loopback or LAN server they
// could not otherwise dial. Both the REST CORS surface and the upgrade therefore run the
// single policy.Origins matcher — the REST surface through Match, the upgrade through
// AllowUpgrade, which is the same call internal/signaling's /ws upgrade makes — and the
// library's check is disabled outright. See allowUpgradeOrigin.
//
// THIRD: no unauthenticated request may drive a control-plane goroutine at request rate.
// Assembling a snapshot round-trips the coordinator's single Run goroutine and listing
// meets round-trips the arbiter's, both reachable with no socket and no credentials, so
// both are bounded by snapshotMinInterval with a single-flight latch. Subscribers per
// meet are capped because fan-out is per-event work on the control plane, and resync is
// rate limited per connection.
//
// FOURTH: every collection on the wire has a defined order. Go randomises map
// iteration and the frontend replays these bodies, so an unordered collection makes two
// renderings of the same history differ. The orders are frozen in §8.1 and asserted
// here rather than assumed of the sources.
package dashboard
