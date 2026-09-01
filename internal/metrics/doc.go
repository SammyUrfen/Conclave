// Package metrics defines conclave's telemetry types and their plumbing.
//
// Each peer periodically reports {uploadEstimate, rtt[neighbor], loss, cpu,
// natType}; the coordinator fans many such report streams in to one coherent
// view of the overlay. That fan-in is a canonical Go concurrency exercise —
// N producers, one owned aggregate — and the trade-off (serialize via a channel
// vs. a shared map + RWMutex) is made explicit where it lives.
//
// Report is that telemetry; Reporter is the peer-side producer that ships it every
// interval. The consuming fan-in lives in the coordinator package.
//
// More precisely, this package owns THE PEER -> CONTROL-PLANE WIRE PAYLOADS as a
// group: Report (telemetry), Heartbeat (liveness plus realized topology state), and
// Reparented (an unsolicited self-promotion), together with the liveness constants
// that relate them. The last two are liveness facts rather than telemetry, so the
// package name undersells it; a separate internal/control package was weighed and
// rejected for now, because it buys a better name at the cost of a fourth tiny leaf
// package, a new import edge in three places, and a rename of Report, which is a
// frozen wire type. Revisit the moment a fourth non-telemetry payload appears.
package metrics
