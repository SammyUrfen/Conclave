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
package metrics
