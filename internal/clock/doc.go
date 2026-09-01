// Package clock is conclave's injectable time seam.
//
// Production wires System(); tests and the simnet harness wire a virtual clock, so
// no control-plane test ever touches the wall clock and every temporal behaviour
// (dwell, hysteresis, heartbeat timeouts) is asserted in virtual time instead of
// through sleeps that are either flaky or slow.
//
// # Why this is a shared package and not a consumer-defined interface
//
// The rest of this codebase declares interfaces at the point of USE, which is what
// keeps the dependency graph acyclic. Clock is the deliberate exception. A bare
// interface{ Now() time.Time } would work structurally, but the moment the
// interface says NewTimer(d) Timer, the RETURN type becomes part of the method
// signature — and Go compares NAMED types, not their shapes, in return position.
// A method returning coordinator.Timer does not satisfy an interface demanding
// simnet.Timer even when the two are written identically, so every consumer would
// need its own fake. One leaf package solves it once.
//
// Treat it like *slog.Logger: an ambient capability injected at construction, not a
// service seam.
//
// # Why C() is a method
//
// Timer and Ticker expose their channel through a C() method rather than the C
// FIELD that time.Timer and time.Ticker use, because a Go interface can declare
// methods but not fields — a *time.Timer therefore cannot satisfy an interface that
// mentions C. The real implementation is a one-line wrapper. This is the standard
// shape for clock injection in Go, and the reason a "just use time.Timer" shortcut
// does not exist.
package clock
