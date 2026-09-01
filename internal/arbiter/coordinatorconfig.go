package arbiter

import (
	"fmt"
	"time"
)

// CoordinatorConfig is the tuning an elected peer must coordinate under.
//
// WHY THIS TRAVELS ON THE ANNOUNCEMENT. The tuning is a property of the MEET, not of
// whichever node happens to hold the role. If one peer coordinates at MaxDepth 2 and its
// successor at MaxDepth 3, the subnet changes shape on handover for a reason nobody
// chose — the tree reconfigures because the COORDINATOR moved, which is precisely what
// the stability work exists to prevent. Per-peer flags would also let two candidates
// disagree, so which tree a meet gets would depend on who won an election.
//
// The rejected alternative was `cmd/peer` flags. Besides the drift above, one field is
// genuinely unknowable to a peer: SocketDetectionMs is a property of the DEPLOYMENT,
// derived from the Hub's liveness budget, and a peer has no hub. The arbiter already
// owns "who coordinates, at what epoch"; this makes it own "and under what constraints"
// too — one announcement, one consistent view, no drift between candidates.
//
// WHAT IS NOT HERE, and why. SelfName is the holder's own identity, which must differ
// per holder by definition. Clock is a per-process capability, not a value. Epoch and
// Rev are the coordinator's to mint at build time and are never dictated. Everything
// else on coordinator.Config shapes the meet and is carried.
//
// The values are fixed when the Arbiter is constructed, so they cannot change under a
// running meet; a redeploy is what changes them, and a redeploy drops every socket.
//
// EVERY VALUE IS RESOLVED, AND ABSENCE IS NOT REPRESENTABLE PER FIELD. This is the
// deliberate difference from coordinator.Config, which uses 0 to mean "use the package
// default" for StickinessMs, Dwell, RecomputeCooldown and JoinSettle. That convention
// cannot be put on a wire, because 0 is ALSO a meaningful value for every one of them —
// memoryless re-parenting, no hysteresis, no anti-thrash floor, build immediately — so a
// bare number would mean "the operator chose zero" or "nobody filled this in" and no
// reader could tell which. (That collision is live on the server today: -stickiness-ms 0
// is documented and validated as memoryless and silently yields 25. It is the same shape
// as an inverted boolean polarity — a value that carries meaning colliding with a
// convention that treats it as absent.)
//
// So a field here always means exactly itself, and the "did anyone fill this in"
// question is answered ONCE, by Resolved, instead of per field by a pointer or a
// sentinel. One flag rather than four keeps the struct comparable with ==, which matters:
// a pointer field would make == compile and silently compare addresses, which is the
// same class of quiet wrongness this type exists to remove.
//
// Durations are carried as milliseconds rather than as time.Duration, matching
// metrics.Heartbeat.IntervalMs — the codebase's established convention for a duration on
// the wire. time.Duration would marshal as nanoseconds, which round-trips fine but is
// unreadable in a log line at exactly the moment someone is debugging a mis-shaped meet.
// Read them through the accessors below so the unit conversion exists in one place.
//
// No field is omitempty: 0 is meaningful throughout, and a configuration record that
// hides its own fields is the wrong thing to find in a log.
type CoordinatorConfig struct {
	// Resolved says an operator actually filled this in — that every value below is a
	// chosen effective one and none is a Go zero standing in for "unset".
	//
	// It exists because the fields split into two hazard classes. MaxDepth and
	// StreamKbps fail LOUDLY when unset: BuildTree hard-errors, so a peer coordinator
	// obviously publishes nothing. The rest fail QUIETLY — a receiver's own
	// zero-means-default convention substitutes a value, which coincides with the
	// server's flag defaults right up until an operator tunes one. Tune -dwell on the
	// arbiter, hand the role to a peer, and the subnet is silently retuned mid-meet:
	// exactly the outcome carrying the config exists to prevent, in the form nobody
	// notices. Validate rejects a config that does not set this, so the quiet case
	// becomes a startup error instead of a mis-shaped meet.
	Resolved bool `json:"resolved"`
	// MaxDepth and StreamKbps flow straight into overlay.Constraints. StreamKbps must
	// be positive: BuildTree hard-errors on a non-positive per-stream cost, so a peer
	// that adopts the role with a zero here runs and publishes nothing.
	MaxDepth   int `json:"max_depth"`
	StreamKbps int `json:"stream_kbps"`
	// DefaultUploadKbps is the budget assumed for a peer that has joined but not yet
	// reported. It shapes the tree — it decides whether a newcomer may be a relay
	// before it has proven it can be — so it is meet policy, not local preference.
	DefaultUploadKbps int `json:"default_upload_kbps"`
	// StickinessMs is the re-parent margin, and 0 means MEMORYLESS — not "unset".
	// Carrying it is the clearest case for this whole mechanism: it is the anti-thrash
	// knob, so a handover that changed it would re-shape the meet on the next rebuild,
	// the exact failure the knob exists to prevent, caused by the mechanism meant to
	// prevent it.
	StickinessMs float64 `json:"stickiness_ms"`
	// DwellMs is the sustained-degradation window and RecomputeCooldownMs the
	// anti-thrash floor between published trees. Both decide WHEN a meet reacts, which
	// is as much a property of the meet as the shape it reacts into. 0 means "no
	// dwell" and "no cooldown" respectively — a real configuration, not an absence.
	DwellMs             int64 `json:"dwell_ms"`
	RecomputeCooldownMs int64 `json:"recompute_cooldown_ms"`
	// DegradedAfterMs and GoneAfterMs override the per-node health thresholds. These
	// two are the genuine exception to the paragraph above, and they stay bare: 0
	// ALREADY means "derive per node from the cadence each peer declared", and an
	// explicitly-chosen zero threshold would be nonsense (every peer instantly gone).
	// The two readings do not collide, so there is no tri-state here to express, and
	// that 0 must survive the wire intact or a deployment relying on derivation would
	// silently get a hard-coded threshold instead.
	DegradedAfterMs int64 `json:"degraded_after_ms"`
	GoneAfterMs     int64 `json:"gone_after_ms"`
	// JoinSettleMs bounds the post-join wait for telemetry before building anyway. 0
	// means build immediately — again a choice, not an absence.
	JoinSettleMs int64 `json:"join_settle_ms"`
	// SocketDetectionMs is the Hub's worst-case socket-death detection window, and
	// every per-node gone threshold is floored at it. This is the field a peer cannot
	// possibly supply for itself: it describes the server's transport, not the peer's.
	// Without it an elected peer's coordinator would leave the permanent-ejection
	// window open — a peer declared gone and dropped from the tree while its socket is
	// still registered, unable to re-announce itself because only a NEW connection
	// runs the join handshake.
	SocketDetectionMs int64 `json:"socket_detection_ms"`
}

// The duration accessors. They exist so no consumer multiplies by time.Millisecond
// itself: the conversion is the one step where an elected peer could silently configure
// its coordinator a thousand times too fast, and that is not a mistake anything downstream
// would catch. A zero field yields a zero duration, preserving "derive it".
func (c CoordinatorConfig) Dwell() time.Duration { return millis(c.DwellMs) }

func (c CoordinatorConfig) RecomputeCooldown() time.Duration { return millis(c.RecomputeCooldownMs) }
func (c CoordinatorConfig) DegradedAfter() time.Duration     { return millis(c.DegradedAfterMs) }
func (c CoordinatorConfig) GoneAfter() time.Duration         { return millis(c.GoneAfterMs) }
func (c CoordinatorConfig) JoinSettle() time.Duration        { return millis(c.JoinSettleMs) }
func (c CoordinatorConfig) SocketDetection() time.Duration   { return millis(c.SocketDetectionMs) }

func millis(ms int64) time.Duration { return time.Duration(ms) * time.Millisecond }

// Validate reports whether this configuration can actually drive a coordinator, naming
// the offending JSON key so a startup failure points at the flag that caused it.
//
// It is deliberately a predicate rather than a defaulter. Inventing a StreamKbps would
// silently shape every meet on the server to a number nobody chose, and the symptom —
// a tree with the wrong fan-out — looks like a bug in the builder rather than a missing
// flag. cmd/server calls this at startup and fails loud; the arbiter warns rather than
// refusing, because an arbiter that will not start is worse than one that says why every
// meet is uncoordinated.
//
// Note which fields are NOT checked for zero: every duration here may legitimately be 0,
// because 0 is a chosen effective value rather than an absence (see the type comment).
// Only a NEGATIVE duration is nonsense. What replaces the per-field zero check is
// Resolved, which is checked first precisely so the error a partially-filled config
// produces names the real problem instead of whichever field happened to be caught.
func (c CoordinatorConfig) Validate() error {
	if !c.Resolved {
		return fmt.Errorf("resolved is false: this configuration was never filled in, so its " +
			"zero fields would be read as \"use the default\" and a handover would silently " +
			"retune the meet")
	}
	if c.MaxDepth < 1 {
		return fmt.Errorf("max_depth is %d, must be at least 1 (1 = a star: root to leaves)", c.MaxDepth)
	}
	if c.StreamKbps <= 0 {
		return fmt.Errorf("stream_kbps is %d, must be positive: BuildTree divides an upload "+
			"budget by it, so a coordinator configured this way publishes nothing", c.StreamKbps)
	}
	if c.DefaultUploadKbps < 0 {
		return fmt.Errorf("default_upload_kbps is %d, must not be negative", c.DefaultUploadKbps)
	}
	if c.StickinessMs < 0 {
		return fmt.Errorf("stickiness_ms is %v, must not be negative (0 = memoryless)", c.StickinessMs)
	}
	for _, d := range []struct {
		key string
		ms  int64
	}{
		{"dwell_ms", c.DwellMs},
		{"recompute_cooldown_ms", c.RecomputeCooldownMs},
		{"degraded_after_ms", c.DegradedAfterMs},
		{"gone_after_ms", c.GoneAfterMs},
		{"join_settle_ms", c.JoinSettleMs},
		{"socket_detection_ms", c.SocketDetectionMs},
	} {
		if d.ms < 0 {
			return fmt.Errorf("%s is %d, must not be negative (0 means use the default)", d.key, d.ms)
		}
	}
	return nil
}
