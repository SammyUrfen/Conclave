package media

import (
	"fmt"
	"strings"
)

// layerLadder is the fixed low→high ordering of the encodings a source may
// publish: quarter, half, full. It is a CLOSED vocabulary on purpose — the ids
// arrive on the wire inside a remote peer's track name, so an open one would let a
// peer invent unbounded map keys on every relay that receives it.
//
// Three rungs, not more: this is a demonstration of layer SELECTION and each rung
// costs the origin a whole extra encode and a whole extra outbound stream (see
// docs/DESIGN.md §5.14 for what that costs and why it is still the cheap side of
// the trade). Two rungs cannot show a middle; four buys nothing a third does not.
//
// The EMPTY id is deliberately not on the ladder. It is what a single-layer source
// publishes — today's `video` / `fwd-<name>` track — and giving it a rank would put
// it into comparisons it has no business in. A source publishes either one unnamed
// layer or several named ones, never a mixture.
var layerLadder = []string{"q", "h", "f"}

// layerRank returns id's position in layerLadder, or -1 for anything not on it
// (including the single-layer empty id). Negative means "not comparable", never
// "lowest": an unknown id must not be able to alias onto a real rung.
func layerRank(id string) int {
	for i, l := range layerLadder {
		if l == id {
			return i
		}
	}
	return -1
}

// layerSep separates a track's base name from its layer id (`video.h`,
// `fwd-leaf-b.q`).
//
// '.' rather than '-', and the reason is not cosmetic: policy.PeerNamePattern
// ADMITS '-' and '_', so `leaf-b` is a perfectly ordinary peer name and
// `fwd-leaf-b` split on the last '-' yields source "fwd-leaf" layer "b". The
// pattern FORBIDS '.', which makes this split unambiguous for every name the hub
// will ever accept. A layer suffix is a wire contract between two conclave peers,
// so it is worth spending one character that names cannot contain.
const layerSep = "."

// splitTrackName recovers (base name, layer id) from a track id.
//
// A suffix this build does not recognise is NOT a layer: the whole id is returned
// as the base name and the layer is empty. That is the conservative direction at a
// trust boundary — a peer that publishes `video.wat` is treated as an ordinary
// single-layer sender rather than as the author of a new rung.
func splitTrackName(id string) (base, layer string) {
	b, l, ok := strings.Cut(id, layerSep)
	if !ok || b == "" || layerRank(l) < 0 {
		return id, ""
	}
	return b, l
}

// trackSource recovers the ORIGIN peer and the layer a remote track carries.
//
// The order matters: the layer suffix is stripped FIRST, then the forwarding
// prefix, because the source name sits between them (`fwd-leaf-b.q`). Doing it the
// other way round yields a source called "leaf-b.q", which no forwardSource exists
// under, so the relay silently drains the track instead of fanning it out.
//
// A track that is not a forwarded one carries the handing peer's OWN media, which
// is by definition that peer's — the fallback that has been here since Phase 3.
func trackSource(trackID, peerName string) (src, layer string) {
	base, layer := splitTrackName(trackID)
	if s, ok := strings.CutPrefix(base, forwardTrackPrefix); ok {
		return s, layer
	}
	return peerName, layer
}

// mediaLayer is one encoding this peer publishes: the layer id that goes on the
// wire and the IVF file that feeds it.
type mediaLayer struct {
	ID   string // "" for a single-layer peer — exactly today's `video` track
	Path string // "" ⇒ synthetic frames
}

// mediaLayersFor turns the -media path list into the tracks a peer publishes.
//
// ONE path (or none) yields ONE layer whose id is EMPTY, which is byte-for-byte
// what this peer has always sent: a track called `video`, a source keyed by the
// peer's name, one m-line. That equivalence is the whole no-regression contract and
// it is why the single-layer id is "" rather than the ladder's top rung.
//
// Several paths are assigned the ladder's LOWEST rungs in order, so the operator
// lists them lowest-bitrate first. Two files therefore mean q and h rather than q
// and f: rungs are only ever compared with each other, so what matters is the
// order, not which names are used.
func mediaLayersFor(paths []string) []mediaLayer {
	if len(paths) <= 1 {
		path := ""
		if len(paths) == 1 {
			path = paths[0]
		}
		return []mediaLayer{{ID: "", Path: path}}
	}
	out := make([]mediaLayer, 0, len(paths))
	for i, p := range paths {
		out = append(out, mediaLayer{ID: layerLadder[i], Path: p})
	}
	return out
}

// ValidateMediaPaths rejects a layer list this build cannot publish.
//
// It fails LOUD rather than truncating, for the reason every other rule in
// cmd/peer's validate does: a peer that silently publishes three of the four
// encodings it was handed is a configuration that reported success and did
// something else.
func ValidateMediaPaths(paths []string) error {
	if len(paths) > len(layerLadder) {
		return fmt.Errorf("invalid -media: %d files given but only %d layers exist (%s)",
			len(paths), len(layerLadder), strings.Join(layerLadder, ", "))
	}
	for i, p := range paths {
		if len(paths) > 1 && strings.TrimSpace(p) == "" {
			return fmt.Errorf("invalid -media: layer %d has an empty path; every layer needs a file", i+1)
		}
	}
	return nil
}

// --- the selection policy ------------------------------------------------------
//
// WHERE THIS LIVES AND WHY.
//
// Choosing which layer one child receives is a DATA-PLANE control loop running on
// DATA-PLANE inputs. Both of its inputs are already measured inside this package —
// the child's own RTCP reception reports (lossTracker, fed by the drain the relay
// runs anyway) and the nominated ICE pair's round-trip (selectedPairRTTMs) — and it
// reacts on the cadence those reports arrive at, roughly 1 Hz per child.
//
// Routing it through the coordinator instead would be wrong by docs/DESIGN.md
// §2.1's OWN argument, not merely slower. The coordinator's inputs are aggregate
// per-NODE telemetry on a 3 s cadence; a per-CHILD quality choice would have to
// travel up as new telemetry, wait out metrics.DefaultInterval, wait out a build,
// and come back down a WebSocket — turning a local congestion response into a
// distributed one, which is the exact coupling the control/data-plane split exists
// to prevent. It would also make every relay's downstream quality depend on the
// control plane being alive, which nothing in the data plane does today.
//
// It is NOT a new package: one pure function does not earn one, and internal/media
// is where every other thing that reads an RTP packet already lives. It is
// explicitly NOT internal/policy either — that package's charter (§3.5) is
// validating UNTRUSTED BOUNDARY INPUT, and a per-leg control law is not that. §3.5
// names the junk-drawer failure mode by name; this is what it warns about.
//
// The function itself is PURE: every input is a value, there is no clock and no
// I/O, and the hysteresis state is threaded through as a parameter and a return
// rather than held. Same shape as overlay.BuildTree, for the same reason — a
// decision that is a pure function of its inputs is one a table test can pin
// exhaustively, and this one has to be pinned exhaustively because getting the
// hysteresis wrong produces flapping, and flapping is invisible in any test that
// checks one transition.

// layerDownLossPct is the uplink loss, in percent, above which the current layer is
// judged too big for the path.
//
// Five percent is chosen against two things. VP8 without FEC degrades visibly but
// recoverably up to roughly there and breaks up past it, so it is the point where
// shedding a rung is a better picture than keeping one. And rtcp.ReceptionReport's
// FractionLost is a uint8 in 1/256 units — one quantisation step is ~0.39% — so 5%
// sits about thirteen steps above zero and cannot be tripped by rounding on a link
// that is losing one packet in a thousand.
const layerDownLossPct = 5.0

// layerUpLossPct is the loss below which the path is judged to have headroom.
//
// It MUST be strictly below layerDownLossPct; the gap between them is the
// hysteresis band, and a band of zero width means one sample at the threshold
// oscillates the decision forever. One percent is two or three quantisation steps:
// "essentially clean", without demanding the exactly-zero report a real link on a
// real network rarely produces.
const layerUpLossPct = 1.0

// layerUpRTTMs is the round-trip above which an upgrade is refused.
//
// RTT gates UPGRADES ONLY, and that asymmetry is the whole reason it is here. A
// rising round-trip is QUEUEING — the path is already buffering what it has — so
// adding bitrate to it makes the queue deeper and is the one action guaranteed to
// be wrong. It is NOT evidence that the CURRENT bitrate is too high: a peer three
// thousand kilometres away has a large RTT permanently and a perfectly clean link,
// and downgrading on that would penalise geography rather than congestion. Loss is
// the only thing that sheds a rung.
//
// 200 ms is the ITU-T G.114 one-way 150 ms guideline doubled and rounded — past it
// the call is already an uncomfortable one, which is not the moment to spend more
// of the path's budget.
const layerUpRTTMs = 200.0

// layerDownRounds is how many consecutive bad observations shed a rung.
//
// Observations arrive with the child's RTCP reception reports, roughly one a
// second, so two rounds is about two seconds of sustained loss. One round would
// react to a single burst — and absorbing a single burst is what the child's jitter
// buffer is FOR, so reacting to one would be doing the buffer's job worse and
// charging the child a keyframe wait for it.
const layerDownRounds = 2

// layerUpRounds is how many consecutive clean observations add a rung.
//
// Deliberately larger than layerDownRounds — roughly five seconds against two. The
// asymmetry is the whole hysteresis: shedding a rung is a response to evidence that
// already exists, while adding one is a BET that the path can take more, and a lost
// bet re-creates exactly the loss that forced the downgrade. Symmetric thresholds
// make a flapping machine, and every transit costs the child a drop-until-keyframe
// window. Being one rung too conservative for a few seconds is cheap; oscillating
// is not.
const layerUpRounds = 5

// layerObs is one round of evidence about one downstream leg. Both fields come from
// sensors that already exist; nothing here is new telemetry.
type layerObs struct {
	// LossPct is what this child reported losing of what we sent it, 0–100.
	LossPct float64
	// RTTMs is the round-trip to this child on the nominated ICE pair. RTTKnown is
	// false when it has never been measured — first attachment is RTT-blind (see
	// RTTMemory) and a static-tree relay never samples it at all. Absent must read
	// as "no opinion", never as "infinitely bad", or those legs freeze at whatever
	// rung they started on.
	RTTMs    float64
	RTTKnown bool
}

// layerChoice is one leg's selection state: the rung it is on and the length of the
// current run of evidence in each direction. It is carried BETWEEN rounds rather
// than held inside selectLayer, which is what keeps the function pure.
type layerChoice struct {
	Layer string
	Bad   int // consecutive observations above layerDownLossPct
	Good  int // consecutive observations clean enough to consider an upgrade
}

// selectLayer decides which rung of ladder a leg should be on, given where it is
// and one new observation. Pure: no clock, no I/O, no mutation of its arguments.
//
// ladder is the source's available rungs, lowest first. A ladder shorter than two
// has nothing to choose and the state is returned unchanged, which is what makes a
// single-layer source inert.
func selectLayer(ladder []string, cur layerChoice, obs layerObs) layerChoice {
	if len(ladder) < 2 {
		return cur
	}
	// A state naming a rung this source no longer publishes (an upstream went away
	// mid-run) is clamped to the top rather than trusted: indexing off the front of
	// the ladder would switch the leg to a layer nothing is sending, which is a
	// permanently black child with no error anywhere.
	i := indexOf(ladder, cur.Layer)
	if i < 0 {
		return layerChoice{Layer: ladder[len(ladder)-1]}
	}

	next := layerChoice{Layer: cur.Layer, Bad: cur.Bad, Good: cur.Good}
	switch {
	case obs.LossPct >= layerDownLossPct:
		next.Bad, next.Good = cur.Bad+1, 0
	case obs.LossPct <= layerUpLossPct && (!obs.RTTKnown || obs.RTTMs <= layerUpRTTMs):
		next.Good, next.Bad = cur.Good+1, 0
	default:
		// The band between the two thresholds is "no opinion", and it RESETS both
		// runs rather than merely failing to extend one. Letting a run survive an
		// indifferent sample would accumulate evidence across unrelated episodes:
		// four clean samples spread through a minute of intermittent loss would read
		// as a sustained clean run and upgrade onto a link that is visibly struggling.
		next.Bad, next.Good = 0, 0
	}

	switch {
	case next.Bad >= layerDownRounds && i > 0:
		next = layerChoice{Layer: ladder[i-1]}
	case next.Good >= layerUpRounds && i < len(ladder)-1:
		next = layerChoice{Layer: ladder[i+1]}
	}
	return next
}

// indexOf returns the position of s in xs, or -1.
func indexOf(xs []string, s string) int {
	for i, x := range xs {
		if x == s {
			return i
		}
	}
	return -1
}
