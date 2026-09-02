package media

import (
	"testing"
	"time"

	"github.com/pion/rtcp"
)

// The arm/disarm path — forwardLayer.awaiting and the SSRC reset that goes with it —
// is the riskiest mechanism in the layered relay, and it was already hiding one
// CRITICAL defect (the permanently-black subtree docs/DESIGN.md §7.5a records as F1).
// Every failure it can produce is SILENT: the source is registered, the upstream is
// set, a reader loop is running and metering, and the only symptom is a still frame
// on somebody else's screen.
//
// The three tests below are the mutation sweep's three survivors on that path, each
// asserted at the only level that proves anything — WHICH PACKETS REACH A LEG, and
// WHICH SSRC AN UPSTREAM PLI CARRIES. Not `awaiting`, not `activeGen`: an internal
// flag is exactly what the sweep showed a test can assert while the child stays black.
//
// Shared shape: one single-layer source, one leg, a re-parent that COMMITS BEFORE the
// new upstream's track arrives. That ordering is not a corner case — it is the one
// the Router actually produces, because the topology commits on the Run goroutine
// while the new parent's OnTrack fires later on a pion goroutine.

// TestArmedLayerPromotesTheNextReaderOnArrival is M55: `promoteOrArm` never arms.
//
// MUTATION CAUGHT: deleting `l.awaiting = true` from promoteOrArm's else branch. The
// commit then leaves activeGen pointing at the DEAD parent's reader, and the new
// reader — registered after the commit, so neither `nextGen > activeGen` at commit
// time nor `activeGen == 0` on arrival — never becomes authoritative. Every packet
// the new parent delivers is discarded by fanout's generation check. The child is
// black FOREVER, with no error on any path.
//
// The assertion is the child's packet series: two packets, the second from the new
// upstream's SSRC, spliced continuously onto the first.
func TestArmedLayerPromotesTheNextReaderOnArrival(t *testing.T) {
	f := newForwarder(discardLog(), &uploadMeter{}, func(func()) {}, newFakeClock())
	f.setUpstream("S", &captureRTCP{})
	gen1 := f.newUpstreamGen("S", "")
	f.learnSSRC("S", "", gen1, ssrcQ)

	child := &captureTrack{}
	f.addOutLive("S", "C", child, nil)

	f.fanout("S", "", gen1, layerPkt(ssrcQ, 100, 1000, true))
	if got := seqs(child); !equalU16(got, 100) {
		t.Fatalf("child sequence = %v before the re-parent, want [100]", got)
	}

	// Commit the re-parent with NO newer reader in existence: the layer must be armed
	// so the next one to appear wins on arrival.
	f.rebindUpstream("S", &captureRTCP{})

	gen2 := f.newUpstreamGen("S", "")
	f.learnSSRC("S", "", gen2, ssrcF)

	// The commit spliced the leg, so it drops until a keyframe — and then resumes.
	f.fanout("S", "", gen2, layerPkt(ssrcF, 7000, 50000, false))
	f.fanout("S", "", gen2, layerPkt(ssrcF, 7001, 53000, true))

	if got := seqs(child); !equalU16(got, 100, 101) {
		t.Fatalf("child sequence = %v after the new upstream's keyframe, want [100 101] — a "+
			"reader that appears AFTER the commit must take over on arrival, or the child "+
			"is black forever with no error anywhere", got)
	}
	if got := ssrcs(child); !equalU32(got, ssrcQ, ssrcF) {
		t.Errorf("child received SSRCs %#x, want [%#x %#x] — one from the old parent, then "+
			"the new one's", got, ssrcQ, ssrcF)
	}
}

// TestActivationDisarmsSoTheNextReaderMustWait is M32: `activate` never clears
// `awaiting`.
//
// MUTATION CAUGHT: deleting `l.awaiting = false` from activate. The layer stays armed
// after the reader it was armed FOR has taken over, so the NEXT reader to appear
// hijacks the leg the instant its track arrives — before any commit. That is
// make-before-break inverted: the new parent's stream is spliced onto the child while
// the old parent is still the authoritative one and the re-parent may still be
// abandoned, and the working stream is discarded from then on.
//
// The discriminator is the SSRC of the third packet, not the sequence number: the
// rewriter rebases sequence deliberately, so both the correct and the mutated run
// produce 100,101,102. Only the provenance differs.
func TestActivationDisarmsSoTheNextReaderMustWait(t *testing.T) {
	f := newForwarder(discardLog(), &uploadMeter{}, func(func()) {}, newFakeClock())
	f.setUpstream("S", &captureRTCP{})
	gen1 := f.newUpstreamGen("S", "")
	f.learnSSRC("S", "", gen1, ssrcQ)

	child := &captureTrack{}
	f.addOutLive("S", "C", child, nil)
	f.fanout("S", "", gen1, layerPkt(ssrcQ, 100, 1000, true))

	// First re-parent: commit, then the new reader arrives and is promoted.
	f.rebindUpstream("S", &captureRTCP{})
	gen2 := f.newUpstreamGen("S", "")
	f.learnSSRC("S", "", gen2, ssrcH)
	f.fanout("S", "", gen2, layerPkt(ssrcH, 500, 20000, true))
	if got := seqs(child); !equalU16(got, 100, 101) {
		t.Fatalf("child sequence = %v after the first re-parent, want [100 101]", got)
	}

	// Make-before-break for a SECOND re-parent: a third reader's track arrives with
	// no commit behind it. It may not write, and the settled reader must keep writing.
	gen3 := f.newUpstreamGen("S", "")
	f.learnSSRC("S", "", gen3, ssrcF)
	f.fanout("S", "", gen3, layerPkt(ssrcF, 9000, 70000, true))
	f.fanout("S", "", gen2, layerPkt(ssrcH, 501, 23000, false))

	if got := ssrcs(child); !equalU32(got, ssrcQ, ssrcH, ssrcH) {
		t.Errorf("child received SSRCs %#x, want [%#x %#x %#x] — an uncommitted reader may "+
			"not take over merely by arriving; the layer is disarmed by the activation it "+
			"was armed for", got, ssrcQ, ssrcH, ssrcH)
	}
	if got := f.activeGen("S", ""); got != gen2 {
		t.Errorf("active generation = %d, want %d (the committed one, not the pending %d)",
			got, gen2, gen3)
	}
}

// TestArmingResetsTheLayerSSRC is M31, and it is the one with a documented
// black-video failure attached to it: rebindUpstream's own doc comment says the SSRC
// MUST be reset because an upstream PLI carrying the OLD parent's SSRC is SILENTLY
// IGNORED by the new upstream. Nothing tested that.
//
// MUTATION CAUGHT: deleting `l.ssrc.Store(0)` from promoteOrArm's arm branch. Between
// the commit and the new reader's arrival the layer still holds the dead parent's
// SSRC, so a child's keyframe request is forwarded to the NEW upstream naming a
// stream it has never heard of. It is discarded with no error at either end, the
// relay's pliForwarded counter goes UP, and the child waits out the new encoder's
// next natural I-frame — seconds, on a looping file.
//
// The assertion is what reaches the new upstream, which is the whole failure: NOTHING
// while the layer is not flowing, and then a request naming the NEW SSRC once it is.
func TestArmingResetsTheLayerSSRC(t *testing.T) {
	clk := newFakeClock()
	f := newForwarder(discardLog(), &uploadMeter{}, func(func()) {}, clk)
	old := &captureRTCP{}
	f.setUpstream("S", old)
	gen1 := f.newUpstreamGen("S", "")
	f.learnSSRC("S", "", gen1, ssrcQ)
	f.addOutLive("S", "C", &captureTrack{}, nil)

	pli := []rtcp.Packet{&rtcp.PictureLossIndication{MediaSSRC: 0xDEAD}}

	// Baseline: while the old parent is authoritative, its SSRC is what goes upstream.
	f.handleRTCP("S", "C", pli)
	if old.count() != 1 {
		t.Fatalf("the old upstream got %d requests before the re-parent, want 1", old.count())
	}
	if got := old.pkts[0].(*rtcp.PictureLossIndication); got.MediaSSRC != ssrcQ {
		t.Fatalf("pre-re-parent PLI names %#x, want the old parent's %#x", got.MediaSSRC, ssrcQ)
	}

	// Commit onto a new parent whose track has not arrived. The throttle window is
	// advanced past so that a request COULD go out — otherwise this proves nothing.
	next := &captureRTCP{}
	f.rebindUpstream("S", next)
	clk.advance(pliThrottle + time.Millisecond)
	f.handleRTCP("S", "C", pli)
	if next.count() != 0 {
		t.Fatalf("the new upstream got %d requests naming %#x while the layer is not flowing, "+
			"want 0 — a PLI carrying the DEAD parent's SSRC is discarded by the new one with "+
			"no error, and the child stays black until the next natural I-frame",
			next.count(), next.pkts[0].(*rtcp.PictureLossIndication).MediaSSRC)
	}

	// …and once the new reader's track does arrive, requests resume naming ITS SSRC.
	gen2 := f.newUpstreamGen("S", "")
	f.learnSSRC("S", "", gen2, ssrcF)
	clk.advance(pliThrottle + time.Millisecond)
	f.handleRTCP("S", "C", pli)
	if next.count() != 1 {
		t.Fatalf("the new upstream got %d requests once its track arrived, want 1", next.count())
	}
	if got := next.pkts[0].(*rtcp.PictureLossIndication); got.MediaSSRC != ssrcF {
		t.Errorf("post-re-parent PLI names %#x, want the NEW parent's %#x", got.MediaSSRC, ssrcF)
	}
}

// TestReparentOntoALayeredUpstreamKeepsFlowing is the probe the review asked for: the
// F1 shape change run in the OTHER direction.
//
// TestReparentOntoAnUnlayeredUpstreamKeepsFlowing covers named rungs → one unnamed
// layer (our new parent is a relay forwarding what IT selected). This is the mirror:
// one unnamed layer → named rungs, which is what happens when a re-parent moves us
// ONTO the origin's own neighbour, so its `video.q/.h/.f` arrive where `fwd-S` used
// to. Both directions cross dropOtherShape, and neither had a test.
//
// It also pins the rung-by-rung discovery that follows: the auto leg follows the top
// as each higher rung appears, which costs it one drop-until-keyframe per rung and
// leaves it on `f`. That is the intended behaviour of "a fresh leg is never worse off
// than before layers existed", asserted here rather than assumed.
func TestReparentOntoALayeredUpstreamKeepsFlowing(t *testing.T) {
	f := newForwarder(discardLog(), &uploadMeter{}, func(func()) {}, newFakeClock())
	f.setUpstream("S", &captureRTCP{})
	gen := f.newUpstreamGen("S", "")
	f.learnSSRC("S", "", gen, 0xABCDEF)

	child := &captureTrack{}
	f.addOutLive("S", "C", child, nil)
	f.fanout("S", "", gen, layerPkt(0xABCDEF, 100, 1000, true))
	if got := len(child.snapshot()); got != 1 {
		t.Fatalf("child holds %d packets before the re-parent, want 1", got)
	}

	// Re-parent onto the origin's own neighbour: the same origin now arrives as three
	// named rungs. Commit first, then the readers appear — the Router's ordering.
	f.rebindUpstream("S", &captureRTCP{})

	// Lowest rung first, which is the order the origin adds its tracks in.
	for _, l := range []struct {
		id   string
		ssrc uint32
		seq  uint16
	}{{"q", ssrcQ, 200}, {"h", ssrcH, 300}, {"f", ssrcF, 400}} {
		g := f.newUpstreamGen("S", l.id)
		f.learnSSRC("S", l.id, g, l.ssrc)
		// Each new top splices the auto leg, so each rung must re-open with a keyframe.
		f.fanout("S", l.id, g, layerPkt(l.ssrc, l.seq, 10000, true))
		if got := f.legLayer("S", "C"); got != l.id {
			t.Fatalf("after rung %q arrived the auto leg resolves to %q, want %q — an auto "+
				"leg follows the source's top rung", l.id, got, l.id)
		}
	}

	// Four packets: the pre-re-parent one, then one per rung as the leg followed the
	// top up the ladder. A leg left resolving to the vanished unnamed layer would hold
	// exactly one — black forever, with no error anywhere.
	if got := seqs(child); !equalU16(got, 100, 101, 102, 103) {
		t.Fatalf("child sequence = %v, want 100..103 — one continuous series across the shape "+
			"change and each rung promotion", got)
	}
	if got := ssrcs(child); !equalU32(got, 0xABCDEF, ssrcQ, ssrcH, ssrcF) {
		t.Errorf("child received SSRCs %#x, want the unnamed layer then q, h, f", got)
	}
	// The unnamed layer is GONE, not merely outranked: leaving it in the map is what
	// lets the two shapes coexist and puts a rung nothing sends back on the ladder.
	f.mu.RLock()
	_, stillThere := f.sources["S"].layers[""]
	rungs := f.sources["S"].ladder()
	f.mu.RUnlock()
	if stillThere {
		t.Error("the unnamed layer survived the shape change; a source publishes either one " +
			"unnamed layer or several named rungs, never a mixture")
	}
	if len(rungs) != 3 {
		t.Errorf("ladder = %v, want the three named rungs only", rungs)
	}
}
