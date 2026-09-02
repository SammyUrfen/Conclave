package media

import (
	"testing"
	"time"

	"github.com/pion/rtcp"
)

// cleanReport is the wire form of "I lost nothing of what you sent me". FractionLost
// is a uint8 in 1/256 units, so zero is the clean end of it.
func cleanReport() []rtcp.Packet {
	return []rtcp.Packet{&rtcp.ReceiverReport{Reports: []rtcp.ReceptionReport{{SSRC: 1, FractionLost: 0}}}}
}

// TestNewTopRungMovesEveryAutoLeg pins the auto-follow-the-top behaviour, which was
// entirely unpinned: `retop`'s return value and the splice it drives were both
// deletable with the suite green.
//
// A leg is born AUTO — no explicit choice — and follows the source's highest rung, so
// that a child is never worse off than it would have been before layers existed. The
// rungs of a multi-layer origin arrive as separate tracks at separate moments, so the
// top MOVES during ladder discovery, and every auto leg moves with it.
//
// MUTATION CAUGHT (M15): `retop` never reporting that the top moved. MUTATION CAUGHT
// (M17b): deleting newUpstreamGen's `if topChanged` splice. Both produce the same
// corruption and neither is visible in the leg's LABEL: the leg correctly resolves to
// the new top, and then forwards the new encoding's raw sequence numbers on top of
// the old series with no rebase and no drop-until-keyframe — which is precisely the
// defect the sweep already caught once, in TestReviewLayerDowngradesOnSustainedLoss.
//
// A third mutation dies on the first assertion: a `retop` that never writes s.top at
// all leaves the leg resolving to the rung it started on.
func TestNewTopRungMovesEveryAutoLeg(t *testing.T) {
	f := newForwarder(discardLog(), &uploadMeter{}, func(func()) {}, newFakeClock())
	f.setUpstream("S", &captureRTCP{})
	genQ := f.newUpstreamGen("S", "q")
	f.learnSSRC("S", "q", genQ, ssrcQ)
	genH := f.newUpstreamGen("S", "h")
	f.learnSSRC("S", "h", genH, ssrcH)

	auto, picked := &captureTrack{}, &captureTrack{}
	f.addOutLive("S", "auto", auto, nil)
	f.addOutLive("S", "picked", picked, nil)
	// `picked` made an explicit choice and must NOT be dragged along by the top.
	// Without it the test cannot tell "splice the auto legs" from "splice everything".
	// It has to be a rung BELOW the current top, or selectLegLayer is a no-op and the
	// leg is still auto — which is how the first draft of this fixture proved nothing.
	if !f.selectLegLayer("S", "picked", "q") {
		t.Fatal("selectLegLayer reported no change moving `picked` off the top rung")
	}

	f.fanout("S", "h", genH, layerPkt(ssrcH, 100, 1000, true))
	f.fanout("S", "h", genH, layerPkt(ssrcH, 101, 4000, false))
	f.fanout("S", "q", genQ, layerPkt(ssrcQ, 40, 2000, true))
	if got := seqs(auto); !equalU16(got, 100, 101) {
		t.Fatalf("auto leg sequence = %v before the ladder grew, want [100 101]", got)
	}

	// A HIGHER rung appears — the origin's last track arriving a moment after the
	// others. The source's top moves from h to f.
	genF := f.newUpstreamGen("S", "f")
	f.learnSSRC("S", "f", genF, ssrcF)

	if got := f.legLayer("S", "auto"); got != "f" {
		t.Fatalf("the auto leg resolves to %q after a higher rung appeared, want %q — an auto "+
			"leg follows the source's top rung", got, "f")
	}
	if got := f.legLayer("S", "picked"); got != "q" {
		t.Fatalf("the leg with an explicit choice moved to %q, want %q", got, "q")
	}

	// The auto leg was SPLICED, so it drops until the new rung's keyframe. Asserted as
	// packets, not as the label above: the label is true under both mutations.
	f.fanout("S", "f", genF, layerPkt(ssrcF, 700, 50000, false))
	if got := len(auto.snapshot()); got != 2 {
		t.Fatalf("the auto leg wrote %d packets on an INTERFRAME of the new top rung, want 2 — "+
			"a leg whose effective layer just changed must arm the same drop-until-keyframe "+
			"latch a re-parent does, or it forwards an unrelated sequence series raw", got)
	}
	f.fanout("S", "f", genF, layerPkt(ssrcF, 701, 53000, true))
	if got := seqs(auto); !equalU16(got, 100, 101, 102) {
		t.Fatalf("auto leg sequence = %v, want [100 101 102] — continuous across the top move", got)
	}
	if got := ssrcs(auto); !equalU32(got, ssrcH, ssrcH, ssrcF) {
		t.Errorf("auto leg received SSRCs %#x, want [h h f]", got)
	}

	// …and the leg that chose q for itself saw nothing happen at all: its series runs
	// straight on with no drop and no rebase.
	f.fanout("S", "q", genQ, layerPkt(ssrcQ, 41, 5000, false))
	f.fanout("S", "q", genQ, layerPkt(ssrcQ, 42, 8000, false))
	if got := seqs(picked); !equalU16(got, 40, 41, 42) {
		t.Errorf("the explicitly-chosen leg's sequence = %v, want [40 41 42] untouched — a "+
			"top move is not that leg's business", got)
	}
}

// TestReparentAsksEveryLayerForAKeyframe covers requestUpstreamKeyframeAll, which the
// re-parent state machine calls the moment a new parent connects.
//
// MUTATION CAUGHT (M34): collapsing it to one request for the single unnamed layer.
// Every named rung then goes unasked, so each one stays black until its own encoder's
// next natural I-frame — and the layers are independently encoded, so f's keyframe
// does not decode q. Nothing errors, the relay's counter still moves, and the request
// that IS sent looks perfectly correct.
func TestReparentAsksEveryLayerForAKeyframe(t *testing.T) {
	clk := newFakeClock()
	f := newForwarder(discardLog(), &uploadMeter{}, func(func()) {}, clk)
	up := &captureRTCP{}
	twoLayerSource(t, f, up)

	f.requestUpstreamKeyframeAll("S")
	if up.count() != 2 {
		t.Fatalf("a re-parent asked for %d keyframes on a two-rung source, want 2 — every rung "+
			"arriving over the replaced edge needs its own I-frame", up.count())
	}
	// Sorted by layer id for determinism, so f precedes q.
	if got := up.pkts[0].(*rtcp.PictureLossIndication); got.MediaSSRC != ssrcF {
		t.Errorf("first request names %#x, want f's %#x", got.MediaSSRC, ssrcF)
	}
	if got := up.pkts[1].(*rtcp.PictureLossIndication); got.MediaSSRC != ssrcQ {
		t.Errorf("second request names %#x, want q's %#x", got.MediaSSRC, ssrcQ)
	}

	// The single-layer source still gets exactly one, naming its own SSRC — the
	// no-regression half, and the control arm for "the ladder ids are hardcoded".
	f.setUpstream("T", up)
	f.learnSSRC("T", "", f.newUpstreamGen("T", ""), 0xABCDEF)
	before := up.count()
	f.requestUpstreamKeyframeAll("T")
	if up.count() != before+1 {
		t.Fatalf("a single-layer source got %d requests, want 1", up.count()-before)
	}
	if got := up.pkts[before].(*rtcp.PictureLossIndication); got.MediaSSRC != 0xABCDEF {
		t.Errorf("single-layer request names %#x, want %#x", got.MediaSSRC, 0xABCDEF)
	}
}

// TestArmedLayerSplicesOnlyItsOwnLegs is the too-many-splices half of the invariant
// TestReparentAndLayerSwitchInterleaved only covers in the too-few direction.
//
// When ONE layer's armed generation is promoted on arrival, only the legs CARRYING
// that layer changed upstream. A leg watching a different rung saw nothing happen and
// must not be charged a drop-until-keyframe window for it.
//
// MUTATION CAUGHT (M36): newUpstreamGen's awaiting branch calling s.spliceAll()
// instead of s.spliceLayer(layer). MUTATION CAUGHT (M63): spliceLayer ignoring the
// rung and splicing every leg. Both make an unrelated child drop its next interframe
// and resume with a timestamp jump — a visible stutter on a leg nothing happened to,
// once per rung the far side is still bringing up.
//
// The fixture is the only way ONE layer ends up armed alone: a make-before-break
// re-parent where the new parent's `f` track arrives before the commit and its `q`
// track after it.
func TestArmedLayerSplicesOnlyItsOwnLegs(t *testing.T) {
	f := newForwarder(discardLog(), &uploadMeter{}, func(func()) {}, newFakeClock())
	genQ1, genF1 := twoLayerSource(t, f, &captureRTCP{})

	onF, onQ := &captureTrack{}, &captureTrack{}
	f.addOutLive("S", "onF", onF, nil) // auto ⇒ the top rung, f
	f.addOutLive("S", "onQ", onQ, nil)
	f.selectLegLayer("S", "onQ", "q")

	f.fanout("S", "f", genF1, layerPkt(ssrcF, 500, 90000, true))
	f.fanout("S", "q", genQ1, layerPkt(ssrcQ, 10, 1000, true))
	if got := seqs(onF); !equalU16(got, 500) {
		t.Fatalf("onF sequence = %v, want [500]", got)
	}
	if got := seqs(onQ); !equalU16(got, 10) {
		t.Fatalf("onQ sequence = %v, want [10]", got)
	}

	// Make before break: only the new parent's f track has arrived when the topology
	// commits. f is promoted; q has nothing newer, so it is ARMED.
	genF2 := f.newUpstreamGen("S", "f")
	f.learnSSRC("S", "f", genF2, ssrcF)
	f.rebindUpstream("S", &captureRTCP{})

	// The commit spliced BOTH legs (the whole upstream edge changed), so onF re-opens
	// on the new f reader's keyframe.
	f.fanout("S", "f", genF2, layerPkt(ssrcF, 800, 20000, true))
	if got := seqs(onF); !equalU16(got, 500, 501) {
		t.Fatalf("onF sequence = %v after the commit, want [500 501]", got)
	}

	// Now q's reader appears and the armed layer promotes it. onF is on a DIFFERENT
	// rung and is already running: it must not be spliced a second time.
	genQ2 := f.newUpstreamGen("S", "q")
	f.learnSSRC("S", "q", genQ2, ssrcQ)

	f.fanout("S", "f", genF2, layerPkt(ssrcF, 801, 23000, false))
	if got := seqs(onF); !equalU16(got, 500, 501, 502) {
		t.Fatalf("onF sequence = %v after layer q's armed promotion, want [500 501 502] — "+
			"promoting one layer must splice only the legs carrying THAT layer; a sibling on "+
			"another rung saw no discontinuity and must not be charged one", got)
	}

	// …and the leg that IS on q resumes, spliced exactly once.
	f.fanout("S", "q", genQ2, layerPkt(ssrcQ, 4000, 60000, true))
	if got := seqs(onQ); !equalU16(got, 10, 11) {
		t.Errorf("onQ sequence = %v, want [10 11] — the armed layer's own leg does resume", got)
	}
}

// TestPendingGenerationDoesNotStealTheLiveSSRC is M56: learnSSRC promoting any
// generation's SSRC rather than only the authoritative one's.
//
// During a make-before-break re-parent two readers exist for the same layer. The
// child is still being served by the OLD one, so an upstream keyframe request must
// name the OLD stream and go to the OLD parent. Naming the pending parent's SSRC
// sends the old parent a request for a stream it has never sent, which it discards
// with no error — the documented black-video failure, arrived at from a third
// direction.
//
// MUTATION CAUGHT: dropping the `gen == l.activeGen` guard in learnSSRC.
func TestPendingGenerationDoesNotStealTheLiveSSRC(t *testing.T) {
	clk := newFakeClock()
	f := newForwarder(discardLog(), &uploadMeter{}, func(func()) {}, clk)
	old := &captureRTCP{}
	f.setUpstream("S", old)
	gen1 := f.newUpstreamGen("S", "f")
	f.learnSSRC("S", "f", gen1, ssrcF)
	f.addOutLive("S", "C", &captureTrack{}, nil)

	pli := []rtcp.Packet{&rtcp.PictureLossIndication{MediaSSRC: 0xDEAD}}

	// The pending parent's track for the same layer arrives. No commit yet.
	gen2 := f.newUpstreamGen("S", "f")
	f.learnSSRC("S", "f", gen2, ssrcH)

	f.handleRTCP("S", "C", pli)
	if old.count() != 1 {
		t.Fatalf("the old upstream got %d requests, want 1", old.count())
	}
	if got := old.pkts[0].(*rtcp.PictureLossIndication); got.MediaSSRC != ssrcF {
		t.Errorf("the request names %#x, want the ACTIVE reader's %#x — a pending reader's "+
			"SSRC points the request at a stream the current upstream never sent, and it is "+
			"discarded with no error", got.MediaSSRC, ssrcF)
	}

	// …and the commit is what promotes it. This is activate's own SSRC store, the
	// promote-branch counterpart of the arm-branch reset.
	next := &captureRTCP{}
	f.rebindUpstream("S", next)
	clk.advance(pliThrottle + time.Millisecond)
	f.handleRTCP("S", "C", pli)
	if next.count() != 1 {
		t.Fatalf("the new upstream got %d requests after the commit, want 1", next.count())
	}
	if got := next.pkts[0].(*rtcp.PictureLossIndication); got.MediaSSRC != ssrcH {
		t.Errorf("the post-commit request names %#x, want the promoted reader's %#x",
			got.MediaSSRC, ssrcH)
	}
}

// TestReviewLayerSurvivesASourceWithNoTracksYet pins that selectLayer's
// `len(ladder) < 2` guard is NOT dead code, which is the only honest way to record a
// mutation that "survives".
//
// A source acquires its forwardSource — and its legs — from the topology, before any
// of its tracks have arrived. `forwardSource.ladder()` is EMPTY for it. If that leg's
// child sends a reception report in that window (a peer that has received nothing yet
// still sends them), reviewLayer calls selectLayer with an empty ladder: without the
// guard, indexOf returns -1 and the clamp indexes `ladder[len(ladder)-1]` — i.e.
// ladder[-1] — and PANICS, on a per-child RTCP-drain goroutine, taking the process
// with it.
//
// MUTATION CAUGHT: deleting the guard. This test panics rather than fails, which is
// exactly what that mutation does in production.
func TestReviewLayerSurvivesASourceWithNoTracksYet(t *testing.T) {
	f := newForwarder(discardLog(), &uploadMeter{}, func(func()) {}, newFakeClock())
	f.setUpstream("S", &captureRTCP{}) // the source exists; no track has arrived
	f.addOutLive("S", "C", &captureTrack{}, nil)

	f.mu.RLock()
	rungs := len(f.sources["S"].ladder())
	f.mu.RUnlock()
	if rungs != 0 {
		t.Fatalf("the fixture has %d rungs; it must have none, or it does not reach the "+
			"empty-ladder path at all", rungs)
	}

	f.handleRTCP("S", "C", cleanReport())

	if got := f.legLayer("S", "C"); got != "" {
		t.Errorf("leg layer = %q after a report on a source with no tracks, want %q", got, "")
	}
}

// TestRTTSensorIsWiredToTheForwarder covers the two mutations on the pairwise-RTT
// input, both of which fail toward SILENCE and are therefore invisible.
//
// RTT gates UPGRADES ONLY. A sensor that is never wired up, or that always answers
// "no measurement", produces exactly `RTTKnown: false` — which selectLayer is
// specified to read as "no opinion", because a static-tree relay legitimately has no
// RTT at all. So a broken sensor does not error, does not log, and does not change a
// single existing assertion: it just quietly stops refusing upgrades onto a queueing
// path. That is §7.5's inert-feature shape a third time.
//
// MUTATION CAUGHT (M40): deleting `r.fwd.rtt = r.rttToPeer` from NewRouter.
// MUTATION CAUGHT (M41): `rttToPeer` returning (0, false) unconditionally.
//
// The discriminator is a leg that must NOT upgrade under a high measured RTT while an
// otherwise identical leg under a low one does — a control arm, because "did not
// upgrade" is also what a broken hysteresis, a broken loss sensor and a one-rung
// ladder all produce.
func TestRTTSensorIsWiredToTheForwarder(t *testing.T) {
	clk := newFakeClock()
	// Managed, so NewRouter builds the forwarder. No Run, no client: the wiring under
	// test is a field assignment in the constructor and a lookup in the store.
	r := NewRouter(discardLog(), nil, RouterConfig{Managed: true, SelfName: "relay", Clock: clk})
	if r.fwd == nil {
		t.Fatal("a managed Router must hold a forwarder")
	}
	f := r.fwd
	f.setUpstream("S", &captureRTCP{})
	for _, id := range layerLadder {
		f.learnSSRC("S", id, f.newUpstreamGen("S", id), ssrcQ)
	}
	f.addOutLive("S", "C", &captureTrack{}, nil)
	if !f.selectLegLayer("S", "C", "q") {
		t.Fatal("selectLegLayer reported no change moving the leg to the bottom rung")
	}

	record := func(ms float64) {
		r.mu.Lock()
		r.rtt.record("C", ms, r.clk.Now())
		r.mu.Unlock()
	}
	feed := func(rounds int) {
		for i := 0; i < rounds; i++ {
			f.handleRTCP("S", "C", cleanReport())
		}
	}

	// A perfectly clean link with a QUEUEING round-trip: no upgrade, however long the
	// evidence runs. Adding bitrate to a deep queue is the one action guaranteed to
	// be wrong, and it is the only thing this signal is here to prevent.
	record(layerUpRTTMs + 50)
	feed(layerUpRounds * 3)
	if got := f.legLayer("S", "C"); got != "q" {
		t.Fatalf("the leg upgraded to %q under a measured %.0f ms round-trip, want %q — either "+
			"the sensor is not wired to the forwarder or it reports no measurement, and both "+
			"fail toward 'no opinion' with nothing in the logs",
			got, layerUpRTTMs+50, "q")
	}

	// Control arm: the SAME evidence with a healthy round-trip does upgrade. Without
	// it, the assertion above is satisfied by any policy that never upgrades at all.
	record(10)
	feed(layerUpRounds)
	if got := f.legLayer("S", "C"); got != "h" {
		t.Fatalf("the leg is on %q after %d clean rounds at 10 ms, want %q — the control arm "+
			"must move, or the assertion above proves nothing about RTT", got, layerUpRounds, "h")
	}

	// A sample older than RTTMemory is not a measurement. It must read as ABSENT, not
	// as its stale value: rttToPeer is only as fresh as the telemetry cadence, and a
	// peer whose edge closed long ago must not gate today's decision.
	record(layerUpRTTMs + 50)
	clk.advance(RTTMemory + time.Second)
	feed(layerUpRounds)
	if got := f.legLayer("S", "C"); got != "f" {
		t.Errorf("the leg is on %q after clean rounds with only an EXPIRED high-RTT sample, "+
			"want %q — past RTTMemory the sample is no longer evidence", got, "f")
	}
}
