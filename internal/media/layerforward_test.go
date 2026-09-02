package media

import (
	"testing"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
)

// Layer SSRCs. Every assertion below identifies which LAYER a downstream packet
// came from by its SSRC, not by its sequence number: the rewriter deliberately
// rebases sequence and timestamp so a switched leg reads as one continuous series,
// which is exactly what makes seq useless as a provenance marker. The SSRC is
// copied through by rtpRewriter.Rewrite and overwritten only later, by pion's own
// per-binding rewrite, which the capture double stands in front of.
const (
	ssrcQ = 0xAA0001
	ssrcH = 0xBB0002
	ssrcF = 0xCC0003
)

// layerPkt is vp8Pkt with an explicit SSRC so a captured packet can be attributed
// to the layer that produced it.
func layerPkt(ssrc uint32, seq uint16, ts uint32, keyframe bool) *rtp.Packet {
	p := vp8Pkt(seq, ts, keyframe)
	p.SSRC = ssrc
	return p
}

// ssrcs lists the SSRC of every packet a leg actually received, in order.
func ssrcs(c *captureTrack) []uint32 {
	got := c.snapshot()
	out := make([]uint32, len(got))
	for i, p := range got {
		out[i] = p.SSRC
	}
	return out
}

// seqs lists the outgoing sequence numbers a leg actually received, in order.
func seqs(c *captureTrack) []uint16 {
	got := c.snapshot()
	out := make([]uint16, len(got))
	for i, p := range got {
		out[i] = p.SequenceNumber
	}
	return out
}

func equalU32(a []uint32, b ...uint32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalU16(a []uint16, b ...uint16) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// twoLayerSource wires a forwarder holding source "S" at layers q and f, with one
// reader generation each and a learned SSRC each. It returns the two generations.
func twoLayerSource(t *testing.T, f *forwarder, up rtcpWriter) (genQ, genF uint64) {
	t.Helper()
	f.setUpstream("S", up)
	genQ = f.newUpstreamGen("S", "q")
	f.learnSSRC("S", "q", genQ, ssrcQ)
	genF = f.newUpstreamGen("S", "f")
	f.learnSSRC("S", "f", genF, ssrcF)
	return genQ, genF
}

// TestRelayForwardsDifferentLayersToTwoChildren is the work item's headline claim,
// asserted at the only level that proves it: ONE source, TWO children, TWO
// different encodings reaching them AT THE SAME INSTANT.
//
// MUTATION CAUGHT: deleting the per-leg layer check in fanout (send every child
// every layer). The low child then receives four packets carrying both SSRCs
// instead of two carrying only ssrcQ — which is precisely the shipped behaviour
// this work item replaces, so the test also discriminates against the code as it
// stands today.
func TestRelayForwardsDifferentLayersToTwoChildren(t *testing.T) {
	f := newForwarder(discardLog(), &uploadMeter{}, func(func()) {}, newFakeClock())
	genQ, genF := twoLayerSource(t, f, &captureRTCP{})

	lo, hi := &captureTrack{}, &captureTrack{}
	f.addOutLive("S", "lo", lo, nil)
	f.addOutLive("S", "hi", hi, nil)

	// Both legs start on the source's HIGHEST layer — a child must never be worse
	// off merely because the source gained a lower rung.
	if got := f.legLayer("S", "hi"); got != "f" {
		t.Fatalf("a fresh leg resolves to layer %q, want the top rung %q", got, "f")
	}
	if !f.selectLegLayer("S", "lo", "q") {
		t.Fatal("selectLegLayer reported no change moving lo from f to q")
	}

	// Both layers are flowing simultaneously, interleaved as they would be on a
	// real relay where each layer has its own reader goroutine.
	f.fanout("S", "q", genQ, layerPkt(ssrcQ, 10, 1000, true))
	f.fanout("S", "f", genF, layerPkt(ssrcF, 500, 90000, true))
	f.fanout("S", "q", genQ, layerPkt(ssrcQ, 11, 4000, false))
	f.fanout("S", "f", genF, layerPkt(ssrcF, 501, 93000, false))

	if got := ssrcs(lo); !equalU32(got, ssrcQ, ssrcQ) {
		t.Errorf("lo received SSRCs %#x, want exactly the two q packets [%#x %#x]", got, ssrcQ, ssrcQ)
	}
	if got := ssrcs(hi); !equalU32(got, ssrcF, ssrcF) {
		t.Errorf("hi received SSRCs %#x, want exactly the two f packets [%#x %#x]", got, ssrcF, ssrcF)
	}
}

// TestLayerSwitchDoesNotPerturbTheOtherLeg pins the per-leg isolation. A leg is the
// unit of selection; a switch on one must be invisible to every sibling.
//
// MUTATION CAUGHT: splicing every leg of the source on a layer change (calling
// s.commitSwitch / iterating s.outs instead of touching the one leg). The untouched
// sibling would then arm waitKey, drop its next interframe, and resume with a
// TimestampGapTicks jump — a visible stutter on a child nothing happened to.
func TestLayerSwitchDoesNotPerturbTheOtherLeg(t *testing.T) {
	f := newForwarder(discardLog(), &uploadMeter{}, func(func()) {}, newFakeClock())
	genQ, genF := twoLayerSource(t, f, &captureRTCP{})

	moved, still := &captureTrack{}, &captureTrack{}
	f.addOutLive("S", "moved", moved, nil)
	f.addOutLive("S", "still", still, nil)

	// Both legs are on f and running.
	f.fanout("S", "f", genF, layerPkt(ssrcF, 500, 90000, true))
	f.fanout("S", "f", genF, layerPkt(ssrcF, 501, 93000, false))

	f.selectLegLayer("S", "moved", "q")

	f.fanout("S", "f", genF, layerPkt(ssrcF, 502, 96000, false))
	f.fanout("S", "q", genQ, layerPkt(ssrcQ, 20, 500, true))
	f.fanout("S", "f", genF, layerPkt(ssrcF, 503, 99000, false))

	// The sibling's series is untouched: no drop, no rebase, no gap.
	//
	// FATAL, not an error: the timestamp assertion below indexes this capture, and a
	// test that PANICS aborts the whole test binary — every test after it in the file
	// silently never runs, which is a far worse failure than the one being reported.
	if got := seqs(still); !equalU16(got, 500, 501, 502, 503) {
		t.Fatalf("the untouched leg's sequence = %v, want 500..503 continuous — a layer switch "+
			"on a sibling must not splice this leg", got)
	}
	if got := ssrcs(still); !equalU32(got, ssrcF, ssrcF, ssrcF, ssrcF) {
		t.Errorf("the untouched leg received SSRCs %#x, want four f packets", got)
	}
	// The last timestamp must be the input's, not a rebased one: a spurious Switch
	// would show up here as 96000+TimestampGapTicks rather than 99000.
	last := still.snapshot()[3]
	if last.Timestamp != 99000 {
		t.Errorf("the untouched leg's last ts = %d, want 99000 (its own upstream's), not a rebase", last.Timestamp)
	}

	// …and the moved leg did switch: it dropped f and resumed on q.
	if got := ssrcs(moved); !equalU32(got, ssrcF, ssrcF, ssrcQ) {
		t.Errorf("the moved leg received SSRCs %#x, want [f f q] — two before the switch, "+
			"then only q", got)
	}
}

// TestLayerSwitchDropsUntilKeyframeThenResumesContinuous asserts that a layer
// switch is handled as the SAME discontinuity a re-parent is: drop until a VP8
// keyframe, then splice the new encoding onto the outgoing series with no jump.
//
// This is the assertion that says rtpRewriter needed no mechanism change — the
// numbers below are exactly the ones TestRTPRewriter demands of a re-parent.
//
// MUTATION CAUGHT (two of them, separately):
//   - skipping rw.Switch() on a layer change: the first q packet is an interframe
//     and would be forwarded, at seq 20 — a backwards jump the child's jitter buffer
//     reads as catastrophic loss, and an interframe its decoder cannot use.
//   - arming waitKey but not rebase: the resuming keyframe would be forwarded with
//     the new upstream's own seq 21 instead of 502.
func TestLayerSwitchDropsUntilKeyframeThenResumesContinuous(t *testing.T) {
	f := newForwarder(discardLog(), &uploadMeter{}, func(func()) {}, newFakeClock())
	genQ, genF := twoLayerSource(t, f, &captureRTCP{})

	child := &captureTrack{}
	f.addOutLive("S", "C", child, nil)

	f.fanout("S", "f", genF, layerPkt(ssrcF, 500, 90000, true))
	f.fanout("S", "f", genF, layerPkt(ssrcF, 501, 93000, false))

	f.selectLegLayer("S", "C", "q")

	// An interframe from the new layer must NOT resume the leg.
	f.fanout("S", "q", genQ, layerPkt(ssrcQ, 20, 500, false))
	if got := len(child.snapshot()); got != 2 {
		t.Fatalf("child holds %d packets after an interframe on the new layer, want 2 — "+
			"a layer switch must drop until a keyframe", got)
	}
	// …the keyframe does, and it continues the series.
	f.fanout("S", "q", genQ, layerPkt(ssrcQ, 21, 3500, true))
	f.fanout("S", "q", genQ, layerPkt(ssrcQ, 22, 6500, false))

	if got := seqs(child); !equalU16(got, 500, 501, 502, 503) {
		t.Fatalf("child sequence = %v, want 500,501,502,503 — one continuous series across "+
			"the layer switch", got)
	}
	got := child.snapshot()
	if got[2].Timestamp != 93000+TimestampGapTicks {
		t.Errorf("first post-switch ts = %d, want %d (last written + one nominal frame)",
			got[2].Timestamp, 93000+TimestampGapTicks)
	}
	// The new layer's OWN inter-frame delta must carry forward — the switch rebases
	// the origin of the series, it does not re-time it.
	if got[3].Timestamp != 93000+TimestampGapTicks+3000 {
		t.Errorf("second post-switch ts = %d, want %d", got[3].Timestamp, 93000+TimestampGapTicks+3000)
	}
}

// TestUpstreamPLIAfterLayerSwitchCarriesThatLayerSSRC is the silent-black-video
// footgun, one axis deeper than the version relay_test.go already pins.
//
// A PLI naming an SSRC the sender does not recognise is DISCARDED WITH NO ERROR.
// With layers there are several upstream SSRCs for one source, so "the source's
// SSRC" is no longer a well-formed question — only "this layer's SSRC" is.
//
// MUTATION CAUGHT: keeping one ssrc field per source (promoting whichever layer
// learned its SSRC last). Both requests below would then carry ssrcF, the q encoder
// would never be asked for a keyframe, and a leg switched down to q would stay
// black until q's next natural I-frame — which, on a looping IVF, can be seconds.
func TestUpstreamPLIAfterLayerSwitchCarriesThatLayerSSRC(t *testing.T) {
	clk := newFakeClock()
	f := newForwarder(discardLog(), &uploadMeter{}, func(func()) {}, clk)
	up := &captureRTCP{}
	twoLayerSource(t, f, up)
	f.addOutLive("S", "C", &captureTrack{}, nil)

	// The leg is on f. A keyframe request from the child must name f's SSRC.
	f.handleRTCP("S", "C", []rtcp.Packet{&rtcp.PictureLossIndication{MediaSSRC: 0xDEAD}})
	if up.count() != 1 {
		t.Fatalf("upstream got %d packets, want 1", up.count())
	}
	if pli, ok := up.pkts[0].(*rtcp.PictureLossIndication); !ok || pli.MediaSSRC != ssrcF {
		t.Fatalf("first PLI = %+v, want MediaSSRC %#x (layer f's upstream SSRC)", up.pkts[0], ssrcF)
	}

	// Switching the leg to q must itself request a keyframe — for q. Without it the
	// child stares at a dropped stream until the new layer's next natural I-frame.
	f.selectLegLayer("S", "C", "q")
	if up.count() != 2 {
		t.Fatalf("upstream got %d packets after a layer switch, want 2 — a switch that does not "+
			"ask the NEW layer for a keyframe leaves the child frozen", up.count())
	}
	if pli, ok := up.pkts[1].(*rtcp.PictureLossIndication); !ok || pli.MediaSSRC != ssrcQ {
		t.Fatalf("post-switch PLI = %+v, want MediaSSRC %#x (layer q's upstream SSRC)", up.pkts[1], ssrcQ)
	}

	// …and from now on the child's own requests follow the leg to q.
	clk.advance(pliThrottle + time.Millisecond)
	f.handleRTCP("S", "C", []rtcp.Packet{&rtcp.PictureLossIndication{MediaSSRC: 0xDEAD}})
	if up.count() != 3 {
		t.Fatalf("upstream got %d packets, want 3", up.count())
	}
	if pli, ok := up.pkts[2].(*rtcp.PictureLossIndication); !ok || pli.MediaSSRC != ssrcQ {
		t.Errorf("child-driven PLI after the switch = %+v, want MediaSSRC %#x — a child's PLI "+
			"is about the layer it is ACTUALLY receiving", up.pkts[2], ssrcQ)
	}
}

// TestPLIThrottleIsPerSourceAndLayer pins the throttle's key.
//
// MUTATION CAUGHT: keeping the throttle per SOURCE (one lastPLI on forwardSource).
// The q request below is then swallowed by the window the f request just opened —
// so the child that has just been switched down to q waits up to pliThrottle for a
// request that is never re-tried, and stays black meanwhile. The failure is
// asymmetric and easy to miss: the request that IS sent looks perfectly correct.
func TestPLIThrottleIsPerSourceAndLayer(t *testing.T) {
	clk := newFakeClock()
	f := newForwarder(discardLog(), &uploadMeter{}, func(func()) {}, clk)
	up := &captureRTCP{}
	twoLayerSource(t, f, up)

	f.requestUpstreamKeyframe("S", "f")
	f.requestUpstreamKeyframe("S", "q") // a DIFFERENT layer: must not be throttled
	f.requestUpstreamKeyframe("S", "f") // the same layer, same window: throttled away
	f.requestUpstreamKeyframe("S", "q") // ditto

	if up.count() != 2 {
		t.Fatalf("upstream got %d packets, want 2 (one per layer; the repeats throttled)", up.count())
	}
	if pli := up.pkts[0].(*rtcp.PictureLossIndication); pli.MediaSSRC != ssrcF {
		t.Errorf("first PLI MediaSSRC = %#x, want f's %#x", pli.MediaSSRC, ssrcF)
	}
	if pli := up.pkts[1].(*rtcp.PictureLossIndication); pli.MediaSSRC != ssrcQ {
		t.Errorf("second PLI MediaSSRC = %#x, want q's %#x", pli.MediaSSRC, ssrcQ)
	}

	// The window is still per-layer once it expires.
	clk.advance(pliThrottle + time.Millisecond)
	f.requestUpstreamKeyframe("S", "f")
	if up.count() != 3 {
		t.Errorf("upstream got %d packets after the window expired, want 3", up.count())
	}

	// A layer whose media has not arrived (SSRC 0) must emit nothing: a PLI naming
	// SSRC 0 is a PLI naming nothing.
	f.newUpstreamGen("S", "h") // registered, no SSRC learned
	before := up.count()
	f.requestUpstreamKeyframe("S", "h")
	if up.count() != before {
		t.Error("requested a keyframe for a layer with no learned SSRC")
	}
}

// TestReparentAndLayerSwitchInterleaved is the two-axis invariant, and the reason
// this work item is delicate: **exactly one (generation, layer) per leg may write,
// and the leg is spliced ONCE per transition.**
//
// The forwarder now has two independent axes. `activeGen` answers "which upstream
// reader is authoritative", the leg's layer answers "which encoding this child
// wants", and a packet must satisfy BOTH. A commit that races a selection is the
// case where getting it wrong produces a leg that is spliced twice — consuming an
// extra sequence slot the child sees as a one-packet gap — or, worse, one that
// resumes on the losing side of one axis and never recovers.
//
// MUTATION CAUGHT (each independently):
//   - letting a non-active generation write: the genF1/genQ1 packets after the
//     commit land, and the child sees two interleaved unrelated series.
//   - dropping the leg's layer check: the genF2 keyframe resumes the leg on f,
//     and every subsequent q packet is discarded — a child frozen on one frame.
//   - splicing the WRONG legs on the commit: promoting q without splicing this leg
//     leaves it forwarding genQ2's raw sequence numbers on top of genQ1's series.
//
// What it deliberately does NOT claim: that an EXTRA rw.Switch() on the commit is
// caught. A mutation sweep proved that claim false — Switch is idempotent before a
// leg resumes, exactly as rewrite.go documents and as TestRTPRewriter's "a second
// switch before the first resumed keeps dropping" already pins, so splicing twice
// costs no sequence slot. Splicing once per leg is still the right code (it is the
// cheaper and more obviously correct statement of the invariant); it is simply not
// the thing this test discriminates, and a comment claiming otherwise is how the
// next person deletes the wrong line.
func TestReparentAndLayerSwitchInterleaved(t *testing.T) {
	f := newForwarder(discardLog(), &uploadMeter{}, func(func()) {}, newFakeClock())
	genQ1, genF1 := twoLayerSource(t, f, &captureRTCP{})

	child := &captureTrack{}
	f.addOutLive("S", "C", child, nil)

	// The leg is on the top rung and running off the ORIGINAL upstream.
	f.fanout("S", "f", genF1, layerPkt(ssrcF, 1000, 90000, true))
	if got := seqs(child); !equalU16(got, 1000) {
		t.Fatalf("child sequence = %v, want [1000]", got)
	}

	// Make-before-break: the new parent's two layer tracks arrive while the old
	// parent is still delivering. Neither may write yet.
	genQ2 := f.newUpstreamGen("S", "q")
	genF2 := f.newUpstreamGen("S", "f")
	f.fanout("S", "f", genF2, layerPkt(ssrcF, 7000, 10000, true))
	f.fanout("S", "q", genQ2, layerPkt(ssrcQ, 8000, 20000, true))
	if got := seqs(child); !equalU16(got, 1000) {
		t.Fatalf("child sequence = %v after the pending upstream spoke, want [1000] — a "+
			"generation that is not active may not write", got)
	}

	// …and a LAYER switch races in before the re-parent commits. The old upstream
	// is still authoritative, so the leg must resume on genQ1, not genQ2.
	f.selectLegLayer("S", "C", "q")
	f.fanout("S", "f", genF1, layerPkt(ssrcF, 1001, 93000, false)) // right gen, wrong layer
	f.fanout("S", "q", genQ2, layerPkt(ssrcQ, 8001, 23000, true))  // right layer, wrong gen
	f.fanout("S", "q", genQ1, layerPkt(ssrcQ, 300, 5000, true))    // both right: writes
	if got := seqs(child); !equalU16(got, 1000, 1001) {
		t.Fatalf("child sequence = %v, want [1000 1001] — exactly one (gen, layer) may write", got)
	}

	// Commit the re-parent. Every layer's newest generation becomes authoritative.
	f.rebindUpstream("S", &captureRTCP{})

	f.fanout("S", "q", genQ1, layerPkt(ssrcQ, 301, 8000, true))    // retired generation
	f.fanout("S", "f", genF2, layerPkt(ssrcF, 7001, 13000, true))  // wrong layer
	f.fanout("S", "q", genQ2, layerPkt(ssrcQ, 9000, 40000, false)) // right, but an interframe
	if got := seqs(child); !equalU16(got, 1000, 1001) {
		t.Fatalf("child sequence = %v after the commit, want [1000 1001] still", got)
	}

	f.fanout("S", "q", genQ2, layerPkt(ssrcQ, 9001, 43000, true))
	f.fanout("S", "q", genQ2, layerPkt(ssrcQ, 9002, 46000, false))

	if got := seqs(child); !equalU16(got, 1000, 1001, 1002, 1003) {
		t.Fatalf("child sequence = %v, want 1000..1003 — the leg is spliced ONCE per "+
			"transition, not once per layer the commit touched", got)
	}
	if got := ssrcs(child); !equalU32(got, ssrcF, ssrcQ, ssrcQ, ssrcQ) {
		t.Errorf("child SSRCs = %#x, want [f q q q] — one f packet before the switch, "+
			"then only q", got)
	}
	// The leg ended on exactly one (generation, layer), and it is the newest of the
	// layer the selection chose.
	if got := f.legLayer("S", "C"); got != "q" {
		t.Errorf("leg layer = %q after the interleaving, want %q", got, "q")
	}
	if got := f.activeGen("S", "q"); got != genQ2 {
		t.Errorf("active generation for layer q = %d, want %d (the newest)", got, genQ2)
	}
	if got := f.activeGen("S", "f"); got != genF2 {
		t.Errorf("active generation for layer f = %d, want %d — the commit promotes EVERY "+
			"layer, not just the one the leg happens to be on", got, genF2)
	}
}

// TestSingleLayerSourceBehavesExactlyAsBefore is the no-regression contract. A peer
// publishing one track must be byte-for-byte what it is today: no drops, no
// rebasing, passthrough sequence and timestamp, one PLI throttle window per source.
//
// MUTATION CAUGHT: giving the unnamed layer a rank on the ladder, or making a
// fresh leg resolve to a NAMED rung rather than to whatever the source actually
// publishes. Either makes fanout's layer comparison fail for every packet and a
// single-layer child receives nothing at all — the loudest possible regression,
// which is why it is worth an explicit test rather than trusting the suite.
func TestSingleLayerSourceBehavesExactlyAsBefore(t *testing.T) {
	clk := newFakeClock()
	f := newForwarder(discardLog(), &uploadMeter{}, func(func()) {}, clk)
	up := &captureRTCP{}
	f.setUpstream("S", up)
	gen := f.newUpstreamGen("S", "")
	f.learnSSRC("S", "", gen, 0xABCDEF)

	a, b := &captureTrack{}, &captureTrack{}
	f.addOutLive("S", "A", a, nil)
	f.addOutLive("S", "B", b, nil)

	in := []*rtp.Packet{
		layerPkt(0xABCDEF, 100, 1000, true),
		layerPkt(0xABCDEF, 101, 4000, false),
		layerPkt(0xABCDEF, 102, 7000, false),
	}
	for _, p := range in {
		f.fanout("S", "", gen, p)
	}
	for name, c := range map[string]*captureTrack{"A": a, "B": b} {
		if got := seqs(c); !equalU16(got, 100, 101, 102) {
			t.Errorf("child %s sequence = %v, want the input's 100,101,102 untouched", name, got)
		}
		if got := c.snapshot(); len(got) == 3 && got[2].Timestamp != 7000 {
			t.Errorf("child %s last ts = %d, want the input's 7000", name, got[2].Timestamp)
		}
	}

	// The PLI path is the shipped one: source SSRC, one per throttle window.
	f.requestUpstreamKeyframe("S", "")
	f.requestUpstreamKeyframe("S", "")
	if up.count() != 1 {
		t.Fatalf("upstream got %d packets, want 1 (the second throttled)", up.count())
	}
	if pli := up.pkts[0].(*rtcp.PictureLossIndication); pli.MediaSSRC != 0xABCDEF {
		t.Errorf("PLI MediaSSRC = %#x, want the source's %#x", pli.MediaSSRC, 0xABCDEF)
	}

	// And selection is inert: a one-rung ladder has nothing to choose, so no amount
	// of reported loss may splice these legs.
	f.loss.observe("A", 0.9)
	for i := 0; i < 10; i++ {
		f.reviewLayer("S", "A")
	}
	if got := f.legLayer("S", "A"); got != "" {
		t.Errorf("leg layer = %q after sustained loss on a single-layer source, want %q", got, "")
	}
	f.fanout("S", "", gen, layerPkt(0xABCDEF, 103, 10000, false))
	if got := seqs(a); !equalU16(got, 100, 101, 102, 103) {
		t.Errorf("child A sequence = %v after the review rounds, want 100..103 — a review that "+
			"cannot change anything must not splice the leg", got)
	}
}

// TestReviewLayerDowngradesOnSustainedLoss wires the pure policy to the sensor the
// relay already drains, and asserts the loop closes: reception reports from a child
// eventually move that child's leg, and only that child's.
//
// MUTATION CAUGHT: never calling reviewLayer from handleRTCP. Every unit test of
// selectLayer still passes and the whole adaptive feature is dead — "the policy is
// correct but nothing ever calls it" is exactly the failure a green suite cannot
// distinguish, which is the same argument handleRTCP was split out of drainRTCP for.
func TestReviewLayerDowngradesOnSustainedLoss(t *testing.T) {
	f := newForwarder(discardLog(), &uploadMeter{}, func(func()) {}, newFakeClock())
	_, genF := twoLayerSource(t, f, &captureRTCP{})
	// A THREE-rung source, so "shed one rung" and "drop to the floor" are different
	// answers and this test discriminates between them too.
	genH := f.newUpstreamGen("S", "h")
	f.learnSSRC("S", "h", genH, ssrcH)
	f.addOutLive("S", "bad", &captureTrack{}, nil)
	f.addOutLive("S", "good", &captureTrack{}, nil)

	f.mu.RLock()
	s := f.sources["S"]
	f.mu.RUnlock()

	// Both legs are on the top rung and running, so a later drop is measurable as a
	// discontinuity rather than as a leg that never started.
	f.fanout("S", "f", genF, layerPkt(ssrcF, 500, 90000, true))
	f.fanout("S", "f", genF, layerPkt(ssrcF, 501, 93000, false))

	// A reception report is the wire form of "I lost this much of what you sent me".
	// 128/256 = 50% loss, well past layerDownLossPct.
	lossy := []rtcp.Packet{&rtcp.ReceiverReport{
		Reports: []rtcp.ReceptionReport{{SSRC: 1, FractionLost: 128}},
	}}
	clean := []rtcp.Packet{&rtcp.ReceiverReport{
		Reports: []rtcp.ReceptionReport{{SSRC: 1, FractionLost: 0}},
	}}
	for i := 0; i < layerDownRounds; i++ {
		f.handleRTCP("S", "bad", lossy)
		f.handleRTCP("S", "good", clean)
	}

	if got := f.legLayer("S", "bad"); got != "h" {
		t.Fatalf("the lossy child's leg = %q, want %q — %d reception reports above "+
			"layerDownLossPct must shed a rung", got, "h", layerDownRounds)
	}
	if got := f.legLayer("S", "good"); got != "f" {
		t.Errorf("the clean child's leg = %q, want %q — one child's loss is not the other's", got, "f")
	}

	// …and the leg was actually SPLICED, not merely relabelled.
	//
	// MUTATION CAUGHT: reviewLayer setting o.layer without calling rw.Switch(). The
	// label above is then perfectly correct and the child's stream is corrupt — the
	// new encoding's raw sequence numbers are forwarded on top of the old series with
	// no rebase and no drop-until-keyframe. Asserting the label alone let that pass;
	// a mutation sweep is what found it.
	bad := s.outFor("bad").track.(*captureTrack)
	f.fanout("S", "h", genH, layerPkt(ssrcH, 900, 70000, false))
	if got := len(bad.snapshot()); got != 2 {
		t.Fatalf("the switched leg wrote %d packets on an interframe of its new layer, want 2 "+
			"(the two pre-switch ones only) — a policy-driven switch must arm the same "+
			"drop-until-keyframe latch a re-parent does", got)
	}
	f.fanout("S", "h", genH, layerPkt(ssrcH, 901, 73000, true))
	if got := seqs(bad); !equalU16(got, 500, 501, 502) {
		t.Errorf("the switched leg's sequence = %v, want [500 501 502] — continuous across the "+
			"policy-driven layer switch", got)
	}
	if got := ssrcs(bad); !equalU32(got, ssrcF, ssrcF, ssrcH) {
		t.Errorf("the switched leg received SSRCs %#x, want [f f h]", got)
	}
}

// TestPinnedLegNeverSelects pins the deferral this work item makes deliberately: a
// relay can only forward a layer it RECEIVES, so per-child selection is a LEAF-only
// decision. A child RELAY is pinned to the top rung.
//
// MUTATION CAUGHT: dropping the pinned check. A congested intermediate relay would
// be walked down to layer q — and then every peer in its whole subtree is capped at
// q, including ones on perfect links, because the intermediate has nothing better to
// forward. One child's bad link would silently degrade a whole branch.
func TestPinnedLegNeverSelects(t *testing.T) {
	f := newForwarder(discardLog(), &uploadMeter{}, func(func()) {}, newFakeClock())
	twoLayerSource(t, f, &captureRTCP{})
	f.addOutPinned("S", "sub-relay", &captureTrack{}, nil)

	f.loss.observe("sub-relay", 0.9)
	for i := 0; i < layerDownRounds*5; i++ {
		f.reviewLayer("S", "sub-relay")
	}
	if got := f.legLayer("S", "sub-relay"); got != "f" {
		t.Errorf("a pinned leg moved to %q, want the top rung %q", got, "f")
	}
}

// TestKeyframeForChildAsksTheLegsLayer covers the on-connect keyframe request, which
// runs when a child's PeerConnection comes up so it gets an I-frame immediately
// instead of waiting for a natural one.
//
// MUTATION CAUGHT: asking for the SOURCE's top rung rather than the LEG's layer. A
// child that has been downgraded and then reconnects gets a keyframe request sent to
// an encoder whose output it is not receiving — so it waits out the layer it IS on
// until that encoder's next natural I-frame. Nothing errors; the child is just black
// for longer than it should be, on exactly the path that exists to prevent that.
func TestKeyframeForChildAsksTheLegsLayer(t *testing.T) {
	f := newForwarder(discardLog(), &uploadMeter{}, func(func()) {}, newFakeClock())
	up := &captureRTCP{}
	twoLayerSource(t, f, up)
	f.addOutLive("S", "C", &captureTrack{}, nil)

	f.selectLegLayer("S", "C", "q") // consumes q's throttle window
	before := up.count()
	f.keyframeForChild("C")
	if got := up.count() - before; got != 0 {
		t.Fatalf("keyframeForChild emitted %d requests inside q's throttle window, want 0", got)
	}

	// A leg still on the top rung asks for the top rung. Using a second source keeps
	// this independent of the first one's throttle state.
	f.setUpstream("T", up)
	genT := f.newUpstreamGen("T", "f")
	f.learnSSRC("T", "f", genT, ssrcF)
	f.newUpstreamGen("T", "q")
	f.learnSSRC("T", "q", f.newUpstreamGen("T", "q"), ssrcQ)
	f.addOutLive("T", "C", &captureTrack{}, nil)
	f.selectLegLayer("T", "C", "q")

	// The one request keyframeForChild is allowed to make must name q — the layer the
	// leg is on — and q's window is already spent, so the observable assertion is on
	// the SOURCE whose leg sits at the top.
	f.setUpstream("U", up)
	f.learnSSRC("U", "f", f.newUpstreamGen("U", "f"), ssrcF)
	f.learnSSRC("U", "q", f.newUpstreamGen("U", "q"), ssrcQ)
	f.addOutLive("U", "C", &captureTrack{}, nil)
	before = up.count()
	f.keyframeForChild("C")
	got := up.pkts[len(up.pkts)-1].(*rtcp.PictureLossIndication)
	if up.count() != before+1 || got.MediaSSRC != ssrcF {
		t.Errorf("keyframeForChild sent %d requests, last MediaSSRC %#x; want 1 naming the top "+
			"rung %#x (source U's leg was never downgraded)", up.count()-before, got.MediaSSRC, ssrcF)
	}
}

// TestReparentOntoAnUnlayeredUpstreamKeepsFlowing is the case where the two axes
// stop being independent: a re-parent that changes the SHAPE of the source's ladder.
//
// A relay keys a source by its ORIGIN, so the same origin can arrive as three named
// rungs (`video.q/.h/.f`, when the origin is our own neighbour) and then, after a
// re-parent, as ONE unnamed stream (`fwd-O`, when the new parent is a relay that
// forwards what it selected). layers.go states the contract — "a source publishes
// either one unnamed layer or several named ones, never a mixture" — and this is the
// path that violates it.
//
// MUTATION CAUGHT: letting the two kinds coexist in forwardSource.layers. The named
// rungs survive commitSwitch as ARMED records that no reader will ever appear on;
// `retop` still ranks `f` above the unnamed layer, so every leg keeps resolving to a
// rung nothing is sending, and fanout drops every packet the new parent delivers.
// The child goes black PERMANENTLY, with no error on any path — the source is
// registered, the upstream is set, the reader loop is running and metering, and the
// only symptom is a still frame.
func TestReparentOntoAnUnlayeredUpstreamKeepsFlowing(t *testing.T) {
	f := newForwarder(discardLog(), &uploadMeter{}, func(func()) {}, newFakeClock())
	genQ, genF := twoLayerSource(t, f, &captureRTCP{})
	genH := f.newUpstreamGen("S", "h")
	f.learnSSRC("S", "h", genH, ssrcH)

	auto, picked := &captureTrack{}, &captureTrack{}
	f.addOutLive("S", "auto", auto, nil)
	f.addOutLive("S", "picked", picked, nil)
	f.selectLegLayer("S", "picked", "q") // an explicit choice the policy made earlier

	f.fanout("S", "f", genF, layerPkt(ssrcF, 500, 90000, true))
	f.fanout("S", "q", genQ, layerPkt(ssrcQ, 10, 1000, true))
	if got := len(auto.snapshot()); got != 1 {
		t.Fatalf("the auto leg holds %d packets before the re-parent, want 1", got)
	}
	if got := len(picked.snapshot()); got != 1 {
		t.Fatalf("the picked leg holds %d packets before the re-parent, want 1", got)
	}

	// Re-parent onto a neighbour that FORWARDS this origin: one track, `fwd-S`, no
	// layer suffix. Commit first, then the new reader appears — the ordering the
	// Router actually produces, since the topology commits before pion's OnTrack.
	f.rebindUpstream("S", &captureRTCP{})
	gen := f.newUpstreamGen("S", "")
	f.learnSSRC("S", "", gen, 0xABCDEF)

	// A keyframe, then two interframes: enough for any leg that is going to resume.
	f.fanout("S", "", gen, layerPkt(0xABCDEF, 100, 1000, true))
	f.fanout("S", "", gen, layerPkt(0xABCDEF, 101, 4000, false))
	f.fanout("S", "", gen, layerPkt(0xABCDEF, 102, 7000, false))

	for name, c := range map[string]*captureTrack{"auto": auto, "picked": picked} {
		if got := len(c.snapshot()); got != 4 {
			t.Errorf("the %s leg holds %d packets after the re-parent (layer %q), want 4 — a "+
				"leg left resolving to a rung the new upstream does not publish is black "+
				"forever, with no error anywhere", name, got, f.legLayer("S", name))
		}
	}
	// …and the relay can still ask that upstream for a keyframe, which it cannot do
	// while the leg names a rung whose SSRC was zeroed by the commit.
	if got := f.legLayer("S", "auto"); got != "" {
		t.Errorf("the auto leg resolves to %q, want the unnamed layer the new upstream sends", got)
	}
}
