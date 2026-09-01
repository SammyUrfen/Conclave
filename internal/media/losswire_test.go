package media

import (
	"io"
	"log/slog"
	"testing"

	"github.com/pion/interceptor"
	"github.com/pion/rtcp"
)

// TestRTCPDrainFeedsTheLossSensor pins the WIRING, which the lossTracker unit tests
// cannot: they prove the aggregation is right, not that anything ever calls it.
//
// "Correct and never invoked" is the failure mode this whole sensor is exposed to —
// a Report.LossPct of 0 is a legal value meaning "a perfectly clean uplink", so a
// drain loop that quietly ignored receiver reports would look exactly like a healthy
// relay. The mutation this catches is deleting the *rtcp.ReceiverReport case from
// drainRTCP's type switch, which nothing else in the suite notices.
func TestRTCPDrainFeedsTheLossSensor(t *testing.T) {
	f := &forwarder{log: slog.New(slog.DiscardHandler)}

	f.handleRTCP("origin", "child-a", []rtcp.Packet{
		&rtcp.ReceiverReport{Reports: []rtcp.ReceptionReport{{FractionLost: 64}}}, // 25%
	})
	if got := f.loss.worstPct(); got < 24.9 || got > 25.1 {
		t.Errorf("worstPct = %v, want ~25: a receiver report did not reach the loss sensor", got)
	}

	// A second child reporting worse must win; a report naming the same child must
	// REPLACE that child's previous value rather than accumulate.
	f.handleRTCP("origin", "child-b", []rtcp.Packet{
		&rtcp.ReceiverReport{Reports: []rtcp.ReceptionReport{{FractionLost: 128}}}, // 50%
	})
	if got := f.loss.worstPct(); got < 49.9 || got > 50.1 {
		t.Errorf("worstPct = %v, want ~50 (the worst leg)", got)
	}
	f.handleRTCP("origin", "child-b", []rtcp.Packet{
		&rtcp.ReceiverReport{Reports: []rtcp.ReceptionReport{{FractionLost: 0}}},
	})
	if got := f.loss.worstPct(); got < 24.9 || got > 25.1 {
		t.Errorf("worstPct = %v, want ~25: child-b's recovery was not recorded", got)
	}
}

// TestRTCPDrainStillForwardsKeyframeRequests pins that adding the loss case did not
// displace the one the drain already had. Both arrive on the same socket through the
// same type switch, and a switch that handled the new case while dropping PLI or FIR
// would break keyframe delivery — black video, no error — while every loss assertion
// in this file still passed.
//
// FIR is asserted on its own SOURCE rather than alongside the PLI, because
// requestUpstreamKeyframe throttles per source: sent on the same one, the second
// request is legitimately swallowed and the test could not tell "FIR was dropped from
// the switch" from "FIR was throttled".
func TestRTCPDrainStillForwardsKeyframeRequests(t *testing.T) {
	f := newForwarder(slog.New(slog.DiscardHandler), &uploadMeter{}, func(func()) {}, newFakeClock())
	for _, src := range []string{"via-pli", "via-fir"} {
		f.setUpstream(src, &captureRTCP{})
		f.mu.Lock()
		f.sources[src].ssrc.Store(0x515151)
		f.mu.Unlock()
	}

	before := f.PLIForwarded()
	f.handleRTCP("via-pli", "child-a", []rtcp.Packet{&rtcp.PictureLossIndication{}})
	if got := f.PLIForwarded(); got != before+1 {
		t.Errorf("PLI forwarded %d, want %d: a PLI no longer reaches the source", got-before, 1)
	}
	f.handleRTCP("via-fir", "child-a", []rtcp.Packet{&rtcp.FullIntraRequest{}})
	if got := f.PLIForwarded(); got != before+2 {
		t.Errorf("after FIR the count is %d, want %d: a FIR no longer reaches the source", got-before, 2)
	}

	// And neither is mistaken for packet loss.
	if got := f.loss.worstPct(); got != 0 {
		t.Errorf("a keyframe request was recorded as packet loss: worstPct = %v", got)
	}
}

// TestRTCPDrainIgnoresAnEmptyReport pins that a receiver report with no reception
// blocks — which is what a peer sends when it has received nothing yet — records
// nothing rather than a zero. A zero here is indistinguishable from a measured clean
// link, and it would mask a genuinely lossy sibling leg in worstPct.
func TestRTCPDrainIgnoresAnEmptyReport(t *testing.T) {
	f := &forwarder{log: slog.New(slog.DiscardHandler)}
	f.handleRTCP("origin", "child-a", []rtcp.Packet{
		&rtcp.ReceiverReport{Reports: []rtcp.ReceptionReport{{FractionLost: 128}}},
	})
	f.handleRTCP("origin", "child-b", []rtcp.Packet{&rtcp.ReceiverReport{}}) // no blocks
	if got := f.loss.worstPct(); got < 49.9 || got > 50.1 {
		t.Errorf("worstPct = %v, want ~50: an empty report registered child-b as clean", got)
	}
}

// fakeRTCPSender feeds drainRTCP a scripted batch then EOF, standing in for the
// *webrtc.RTPSender the drain owns in production. onRead runs before each read, which
// is what lets a test observe state MID-LOOP.
type fakeRTCPSender struct {
	batches [][]rtcp.Packet
	n       int
	onRead  func(n int)
}

func (f *fakeRTCPSender) ReadRTCP() ([]rtcp.Packet, interceptor.Attributes, error) {
	if f.onRead != nil {
		f.onRead(f.n)
	}
	if f.n >= len(f.batches) {
		return nil, nil, io.ErrClosedPipe // the child's sender closed: child gone
	}
	b := f.batches[f.n]
	f.n++
	return b, nil, nil
}

// TestDrainRTCPLoopReachesTheSensor closes the last gap in this chain. Every other
// test here calls handleRTCP directly, which proves the dispatch and NOT that the read
// loop ever invokes it — the same "correct and never called" hazard, moved one level
// up.
//
// It has to observe MID-LOOP, and that is the whole subtlety. The obvious version
// checks worstPct after drainRTCP returns, by which time the deferred forget() has
// already removed this child — so "recorded, then forgotten" and "never recorded at
// all" produce the identical final value, and the test passes against the bug it
// exists to catch. (It did, on the first attempt.) So the fake samples the sensor on
// its SECOND read, after the first batch has been dispatched and before the loop ends.
func TestDrainRTCPLoopReachesTheSensor(t *testing.T) {
	f := &forwarder{log: slog.New(slog.DiscardHandler)}
	// A second child holds a reading so the forget() below is observable as a change
	// rather than as "the map was empty anyway".
	f.loss.observe("child-b", 0.02)

	var duringLoop float64
	sender := &fakeRTCPSender{
		batches: [][]rtcp.Packet{
			{&rtcp.ReceiverReport{Reports: []rtcp.ReceptionReport{{FractionLost: 32}}}}, // 12.5%
		},
		onRead: func(n int) {
			if n == 1 {
				duringLoop = f.loss.worstPct()
			}
		},
	}

	f.drainRTCP("origin", "child-a", sender) // returns when the fake reports EOF

	if duringLoop < 12.4 || duringLoop > 12.6 {
		t.Errorf("mid-loop worstPct = %v, want ~12.5: the read loop never handed the batch to the "+
			"sensor", duringLoop)
	}
	if got := f.loss.worstPct(); got < 1.9 || got > 2.1 {
		t.Errorf("worstPct after the drain exited = %v, want ~2: the departed child was not forgotten", got)
	}
}
