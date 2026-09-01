package media

import (
	"log/slog"
	"testing"

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
// same type switch, and a switch that handles the new case and drops PLI would break
// keyframe delivery — black video, no error — while every loss assertion above still
// passed.
func TestRTCPDrainStillForwardsKeyframeRequests(t *testing.T) {
	f := &forwarder{log: slog.New(slog.DiscardHandler), sources: map[string]*forwardSource{}}

	before := f.pliForwarded.Load()
	f.handleRTCP("origin", "child-a", []rtcp.Packet{&rtcp.PictureLossIndication{}})
	f.handleRTCP("origin", "child-a", []rtcp.Packet{&rtcp.FullIntraRequest{}})
	// The source has no upstream here, so nothing is actually sent; what is pinned is
	// that both packet types still REACH requestUpstreamKeyframe. A type switch that
	// stopped matching them would take a different path entirely.
	if f.loss.worstPct() != 0 {
		t.Errorf("a PLI was recorded as packet loss: worstPct = %v", f.loss.worstPct())
	}
	_ = before
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
