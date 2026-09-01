package media

import (
	"fmt"
	"sync"
	"testing"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

// vp8Payload builds a minimal VP8 RTP payload: a one-byte payload descriptor
// (S=1, PID=0 marks the start of a partition) followed by a frame tag whose bit 0
// is the frame type — 0 for a keyframe, 1 for an interframe. That single bit is
// exactly what the drop-until-keyframe latch keys off.
func vp8Payload(startOfPartition, keyframe bool) []byte {
	var desc byte
	if startOfPartition {
		desc = 0x10 // S = 1, PID = 0
	}
	tag := byte(0x01) // interframe
	if keyframe {
		tag = 0x00
	}
	return []byte{desc, tag, 0x00, 0x00, 0x9d, 0x01, 0x2a, 0x40, 0x01, 0xf0}
}

func vp8Pkt(seq uint16, ts uint32, keyframe bool) *rtp.Packet {
	return &rtp.Packet{
		Header:  rtp.Header{Version: 2, SequenceNumber: seq, Timestamp: ts, SSRC: 0x11111111},
		Payload: vp8Payload(true, keyframe),
	}
}

// TestIsVP8Keyframe pins the latch's discriminator. Getting it wrong in either
// direction is silent: too strict and the leg never resumes after a switch; too
// loose and it resumes on an undecodable interframe.
func TestIsVP8Keyframe(t *testing.T) {
	cases := []struct {
		name    string
		payload []byte
		want    bool
	}{
		{"keyframe starting a partition", vp8Payload(true, true), true},
		{"interframe starting a partition", vp8Payload(true, false), false},
		{"keyframe bits but not a partition start", vp8Payload(false, true), false},
		{"empty payload", nil, false},
		{"descriptor only", []byte{0x10}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isVP8Keyframe(tc.payload); got != tc.want {
				t.Errorf("isVP8Keyframe = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestRTPRewriter is the C12 contract. The property under test is the one a
// decoder actually needs: ONE monotonically continuous sequence/timestamp series
// per downstream leg, no matter how many different upstreams fed it.
//
// This test discriminates against the shipped behaviour by construction: today the
// forward loop hands the packet to TrackLocalStaticRTP.WriteRTP, which rewrites
// SSRC and payload type and passes sequence/timestamp through UNCHANGED. Every
// "after the switch" expectation below is a value the input does not contain, so a
// passthrough implementation fails each of them.
func TestRTPRewriter(t *testing.T) {
	t.Run("before any switch the stream is passed through untouched", func(t *testing.T) {
		w := &rtpRewriter{}
		in := []*rtp.Packet{vp8Pkt(100, 1000, true), vp8Pkt(101, 4000, false), vp8Pkt(102, 7000, false)}
		for i, p := range in {
			out := w.Rewrite(p)
			if out == nil {
				t.Fatalf("packet %d dropped before any switch", i)
			}
			if out.SequenceNumber != p.SequenceNumber || out.Timestamp != p.Timestamp {
				t.Errorf("packet %d rewritten to seq=%d ts=%d, want the input's %d/%d",
					i, out.SequenceNumber, out.Timestamp, p.SequenceNumber, p.Timestamp)
			}
			if out == p {
				t.Errorf("packet %d was not copied; legs share the upstream packet and must not alias it", i)
			}
		}
	})

	t.Run("a switch continues the outgoing series from where it stopped", func(t *testing.T) {
		w := &rtpRewriter{}
		for _, p := range []*rtp.Packet{vp8Pkt(100, 1000, true), vp8Pkt(101, 4000, false), vp8Pkt(102, 7000, false)} {
			if w.Rewrite(p) == nil {
				t.Fatal("packet dropped before any switch")
			}
		}

		w.Switch()
		// The new upstream's numbering is unrelated — this is the exact case where
		// passthrough corrupts the stream.
		if got := w.Rewrite(vp8Pkt(50000, 900000, false)); got != nil {
			t.Fatalf("an interframe was forwarded after a switch (seq=%d); the leg must "+
				"drop until a keyframe or the decoder renders garbage", got.SequenceNumber)
		}
		out := w.Rewrite(vp8Pkt(50001, 903000, true))
		if out == nil {
			t.Fatal("the keyframe after a switch was dropped")
		}
		if out.SequenceNumber != 103 {
			t.Errorf("post-switch seq = %d, want 103 (one past the last packet written)", out.SequenceNumber)
		}
		if out.Timestamp != 7000+TimestampGapTicks {
			t.Errorf("post-switch ts = %d, want %d (last written + TimestampGapTicks)",
				out.Timestamp, 7000+TimestampGapTicks)
		}

		// Within the new upstream, gaps must be PRESERVED: a lost packet has to stay
		// visible downstream or NACK/loss reporting silently stops working.
		out2 := w.Rewrite(vp8Pkt(50003, 909000, false))
		if out2 == nil {
			t.Fatal("an interframe after the resuming keyframe was dropped")
		}
		if out2.SequenceNumber != 105 {
			t.Errorf("seq = %d, want 105 (input skipped one, so the output must too)", out2.SequenceNumber)
		}
		if out2.Timestamp != 7000+TimestampGapTicks+6000 {
			t.Errorf("ts = %d, want %d (the new upstream's own delta carried forward)",
				out2.Timestamp, 7000+TimestampGapTicks+6000)
		}
	})

	t.Run("the outgoing sequence wraps like a uint16", func(t *testing.T) {
		w := &rtpRewriter{}
		if w.Rewrite(vp8Pkt(65534, 1000, true)) == nil {
			t.Fatal("first packet dropped")
		}
		if w.Rewrite(vp8Pkt(65535, 4000, false)) == nil {
			t.Fatal("second packet dropped")
		}
		w.Switch()
		out := w.Rewrite(vp8Pkt(7, 500, true))
		if out == nil {
			t.Fatal("the keyframe after a switch was dropped")
		}
		if out.SequenceNumber != 0 {
			t.Errorf("post-switch seq = %d, want 0 (65535 + 1 wraps)", out.SequenceNumber)
		}
	})

	t.Run("a second switch before the first resumed keeps dropping", func(t *testing.T) {
		w := &rtpRewriter{}
		if w.Rewrite(vp8Pkt(10, 1000, true)) == nil {
			t.Fatal("first packet dropped")
		}
		w.Switch()
		w.Switch()
		if got := w.Rewrite(vp8Pkt(900, 90000, false)); got != nil {
			t.Fatal("an interframe slipped through after two switches")
		}
		out := w.Rewrite(vp8Pkt(901, 93000, true))
		if out == nil {
			t.Fatal("the keyframe was dropped")
		}
		if out.SequenceNumber != 11 {
			t.Errorf("seq = %d, want 11 — a second switch must not consume an extra slot", out.SequenceNumber)
		}
	})
}

// captureTrack is a downstream leg's track, recording what actually reached the
// wire. It stands in for *webrtc.TrackLocalStaticRTP so the fan-out can be tested
// with no PeerConnection at all.
type captureTrack struct {
	mu   sync.Mutex
	pkts []*rtp.Packet
}

func (c *captureTrack) WriteRTP(p *rtp.Packet) error {
	c.mu.Lock()
	c.pkts = append(c.pkts, p)
	c.mu.Unlock()
	return nil
}

func (c *captureTrack) snapshot() []*rtp.Packet {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*rtp.Packet(nil), c.pkts...)
}

// TestForwarderMakeBeforeBreak is the whole point of C12, at the level the bug
// actually bites: during a re-parent TWO upstream read loops are alive at once and
// both want to write the SAME downstream leg. The shipped code lets both write, so
// the child sees two interleaved, unrelated sequence series.
//
// Discrimination: the assertions are (1) no packet from the second upstream reaches
// the child before the re-parent commits, (2) no packet from the FIRST upstream
// reaches it after, and (3) the emitted sequence numbers form one continuous run.
// The shipped fan-out fails all three — it has no notion of which upstream is
// authoritative and rewrites nothing.
func TestForwarderMakeBeforeBreak(t *testing.T) {
	f := newForwarder(discardLog(), &uploadMeter{}, func(func()) {}, newFakeClock())
	old := &captureRTCP{}
	f.setUpstream("S", old)
	child := &captureTrack{}
	f.addOutLive("S", "C", child, nil)

	// The original upstream is live and feeding the child.
	genOld := f.newUpstreamGen("S")
	f.fanout("S", genOld, vp8Pkt(1000, 90000, true))
	f.fanout("S", genOld, vp8Pkt(1001, 93000, false))

	// Make-before-break: the new parent connects and its forwarded track arrives
	// while the old one is still delivering.
	genNew := f.newUpstreamGen("S")
	f.fanout("S", genNew, vp8Pkt(60000, 5000, true))
	f.fanout("S", genOld, vp8Pkt(1002, 96000, false))

	if got := len(child.snapshot()); got != 3 {
		t.Fatalf("child received %d packets during the overlap, want 3 — the new upstream "+
			"must not write downstream until the re-parent commits", got)
	}

	// Commit: the forwarder is rebound to the new upstream.
	next := &captureRTCP{}
	f.rebindUpstream("S", next)

	// The old loop is still draining its dying track; nothing it writes may land.
	f.fanout("S", genOld, vp8Pkt(1003, 99000, false))
	f.fanout("S", genNew, vp8Pkt(60001, 8000, false)) // interframe: dropped by the latch
	f.fanout("S", genNew, vp8Pkt(60002, 11000, true)) // keyframe: resumes the leg
	f.fanout("S", genNew, vp8Pkt(60003, 14000, false))

	got := child.snapshot()
	if len(got) != 5 {
		t.Fatalf("child received %d packets, want 5 (3 pre-commit + the resuming keyframe + 1)", len(got))
	}
	wantSeq := []uint16{1000, 1001, 1002, 1003, 1004}
	for i, p := range got {
		if p.SequenceNumber != wantSeq[i] {
			t.Errorf("packet %d seq = %d, want %d — the downstream series must be continuous "+
				"across the upstream switch", i, p.SequenceNumber, wantSeq[i])
		}
	}
	// The commit boundary advances the timestamp by exactly one nominal frame.
	if got[3].Timestamp != 96000+TimestampGapTicks {
		t.Errorf("first post-switch ts = %d, want %d", got[3].Timestamp, 96000+TimestampGapTicks)
	}
	if got[4].Timestamp != 96000+TimestampGapTicks+3000 {
		t.Errorf("second post-switch ts = %d, want %d", got[4].Timestamp, 96000+TimestampGapTicks+3000)
	}

	// Rebinding also resets the learned SSRC, so the next upstream PLI cannot carry
	// the OLD source's SSRC (which the new upstream would silently ignore).
	f.mu.RLock()
	ssrc := f.sources["S"].ssrc.Load()
	up := f.sources["S"].upstream
	f.mu.RUnlock()
	if ssrc != 0 {
		t.Errorf("ssrc = %#x after rebind, want 0 so the next TrackRemote re-learns it", ssrc)
	}
	if up != next {
		t.Error("rebindUpstream did not repoint the keyframe-request target")
	}
}

// TestForwarderLegMutation pins the mid-call leg surface the Router needs: legs can
// be added and dropped on a session that is already running, and dropping a SOURCE
// hands back the senders so the Router can RemoveTrack them instead of leaving them
// lingering (the limitation the shipped removeSource documented).
func TestForwarderLegMutation(t *testing.T) {
	senders := newSenders(t, 3)
	f := newForwarder(discardLog(), &uploadMeter{}, func(func()) {}, newFakeClock())
	f.setUpstream("S", &captureRTCP{})
	f.addOutLive("S", "C1", &captureTrack{}, senders[0])
	f.addOutLive("S", "C2", &captureTrack{}, senders[1])

	if got := f.legs(); len(got) != 2 {
		t.Fatalf("legs = %v, want two", got)
	}
	if got := f.removeOut("S", "C1"); got != senders[0] {
		t.Fatalf("removeOut returned %v, want C1's sender so the Router can RemoveTrack it", got)
	}
	if got := f.legs(); len(got) != 1 || got[0] != (leg{src: "S", child: "C2"}) {
		t.Fatalf("legs after removeOut = %v, want just S→C2", got)
	}
	if f.removeOut("S", "C1") != nil {
		t.Error("removeOut is not idempotent; a second call must report no such leg")
	}

	// removeSource must return the senders of every remaining leg so nothing is
	// left forwarding a departed source's media.
	f.addOutLive("S", "C3", &captureTrack{}, senders[2])
	dropped := f.removeSource("S")
	if len(dropped) != 2 {
		t.Fatalf("removeSource returned %d senders, want 2 (C2 and C3)", len(dropped))
	}
	if got := f.legs(); len(got) != 0 {
		t.Fatalf("legs after removeSource = %v, want none", got)
	}
}

// newSenders mints n real RTPSenders on a throwaway PeerConnection. They are never
// negotiated — the forwarder only ever stores and hands them back — but using the
// real type keeps the leg bookkeeping honest about what it carries.
func newSenders(t *testing.T, n int) []*webrtc.RTPSender {
	t.Helper()
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("new peer connection: %v", err)
	}
	t.Cleanup(func() { _ = pc.Close() })
	out := make([]*webrtc.RTPSender, 0, n)
	for i := 0; i < n; i++ {
		track, err := webrtc.NewTrackLocalStaticRTP(
			webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8, ClockRate: 90000},
			fmt.Sprintf("fwd-%d", i), "conclave")
		if err != nil {
			t.Fatalf("new track: %v", err)
		}
		sender, err := pc.AddTrack(track)
		if err != nil {
			t.Fatalf("add track: %v", err)
		}
		out = append(out, sender)
	}
	return out
}

// TestRTPRewriterIgnoresOutOfOrder pins the boundary the switch rebases off.
//
// A NACK retransmission or a reordered packet arrives with an OLDER sequence number.
// Advancing the leg's high-water mark on it drags the mark backwards, so the next
// Switch rebases from there and the new upstream is spliced on top of a range the
// child has already seen — a duplicate-sequence overlap, which a jitter buffer reads
// as corruption rather than as loss.
//
// The high-water mark must therefore only ever move FORWARD, in uint16 wrapping
// order, while the retransmitted packet itself is still forwarded (dropping it would
// defeat the NACK that asked for it).
func TestRTPRewriterIgnoresOutOfOrder(t *testing.T) {
	w := &rtpRewriter{}
	for _, p := range []*rtp.Packet{
		vp8Pkt(100, 1000, true), vp8Pkt(101, 4000, false), vp8Pkt(102, 7000, false),
	} {
		if w.Rewrite(p) == nil {
			t.Fatal("packet dropped before any switch")
		}
	}
	// A retransmission of an already-sent packet: forwarded, but it must not move
	// the mark back to 100.
	if got := w.Rewrite(vp8Pkt(100, 1000, true)); got == nil {
		t.Fatal("a retransmitted packet was dropped; the NACK that asked for it goes unanswered")
	} else if got.SequenceNumber != 100 {
		t.Errorf("retransmit rewritten to seq %d, want 100 (offsets are unchanged)", got.SequenceNumber)
	}

	w.Switch()
	out := w.Rewrite(vp8Pkt(60000, 900000, true))
	if out == nil {
		t.Fatal("the keyframe after a switch was dropped")
	}
	if out.SequenceNumber != 103 {
		t.Errorf("post-switch seq = %d, want 103; rebasing off a retransmitted packet would "+
			"restart at 101 and overlap sequence numbers the child has already seen",
			out.SequenceNumber)
	}
	if out.Timestamp != 7000+TimestampGapTicks {
		t.Errorf("post-switch ts = %d, want %d", out.Timestamp, 7000+TimestampGapTicks)
	}
}
