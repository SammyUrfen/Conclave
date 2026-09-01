package media

import (
	"sync"

	"github.com/pion/rtp"
	"github.com/pion/rtp/codecs"
)

// TimestampGapTicks is the timestamp advance inserted at a Switch when the true
// inter-frame delta is unknown (the first packet after a switch). 3000 = one frame
// at 30fps on VP8's 90kHz clock. Being slightly wrong here costs a few ms of A/V
// drift, not a broken stream; being DISCONTINUOUS here costs the stream.
const TimestampGapTicks = 3000

// rtpRewriter makes ONE downstream leg's RTP stream continuous across an upstream
// switch. It exists because of a property of pion that is correct until the moment
// a relay re-parents: TrackLocalStaticRTP.WriteRTP rewrites SSRC and payload type
// per binding and passes SEQUENCE NUMBER and TIMESTAMP through untouched. That is
// exactly right while one source feeds one leg forever, and exactly wrong the
// instant a second, unrelated upstream starts feeding the same leg — the child sees
// a stream that jumps backwards and forwards in sequence space, which pion's own
// NACK and jitter-buffer interceptors read as catastrophic loss.
//
// A keyframe does NOT repair that. A keyframe fixes reference state; the damage
// here is to transport ordering, one layer below.
//
// The mechanism is the standard SFU one: a per-leg (sequence, timestamp) OFFSET,
// recomputed only at a Switch. Offsets rather than a counter because gaps WITHIN an
// upstream must survive to the child — a lost packet has to stay visible or loss
// reporting silently stops working — while the discontinuity BETWEEN upstreams must
// not.
//
// Guarded by its own mutex: the forward loop is normally its only writer, but
// during a re-parent two forward loops exist and Switch is called from the Router's
// Run goroutine.
type rtpRewriter struct {
	mu sync.Mutex

	started bool   // has any packet been written on this leg?
	lastSeq uint16 // last OUTGOING sequence number written
	lastTS  uint32 // last OUTGOING timestamp written

	seqOff uint16 // outgoing = incoming + seqOff (mod 2^16)
	tsOff  uint32 // outgoing = incoming + tsOff  (mod 2^32)

	// waitKey latches after a Switch: packets are dropped until a VP8 keyframe
	// arrives from the new upstream. A continuous-but-undecodable stream is not an
	// improvement over a gap — the decoder would render garbage rather than freeze.
	waitKey bool
	// rebase is set with waitKey and cleared when the offsets are recomputed on the
	// first accepted packet of the new upstream.
	rebase bool
}

// Rewrite adapts a packet for this leg and returns the copy to write downstream, or
// nil when the packet must be dropped (the drop-until-keyframe window after a
// Switch). It always copies: several legs share one upstream packet, and each needs
// its own header.
func (w *rtpRewriter) Rewrite(pkt *rtp.Packet) *rtp.Packet {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.waitKey {
		if !isVP8Keyframe(pkt.Payload) {
			return nil
		}
		w.waitKey = false
	}
	if w.rebase {
		// Continue the outgoing series from where it stopped. On a leg that has
		// never written anything the incoming numbering is adopted as-is, so a leg
		// created mid-call does not start at an arbitrary offset.
		if w.started {
			w.seqOff = w.lastSeq + 1 - pkt.SequenceNumber
			w.tsOff = w.lastTS + TimestampGapTicks - pkt.Timestamp
		} else {
			w.seqOff, w.tsOff = 0, 0
		}
		w.rebase = false
	}

	out := *pkt // header copy
	out.Payload = pkt.Payload
	out.SequenceNumber = pkt.SequenceNumber + w.seqOff
	out.Timestamp = pkt.Timestamp + w.tsOff

	w.started = true
	w.lastSeq = out.SequenceNumber
	w.lastTS = out.Timestamp
	return &out
}

// Switch declares that the next packet comes from a DIFFERENT upstream. It arms the
// drop-until-keyframe latch and defers the offset recomputation to the first packet
// that is actually accepted, because only that packet's numbering is meaningful.
//
// Calling it twice before the leg resumes is harmless and must stay so: a topology
// can supersede a re-parent that is still in flight.
func (w *rtpRewriter) Switch() {
	w.mu.Lock()
	w.waitKey = true
	w.rebase = true
	w.mu.Unlock()
}

// isVP8Keyframe reports whether an RTP payload carries the start of a VP8 keyframe.
//
// Three conditions, all necessary: the payload descriptor must mark the start of a
// partition (S == 1) and the FIRST partition (PID == 0) — otherwise this is a
// continuation fragment whose payload bytes are not a frame tag at all — and bit 0
// of the VP8 frame tag must be clear, which is VP8's own "this is a key frame".
//
// Being wrong either way is silent: too strict and a switched leg never resumes;
// too loose and it resumes on an interframe the decoder cannot use.
func isVP8Keyframe(payload []byte) bool {
	if len(payload) == 0 {
		return false
	}
	var vp8 codecs.VP8Packet
	frame, err := vp8.Unmarshal(payload)
	if err != nil || len(frame) == 0 {
		return false
	}
	if vp8.S != 1 || vp8.PID != 0 {
		return false
	}
	return frame[0]&0x01 == 0
}
