package media

import (
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"
)

// rtcpWriter is the slice of a Session the forwarder needs to push a keyframe
// request toward a source: just WriteRTCP. Declaring the interface here
// (consumer-defined) keeps the forwarder off the concrete *Session, so its SSRC
// translation can be unit-tested with a fake. *Session satisfies it.
type rtcpWriter interface {
	WriteRTCP([]rtcp.Packet) error
}

// pliThrottle bounds how often the relay forwards a keyframe request upstream per
// source. Several children converging on a fresh stream (or a chatty decoder) would
// otherwise storm the sender with redundant PLIs; one every ~300 ms is plenty to
// recover a keyframe without wasting the sender's bitrate on extra I-frames.
const pliThrottle = 300 * time.Millisecond

// forwarder is the relay's core — the "selective forwarding" in SFU. It turns each
// inbound track (one per source the relay receives) into an RTP fan-out toward the
// downstreams that should get it, and plumbs keyframe requests (PLI) back upstream
// toward the original sender. It is owned by the Router and exists only on a relay.
//
// Shape: one reader goroutine per source reads that source's TrackRemote and writes
// every packet to each downstream forwarding track — the classic one-producer,
// many-consumer fan-out. No decode, no re-encode; each hop is just
// SRTP-decrypt → rewrite SSRC/PayloadType (pion does this per binding) → SRTP-encrypt.
type forwarder struct {
	log   *slog.Logger
	meter *uploadMeter
	// spawn runs fn as a Router-tracked goroutine (wg.Add/Done) so shutdown joins
	// every RTCP-drain the forwarder starts — no leaks, same discipline as the mesh.
	spawn func(fn func())

	pliForwarded atomic.Int64 // upstream PLIs sent; asserted by the relay test

	mu      sync.RWMutex
	sources map[string]*forwardSource // keyed by SOURCE peer name
}

// forwardSource is everything about one media source S the relay receives: the
// session S's media arrives on (where an upstream PLI is written), S's SSRC
// (captured once the track arrives — the MediaSSRC an upstream PLI MUST carry), a
// throttle clock, and the per-child outbound legs carrying S's media downstream.
type forwardSource struct {
	name     string
	upstream rtcpWriter    // session toward S; target of upstream keyframe requests
	ssrc     atomic.Uint32 // S's upstream SSRC; 0 until S's track arrives
	lastPLI  atomic.Int64  // unixnano of the last upstream PLI, for throttling
	outs     []*forwardOut // downstream legs (guarded by forwarder.mu)
}

// forwardOut is one downstream leg: the child that receives source S's media, the
// track pion fans S's packets onto (one track per (source, child) so each child's
// write errors and teardown are isolated), and its sender — drained for RTCP so
// the child's PLIs are caught and its NACK/interceptor chain keeps running.
type forwardOut struct {
	child  string
	track  *webrtc.TrackLocalStaticRTP
	sender *webrtc.RTPSender
}

func newForwarder(log *slog.Logger, meter *uploadMeter, spawn func(func())) *forwarder {
	return &forwarder{
		log:     log.With(slog.String("component", "relay")),
		meter:   meter,
		spawn:   spawn,
		sources: make(map[string]*forwardSource),
	}
}

// ensureSource returns the forwardSource for src, creating it if needed. Caller
// holds f.mu. Setup registers a source from two independent startPeer calls (its
// own edge sets upstream; each other edge adds a downstream leg), in any order, so
// both entry points create-or-reuse the same record.
func (f *forwarder) ensureSource(src string) *forwardSource {
	s := f.sources[src]
	if s == nil {
		s = &forwardSource{name: src}
		f.sources[src] = s
	}
	return s
}

// setUpstream records the session that source src's media arrives on — the target
// of any keyframe request the relay forwards upstream for src. Called from src's
// own edge setup.
func (f *forwarder) setUpstream(src string, upstream rtcpWriter) {
	f.mu.Lock()
	f.ensureSource(src).upstream = upstream
	f.mu.Unlock()
}

// addOut registers that source src's media should be forwarded to child dst over
// track/sender, and starts draining that sender's RTCP so the child's keyframe
// requests reach the source (and its NACK/interceptor chain keeps running). Called
// before Start, once per (src, dst) pair, from dst's edge setup.
func (f *forwarder) addOut(src, dst string, track *webrtc.TrackLocalStaticRTP, sender *webrtc.RTPSender) {
	f.mu.Lock()
	s := f.ensureSource(src)
	s.outs = append(s.outs, &forwardOut{child: dst, track: track, sender: sender})
	f.mu.Unlock()

	// Each downstream leg counts as one outbound stream on the relay's upload meter,
	// so its aggregate rate shows the relay uploading O(children)·(source bitrate) —
	// exactly the cost the elected-SFU concentrates onto the relay so the leaves
	// stay flat. The gauge drops back when the drain exits (child gone / shutdown).
	f.meter.addPeer(1)
	f.spawn(func() {
		defer f.meter.addPeer(-1)
		f.drainRTCP(src, sender)
	})
}

// forward is the read→fan-out loop for one source. It runs on the pion OnTrack
// goroutine (the Router hands it the TrackRemote), blocking until the source
// leaves. It records the source SSRC first (needed to translate downstream PLIs),
// then copies each packet to every registered downstream leg.
func (f *forwarder) forward(src string, remote *webrtc.TrackRemote) {
	f.mu.RLock()
	s := f.sources[src]
	f.mu.RUnlock()
	if s == nil {
		// A source we received but forward to nobody (e.g. a childless relay leg).
		// Drain it so its receiver doesn't stall, but there is nothing to fan out.
		f.log.Debug("source with no downstreams; draining", slog.String("source", src))
		for {
			if _, _, err := remote.ReadRTP(); err != nil {
				return
			}
		}
	}

	s.ssrc.Store(uint32(remote.SSRC()))
	f.log.Info("forwarding source", slog.String("source", src), slog.Any("ssrc", remote.SSRC()))
	// The source just started: ask it for a keyframe so any already-connected
	// downstream can begin decoding immediately rather than waiting for the next
	// natural I-frame. (Late joiners are covered by the on-connect request.)
	f.requestUpstreamKeyframe(src)

	for {
		pkt, _, err := remote.ReadRTP()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				f.log.Debug("source read ended", slog.String("source", src), slog.Any("error", err))
			}
			return // source gone; its downstream tracks simply stop receiving writes
		}

		f.mu.RLock()
		outs := s.outs
		f.mu.RUnlock()
		for _, o := range outs {
			// WriteRTP deep-copies the packet and rewrites SSRC/PayloadType to this
			// child's negotiated values, leaving sequence/timestamp intact — correct
			// for one source → one track.
			if err := o.track.WriteRTP(pkt); err != nil {
				// A closed pipe means the child's SRTP isn't ready yet or it has left
				// — nothing reached the wire, so it must NOT be metered (that would
				// keep inflating the relay's reported upload after a child departs).
				// A real error is worth a line; either way, never block the siblings.
				if !errors.Is(err, io.ErrClosedPipe) {
					f.log.Warn("forward write", slog.String("child", o.child), slog.Any("error", err))
				}
				continue
			}
			// Meter only bytes that actually went out, once per downstream leg — the
			// relay really does put one copy on the wire per child, so this is the
			// true upload it concentrates, not double-counting.
			f.meter.addBytes(pkt.MarshalSize())
		}
	}
}

// drainRTCP reads RTCP coming back from one downstream child on a forwarded track.
// It MUST run for the whole life of the sender: it is what pumps the per-sender
// interceptor chain (NACK responder, reports) and what catches the child's keyframe
// requests. On PLI/FIR it asks the source for a keyframe. It returns when the child
// leaves (its sender closes), which is the signal to drop this leg's meter count.
func (f *forwarder) drainRTCP(src string, sender *webrtc.RTPSender) {
	for {
		pkts, _, err := sender.ReadRTCP()
		if err != nil {
			return // io.ErrClosedPipe when the child's sender stops → child gone
		}
		for _, p := range pkts {
			switch p.(type) {
			case *rtcp.PictureLossIndication, *rtcp.FullIntraRequest:
				// Normalise PLI and FIR to one upstream PLI — we don't need FIR's
				// per-request sequence bookkeeping, just "send a keyframe".
				f.requestUpstreamKeyframe(src)
			}
		}
	}
}

// requestUpstreamKeyframe forwards a keyframe request to the ORIGINAL sender of
// src's media. The one thing that must be right: the PLI's MediaSSRC is src's
// UPSTREAM SSRC, not the downstream SSRC the child's PLI carried. A child's PLI
// names the SSRC the relay assigned it; forwarding that verbatim would make the
// source ignore it (unknown SSRC) → black video with no error. We build a fresh PLI
// with the source's own SSRC. Throttled so converging children can't storm it.
func (f *forwarder) requestUpstreamKeyframe(src string) {
	// Capture the mutable upstream field UNDER the lock: setUpstream rewrites it on a
	// same-name rejoin, so reading it after unlocking would be a data race. (ssrc /
	// lastPLI are atomics and safe to touch outside the lock.)
	f.mu.RLock()
	s := f.sources[src]
	var upstream rtcpWriter
	if s != nil {
		upstream = s.upstream
	}
	f.mu.RUnlock()
	if s == nil || upstream == nil {
		return
	}
	ssrc := s.ssrc.Load()
	if ssrc == 0 {
		return // source media isn't flowing yet — nothing to request a keyframe of
	}

	// Throttle per SOURCE, not per child, on purpose: a keyframe is not
	// per-subscriber — when the source emits one I-frame it is forwarded to EVERY
	// downstream leg, so one request per window serves all children that just
	// joined. The clock is advanced optimistically (before the best-effort WriteRTCP
	// below, which cannot confirm delivery anyway): a rarely-dropped request just
	// waits one window for the decoder's next PLI, which is the right trade to never
	// storm the source.
	now := time.Now().UnixNano()
	last := s.lastPLI.Load()
	if now-last < int64(pliThrottle) {
		return
	}
	if !s.lastPLI.CompareAndSwap(last, now) {
		return // another goroutine just sent one; don't double up
	}

	if err := upstream.WriteRTCP([]rtcp.Packet{&rtcp.PictureLossIndication{MediaSSRC: ssrc}}); err != nil {
		f.log.Debug("upstream keyframe request failed", slog.String("source", src), slog.Any("error", err))
		return
	}
	f.pliForwarded.Add(1)
	f.log.Debug("forwarded keyframe request upstream",
		slog.String("source", src), slog.Any("ssrc", ssrc))
}

// keyframeForChild proactively asks upstream for a keyframe for every source that
// child receives — called when the child's connection comes up, so a fresh joiner
// gets an I-frame immediately instead of waiting for the next natural one (or for
// the decoder to ask). No-ops for sources whose media isn't flowing yet.
func (f *forwarder) keyframeForChild(child string) {
	f.mu.RLock()
	var srcs []string
	for _, s := range f.sources {
		for _, o := range s.outs {
			if o.child == child {
				srcs = append(srcs, s.name)
				break
			}
		}
	}
	f.mu.RUnlock()
	for _, src := range srcs {
		f.requestUpstreamKeyframe(src)
	}
}

// removeChild prunes every forwarding leg toward a departed child, so the forward
// loop stops iterating dead legs and outs cannot grow without bound across
// join/leave churn. The child's RTCP-drain goroutine exits on its own (its sender
// closes), which is what drops the meter gauge — this only trims the slices. A fresh
// slice is allocated rather than mutated in place so a concurrent forward() holding
// an older snapshot is unaffected.
func (f *forwarder) removeChild(child string) {
	f.mu.Lock()
	for _, s := range f.sources {
		kept := make([]*forwardOut, 0, len(s.outs))
		for _, o := range s.outs {
			if o.child != child {
				kept = append(kept, o)
			}
		}
		s.outs = kept
	}
	f.mu.Unlock()
}

// PLIForwarded reports how many keyframe requests the relay has sent upstream.
// Used by the relay test to prove the PLI path fired.
func (f *forwarder) PLIForwarded() int { return int(f.pliForwarded.Load()) }
