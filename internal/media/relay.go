package media

import (
	"errors"
	"io"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"

	"github.com/SammyUrfen/conclave/internal/clock"
)

// rtcpWriter is the slice of a Session the forwarder needs to push a keyframe
// request toward a source: just WriteRTCP. Declaring the interface here
// (consumer-defined) keeps the forwarder off the concrete *Session, so its SSRC
// translation can be unit-tested with a fake. *Session satisfies it.
type rtcpWriter interface {
	WriteRTCP([]rtcp.Packet) error
}

// rtcpReader is the slice of an RTPSender the drain needs: just ReadRTCP.
// *webrtc.RTPSender satisfies it. Declared here for the same reason as rtcpWriter —
// without it drainRTCP's loop can only be exercised through a negotiated
// PeerConnection, so the one line that hands packets to handleRTCP is unreachable
// from a test, and "the dispatch is correct but the loop never calls it" is exactly
// the failure a loss reading of 0 cannot be distinguished from.
type rtcpReader interface {
	ReadRTCP() ([]rtcp.Packet, interceptor.Attributes, error)
}

// rtpTrack is the slice of a downstream track the fan-out needs: just WriteRTP.
// *webrtc.TrackLocalStaticRTP satisfies it. Declaring it here is what lets the
// make-before-break continuity logic (rtpRewriter) be tested against a recording
// double instead of a negotiated PeerConnection — the packets that reach the wire
// are the whole assertion, and a real track will not show them to a test.
type rtpTrack interface {
	WriteRTP(*rtp.Packet) error
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
//
// From Phase 5 a source may have MORE THAN ONE reader alive at once: a re-parent
// opens the new upstream before tearing the old one down, so two TrackRemotes carry
// the same logical source for a few hundred milliseconds. Exactly one of them is
// authoritative at any instant — see forwardSource.activeGen.
type forwarder struct {
	log   *slog.Logger
	meter *uploadMeter
	clk   clock.Clock
	// spawn runs fn as a Router-tracked goroutine (wg.Add/Done) so shutdown joins
	// every RTCP-drain the forwarder starts — no leaks, same discipline as the mesh.
	spawn func(fn func())

	pliForwarded atomic.Int64 // upstream PLIs sent; asserted by the relay test

	// loss is the uplink-loss sensor, fed from drainRTCP's reception reports. It
	// carries its own mutex rather than living under f.mu because it is written on
	// every child's drain goroutine and read on the metrics reporter's, and it shares
	// no invariant with the source map f.mu guards.
	loss lossTracker

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
	ssrc     atomic.Uint32 // S's ACTIVE upstream SSRC; 0 until S's track arrives
	lastPLI  atomic.Int64  // unixnano of the last upstream PLI, for throttling
	outs     []*forwardOut // downstream legs (guarded by forwarder.mu)

	// Upstream generations, all guarded by forwarder.mu. A generation is one reader
	// loop over one TrackRemote. During a re-parent two exist; only activeGen may
	// write downstream, so the child keeps seeing the working stream until the
	// switch is committed and never sees the two interleaved.
	nextGen   uint64            // highest generation handed out
	activeGen uint64            // the generation currently allowed to write
	genSSRC   map[uint64]uint32 // per-generation learned SSRC, promoted on activation
	awaiting  bool              // rebound with no newer generation yet: the next one wins
}

// forwardOut is one downstream leg: the child that receives source S's media, the
// track pion fans S's packets onto (one track per (source, child) so each child's
// write errors and teardown are isolated), its sender — drained for RTCP so the
// child's PLIs are caught and its NACK/interceptor chain keeps running — and the
// rewriter that keeps this leg's RTP stream continuous across an upstream switch.
type forwardOut struct {
	child  string
	track  rtpTrack
	sender *webrtc.RTPSender
	rw     *rtpRewriter
}

// leg names one (source → child) forwarding relationship. It is the unit the
// topology diff adds and removes, and the unit the forwarder stores.
type leg struct {
	src   string
	child string
}

func newForwarder(log *slog.Logger, meter *uploadMeter, spawn func(func()), clk clock.Clock) *forwarder {
	if clk == nil {
		clk = clock.System()
	}
	return &forwarder{
		log:     log.With(slog.String("component", "relay")),
		meter:   meter,
		clk:     clk,
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
		s = &forwardSource{name: src, genSSRC: make(map[uint64]uint32)}
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

// rebindUpstream points src's forwardSource at a new upstream session after a
// re-parent and COMMITS the switch: the newest reader generation becomes the
// authoritative one and every downstream leg is told to splice.
//
// The SSRC is reset (to the new generation's, or 0 if its track has not arrived
// yet) because an upstream PLI carrying the OLD source SSRC would be silently
// ignored by the new upstream — the exact black-video failure
// requestUpstreamKeyframe already documents.
//
// Both orderings are handled: if the new upstream's track already arrived (a
// generation newer than the active one exists) it is promoted now; if it has not,
// the source is armed so the NEXT generation is promoted the moment it appears.
func (f *forwarder) rebindUpstream(src string, upstream rtcpWriter) {
	f.renameSource(src, src, upstream)
}

// renameSource is rebindUpstream for the case where the source's KEY changes too.
//
// It is needed because a relay keys each forwardSource by the NEIGHBOUR it arrives
// from, so when our parent changes the media we were forwarding stops arriving
// under the old parent's name and starts arriving under the new one's. Moving the
// legs across (rather than dropping and re-adding them) is what makes §7.3's
// promise true: the children's forwarded tracks are the same objects throughout, so
// a re-parent costs the subtree no renegotiation at all.
//
// from == to is the ordinary same-key rebind.
func (f *forwarder) renameSource(from, to string, upstream rtcpWriter) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var moved []*forwardOut
	if from != to {
		if src := f.sources[from]; src != nil {
			moved = src.outs
			delete(f.sources, from)
		}
	}
	dst := f.ensureSource(to)
	dst.upstream = upstream
	dst.outs = append(dst.outs, moved...)
	dst.commitSwitch()
}

// commitSwitch promotes the newest reader generation, or arms the source so the
// next one to appear is promoted on arrival, and splices every downstream leg
// either way. Caller holds f.mu.
func (s *forwardSource) commitSwitch() {
	if s.nextGen > s.activeGen {
		s.activate(s.nextGen)
		return
	}
	s.awaiting = true
	s.ssrc.Store(0)
	for _, o := range s.outs {
		o.rw.Switch()
	}
}

// activate promotes gen to the authoritative generation and splices every
// downstream leg onto it. Caller holds f.mu.
func (s *forwardSource) activate(gen uint64) {
	s.activeGen = gen
	s.awaiting = false
	s.ssrc.Store(s.genSSRC[gen]) // 0 when that generation's track has not arrived
	for g := range s.genSSRC {
		if g < gen {
			delete(s.genSSRC, g) // retired readers cannot come back
		}
	}
	for _, o := range s.outs {
		o.rw.Switch()
	}
}

// newUpstreamGen registers one reader loop over one TrackRemote for src and returns
// its generation token, which the loop passes to every fanout call. The FIRST
// generation is authoritative immediately (nothing to switch from); a later one is
// silent until the Router commits the re-parent — unless a rebind already committed
// while waiting for it, in which case it takes over on arrival.
func (f *forwarder) newUpstreamGen(src string) uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := f.ensureSource(src)
	s.nextGen++
	gen := s.nextGen
	switch {
	case s.awaiting:
		// A rebind already committed while waiting for this reader to appear.
		s.activate(gen)
	case s.activeGen == 0:
		s.activeGen = gen
	}
	return gen
}

// registerOut is the shared bookkeeping behind addOut and addOutLive.
func (f *forwarder) registerOut(src, dst string, track rtpTrack, sender *webrtc.RTPSender) {
	f.mu.Lock()
	s := f.ensureSource(src)
	s.outs = append(s.outs, &forwardOut{child: dst, track: track, sender: sender, rw: &rtpRewriter{}})
	f.mu.Unlock()

	if sender == nil {
		return // nothing to drain (test double); the leg is still metered on write
	}
	// Each downstream leg counts as one outbound stream on the relay's upload meter,
	// so its aggregate rate shows the relay uploading O(children)·(source bitrate) —
	// exactly the cost the elected-SFU concentrates onto the relay so the leaves
	// stay flat. The gauge drops back when the drain exits (child gone / shutdown).
	f.meter.addPeer(1)
	f.spawn(func() {
		defer f.meter.addPeer(-1)
		f.drainRTCP(src, dst, sender)
	})
}

// addOut registers that source src's media should be forwarded to child dst over
// track/sender, and starts draining that sender's RTCP so the child's keyframe
// requests reach the source (and its NACK/interceptor chain keeps running). Called
// BEFORE Start, once per (src, dst) pair, from dst's edge setup — so the forwarded
// m-line rides the session's first offer with no renegotiation.
func (f *forwarder) addOut(src, dst string, track rtpTrack, sender *webrtc.RTPSender) {
	f.registerOut(src, dst, track, sender)
}

// addOutLive is addOut for a leg created AFTER the session has started — the
// mid-call case, where the track was added by AddForwardTrack on a live pc and the
// serializer folds it into the next offer. Identical bookkeeping; it exists as a
// separate name purely so the "added before Start" precondition on addOut stays a
// true, checkable statement.
func (f *forwarder) addOutLive(src, dst string, track rtpTrack, sender *webrtc.RTPSender) {
	f.registerOut(src, dst, track, sender)
}

// removeOut drops the single (src → child) leg and returns its sender so the Router
// can RemoveTrack it. Returns nil if no such leg exists (idempotent).
func (f *forwarder) removeOut(src, child string) *webrtc.RTPSender {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := f.sources[src]
	if s == nil {
		return nil
	}
	var sender *webrtc.RTPSender
	found := false
	kept := make([]*forwardOut, 0, len(s.outs))
	for _, o := range s.outs {
		if !found && o.child == child {
			found = true
			sender = o.sender // may be nil: the leg is still dropped
			continue
		}
		kept = append(kept, o)
	}
	if !found {
		return nil // no such leg; removeOut is idempotent
	}
	s.outs = kept
	return sender
}

// sourcesVia reports every source whose media is arriving over one upstream session
// — the whole far side of that edge, since a source is keyed by ORIGIN and one edge
// carries every origin behind it. Sorted, so teardown is deterministic.
func (f *forwarder) sourcesVia(up rtcpWriter) []string {
	f.mu.RLock()
	var out []string
	for name, s := range f.sources {
		if s.upstream != nil && s.upstream == up {
			out = append(out, name)
		}
	}
	f.mu.RUnlock()
	sort.Strings(out)
	return out
}

// rebindUpstreamSession re-points every source fed by old onto next and splices each
// affected downstream leg. This is the re-parent commit: the source KEYS do not
// change — a source is named for who originated it, and a new parent relays the same
// origins — so the children's forwarded tracks are the same objects throughout and
// the whole subtree pays no renegotiation.
func (f *forwarder) rebindUpstreamSession(old, next rtcpWriter) {
	for _, src := range f.sourcesVia(old) {
		f.renameSource(src, src, next)
	}
}

// legs reports every (source → child) relationship the forwarder currently holds,
// sorted, so the Router can diff it against a topology deterministically.
func (f *forwarder) legs() []leg {
	f.mu.RLock()
	var out []leg
	for src, s := range f.sources {
		for _, o := range s.outs {
			out = append(out, leg{src: src, child: o.child})
		}
	}
	f.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool {
		if out[i].src != out[j].src {
			return out[i].src < out[j].src
		}
		return out[i].child < out[j].child
	})
	return out
}

// forward is the read→fan-out loop for one source over ONE upstream track. It runs
// on the pion OnTrack goroutine (the Router hands it the TrackRemote), blocking
// until that upstream ends. It records the source SSRC first (needed to translate
// downstream PLIs), then copies each packet to every registered downstream leg.
//
// A loop whose generation is not the active one still READS — a TrackRemote that is
// not drained stalls its receiver and its interceptor chain — but its packets are
// discarded rather than interleaved into the children's streams.
func (f *forwarder) forward(src string, remote *webrtc.TrackRemote) {
	f.mu.RLock()
	known := f.sources[src] != nil
	f.mu.RUnlock()
	if !known {
		// A source we received but forward to nobody (e.g. a childless relay leg).
		// Drain it so its receiver doesn't stall, but there is nothing to fan out.
		f.log.Debug("source with no downstreams; draining", slog.String("source", src))
		for {
			if _, _, err := remote.ReadRTP(); err != nil {
				return
			}
		}
	}

	gen := f.newUpstreamGen(src)
	f.learnSSRC(src, gen, uint32(remote.SSRC()))
	f.log.Info("forwarding source",
		slog.String("source", src), slog.Any("ssrc", remote.SSRC()), slog.Uint64("gen", gen))
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
			return // upstream gone; its downstream tracks simply stop receiving writes
		}
		f.fanout(src, gen, pkt)
	}
}

// learnSSRC records the SSRC of one upstream generation, promoting it to the
// source's live SSRC only if that generation is the authoritative one. Storing it
// unconditionally would point upstream PLIs at the pending parent's stream while
// the old parent is still the one being watched.
func (f *forwarder) learnSSRC(src string, gen uint64, ssrc uint32) {
	f.mu.Lock()
	s := f.ensureSource(src)
	s.genSSRC[gen] = ssrc
	if gen == s.activeGen {
		s.ssrc.Store(ssrc)
	}
	f.mu.Unlock()
}

// fanout writes one packet from upstream generation gen to every downstream leg of
// src, or discards it when gen is not the authoritative generation.
//
// Each leg gets its OWN rewritten copy: the outgoing sequence and timestamp are
// continuous per leg across an upstream switch (see rtpRewriter), and the copy is
// what makes that safe when several legs share one upstream packet.
func (f *forwarder) fanout(src string, gen uint64, pkt *rtp.Packet) {
	f.mu.RLock()
	s := f.sources[src]
	var outs []*forwardOut
	var active uint64
	if s != nil {
		outs = s.outs
		active = s.activeGen
	}
	f.mu.RUnlock()
	if s == nil || gen != active {
		return
	}

	for _, o := range outs {
		out := o.rw.Rewrite(pkt)
		if out == nil {
			continue // inside the drop-until-keyframe window after a switch
		}
		// The track rewrites SSRC and payload type to this child's negotiated
		// values; sequence and timestamp are ours to own, and the rewriter already
		// set them.
		if err := o.track.WriteRTP(out); err != nil {
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
		f.meter.addBytes(out.MarshalSize())
	}
}

// drainRTCP reads RTCP coming back from one downstream child on a forwarded track.
// It MUST run for the whole life of the sender: it is what pumps the per-sender
// interceptor chain (NACK responder, reports) and what catches the child's keyframe
// requests. On PLI/FIR it asks the source for a keyframe. It returns when the child
// leaves (its sender closes), which is the signal to drop this leg's meter count.
func (f *forwarder) drainRTCP(src, dst string, sender rtcpReader) {
	// The child's last reported loss must not outlive the child: a departed leg's
	// final bad report would otherwise derate this relay permanently.
	defer f.loss.forget(dst)
	for {
		pkts, _, err := sender.ReadRTCP()
		if err != nil {
			return // io.ErrClosedPipe when the child's sender stops → child gone
		}
		f.handleRTCP(src, dst, pkts)
	}
}

// handleRTCP dispatches one batch of RTCP arriving from downstream child dst on
// source src's forwarded track. It is split out of drainRTCP's read loop so the
// dispatch is reachable from a test: drainRTCP owns a *webrtc.RTPSender, which cannot
// be faked, and "the sensor is correct but nothing ever calls it" is precisely the
// failure a loss reading of 0 is indistinguishable from.
func (f *forwarder) handleRTCP(src, dst string, pkts []rtcp.Packet) {
	for _, p := range pkts {
		switch pkt := p.(type) {
		case *rtcp.PictureLossIndication, *rtcp.FullIntraRequest:
			// Normalise PLI and FIR to one upstream PLI — we don't need FIR's
			// per-request sequence bookkeeping, just "send a keyframe".
			f.requestUpstreamKeyframe(src)
		case *rtcp.ReceiverReport:
			// The child telling us what IT lost of what WE sent. This loop is the
			// only place in the process that already sees it, which is why the
			// uplink-loss sensor lives here rather than in a second RTCP reader:
			// pion v4 collects no RTPSender stats at all, so GetStats cannot answer
			// this (see TestPionPopulatesSelectedPairRTT).
			//
			// An EMPTY report records nothing. A peer that has received no media yet
			// sends a receiver report with no reception blocks, and treating that as
			// "measured, zero loss" would let a silent leg mask a genuinely lossy
			// sibling in worstPct.
			for _, rr := range pkt.Reports {
				f.loss.observe(dst, fractionLost(rr))
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
	// Capture the mutable upstream field UNDER the lock: setUpstream and
	// rebindUpstream rewrite it on a same-name rejoin and on a re-parent, so reading
	// it after unlocking would be a data race. (ssrc / lastPLI are atomics and safe
	// to touch outside the lock.)
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
	// joined. The clock is INJECTED (never time.Now) so this window is drivable in a
	// test, and it is advanced optimistically (before the best-effort WriteRTCP
	// below, which cannot confirm delivery anyway): a rarely-dropped request just
	// waits one window for the decoder's next PLI, which is the right trade to never
	// storm the source.
	now := f.clk.Now().UnixNano()
	last := s.lastPLI.Load()
	if last != 0 && now-last < int64(pliThrottle) {
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
// closes with the child's session), which is what drops the meter gauge — this only
// trims the slices. A fresh slice is allocated rather than mutated in place so a
// concurrent fanout holding an older snapshot is unaffected.
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

// removeSource drops the forwardSource for a departed SOURCE peer — the peer whose
// media the relay was fanning out — and RETURNS every downstream sender that was
// carrying its media, so the Router can RemoveTrack each one and let the serializer
// fold the removals into one renegotiation per child.
//
// removeChild only handles the departed peer in its CHILD role (legs INTO it, which
// die when its own session closes); a peer that was a source leaves behind an
// f.sources entry that nothing else ever deletes, so without this the map grows
// without bound across sender churn. The source's own read loops have already
// returned (their TrackRemotes closed), so deleting the entry is safe.
func (f *forwarder) removeSource(src string) []*webrtc.RTPSender {
	f.mu.Lock()
	s := f.sources[src]
	delete(f.sources, src)
	f.mu.Unlock()
	if s == nil {
		return nil
	}
	var senders []*webrtc.RTPSender
	for _, o := range s.outs {
		if o.sender != nil {
			senders = append(senders, o.sender)
		}
	}
	return senders
}

// PLIForwarded reports how many keyframe requests the relay has sent upstream.
// Used by the relay test to prove the PLI path fired.
func (f *forwarder) PLIForwarded() int { return int(f.pliForwarded.Load()) }
