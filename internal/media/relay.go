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
// (source, LAYER). Several children converging on a fresh stream (or a chatty
// decoder) would otherwise storm the sender with redundant PLIs; one every ~300 ms
// is plenty to recover a keyframe without wasting the sender's bitrate on extra
// I-frames.
//
// The key is per-LAYER, not per-source, and the difference is the whole point of a
// layered relay: a child that has just been switched down to `q` needs q's next
// keyframe NOW, and a per-source window would swallow that request behind one
// another child just spent on `f`. Nothing retries it, so the switched child would
// stare at a dropped stream for up to a window — and the request that DID go out
// looks perfectly correct, which is what makes the failure hard to see.
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

	// rtt answers "what is the measured round-trip to this child, if any". It is the
	// second input to selectLayer, and it is a CALLBACK rather than a field because
	// the measurement lives on the Router (rttStore, fed from the nominated ICE pair)
	// and the forwarder must not reach up into its owner. Nil ⇒ no RTT is available,
	// which selectLayer reads as "no opinion" — the correct answer for a static-tree
	// relay, which never samples it.
	rtt func(child string) (float64, bool)

	mu      sync.RWMutex
	sources map[string]*forwardSource // keyed by SOURCE peer name
}

// forwardSource is everything about one media source S the relay receives: the
// session S's media arrives on (where an upstream PLI is written), the per-LAYER
// upstream state, and the per-child outbound legs carrying S's media downstream.
//
// A source may publish SEVERAL LAYERS — several ordinary tracks on several m-lines,
// `video.q` / `video.h` / `video.f` — and the relay picks one PER CHILD. That is
// the second axis: the first, activeGen, answers "which upstream reader may write",
// and it now lives per layer because each layer is its own TrackRemote and its own
// reader loop. See docs/DESIGN.md §5.14.
type forwardSource struct {
	name     string
	upstream rtcpWriter               // session toward S; target of upstream keyframe requests
	outs     []*forwardOut            // downstream legs (guarded by forwarder.mu)
	layers   map[string]*forwardLayer // layer id → upstream state (guarded by forwarder.mu)
	// top is the highest-ranked layer this source is currently known to publish. A
	// leg with no explicit choice follows it, so a fresh leg is never worse off than
	// it would have been before layers existed. "" for a single-layer source, which
	// is exactly what its one unnamed layer is called. Guarded by forwarder.mu.
	top string
}

// forwardLayer is one encoding of one source: its upstream SSRC (the MediaSSRC an
// upstream PLI for THIS layer must carry), its own throttle clock, and its own
// reader generations.
//
// The SSRC is per layer because there is no such thing as "the source's SSRC" once
// a source publishes several tracks — a PLI naming the wrong one is discarded by
// the sender with no error at all, which is the silent black-video failure
// requestUpstreamKeyframe exists to prevent.
type forwardLayer struct {
	ssrc    atomic.Uint32 // this layer's ACTIVE upstream SSRC; 0 until its track arrives
	lastPLI atomic.Int64  // unixnano of the last upstream PLI for this layer

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
//
// ONE track per leg, whatever the source publishes. The child is never told which
// layer it is getting; the rewriter makes a layer change look like the same
// continuous stream, exactly as it already does for a re-parent. That is what keeps
// the m-line count, the topology diff and overlay.Validate untouched by this work.
//
// layer, pinned and sel are all guarded by forwarder.mu — plain fields rather than
// atomics because fanout now holds the read lock across its whole per-leg decision,
// which is what makes "exactly one (generation, layer) per leg may write" an
// invariant rather than a hope.
type forwardOut struct {
	child  string
	track  rtpTrack
	sender *webrtc.RTPSender
	rw     *rtpRewriter

	// layer is the encoding this leg carries. EMPTY means AUTO: follow the source's
	// top rung. Auto is the state a leg is born in and the state a single-layer leg
	// never leaves, which is why "" is both "no choice made" and "the single-layer
	// source's only layer" — for that source they are the same answer.
	layer string
	// pinned legs are never moved by the selection policy. A child RELAY is pinned
	// to the top rung, because it can only forward a layer it RECEIVES: walking it
	// down would cap its whole subtree at that rung, including peers on perfect
	// links. Per-child selection is a LEAF decision here; see docs/DESIGN.md §8.5.
	pinned bool
	// sel is the hysteresis state selectLayer carries between rounds.
	sel layerChoice
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
		s = &forwardSource{name: src, layers: make(map[string]*forwardLayer)}
		f.sources[src] = s
	}
	return s
}

// ensureLayer returns the per-layer state for id, creating it if this is the first
// time the relay has seen that encoding of this source, and re-resolving the
// source's top rung. It reports whether the top CHANGED, which is the signal that
// every auto leg's effective layer just moved and so must be spliced. Caller holds
// f.mu.
func (s *forwardSource) ensureLayer(id string) (l *forwardLayer, topChanged bool) {
	l = s.layers[id]
	if l == nil {
		s.dropOtherShape(id)
		l = &forwardLayer{genSSRC: make(map[uint64]uint32)}
		s.layers[id] = l
		topChanged = s.retop()
	}
	return l, topChanged
}

// dropOtherShape enforces layers.go's wire contract — a source publishes EITHER one
// unnamed layer OR several named rungs, never a mixture — at the one place the
// mixture can appear.
//
// It appears on a RE-PARENT that changes the ladder's SHAPE. A source is keyed by its
// ORIGIN, so the same origin arrives as `video.q/.h/.f` while it is our own
// neighbour and as ONE `fwd-<origin>` once our new parent is a relay forwarding what
// IT selected. Without this, commitSwitch leaves the three named rungs ARMED for a
// reader that will never appear, retop still ranks `f` above the unnamed layer, and
// every leg goes on resolving to a rung nothing sends — a permanently black subtree
// with no error on any path, because the source, the upstream and the reader loop
// are all present and correct.
//
// Legs are returned to AUTO rather than repointed: every rung any of them could have
// chosen has just gone, and the hysteresis state was evidence about a ladder that no
// longer exists. The SPLICE is the caller's — retop necessarily reports a move here
// (the two shapes' ranks are disjoint), so newUpstreamGen charges each leg exactly
// one discontinuity. Caller holds f.mu.
func (s *forwardSource) dropOtherShape(id string) {
	named := layerRank(id) >= 0
	dropped := false
	for cur := range s.layers {
		if (layerRank(cur) >= 0) != named {
			delete(s.layers, cur)
			dropped = true
		}
	}
	if !dropped {
		return
	}
	for _, o := range s.outs {
		o.layer, o.sel = "", layerChoice{}
	}
}

// retop recomputes the highest-ranked layer this source publishes and reports
// whether it moved. An id not on layerLadder ranks below every real rung, so a
// single-layer source's one unnamed layer stays top and nothing is ever spliced for
// it — which is what makes the single-layer path byte-identical to the shipped one.
// Caller holds f.mu.
func (s *forwardSource) retop() bool {
	best, bestRank := "", -2
	for id := range s.layers {
		if r := layerRank(id); r > bestRank {
			best, bestRank = id, r
		}
	}
	if best == s.top {
		return false
	}
	s.top = best
	return true
}

// ladder returns this source's available rungs, lowest first — the input
// selectLayer chooses from. Caller holds f.mu.
func (s *forwardSource) ladder() []string {
	out := make([]string, 0, len(s.layers))
	for _, id := range layerLadder {
		if _, ok := s.layers[id]; ok {
			out = append(out, id)
		}
	}
	if len(out) == 0 && len(s.layers) > 0 {
		// A source publishing only off-ladder ids (in practice: the single unnamed
		// layer). One rung, nothing to choose.
		for id := range s.layers {
			out = append(out, id)
		}
	}
	return out
}

// resolve is the layer a leg actually carries: its explicit choice, or the source's
// top rung when it has not made one. Caller holds f.mu.
func (s *forwardSource) resolve(o *forwardOut) string {
	if o.layer != "" {
		return o.layer
	}
	return s.top
}

// outFor returns the leg toward child, or nil. Caller holds f.mu.
func (s *forwardSource) outFor(child string) *forwardOut {
	for _, o := range s.outs {
		if o.child == child {
			return o
		}
	}
	return nil
}

// spliceAll tells every downstream leg that its upstream changed. Exactly once per
// leg — a commit that promotes several layers must not consume several sequence
// slots on a leg that only moved once. Caller holds f.mu.
func (s *forwardSource) spliceAll() {
	for _, o := range s.outs {
		o.rw.Switch()
	}
}

// spliceLayer splices only the legs currently carrying layer id. Used when ONE
// layer's authoritative generation changes: a leg watching a different rung saw no
// discontinuity and must not be charged for one. Caller holds f.mu.
func (s *forwardSource) spliceLayer(id string) {
	for _, o := range s.outs {
		if s.resolve(o) == id {
			o.rw.Switch()
		}
	}
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

// commitSwitch promotes the newest reader generation OF EVERY LAYER — or arms the
// layers that have no newer one, so the next reader to appear on them wins — and
// splices every downstream leg ONCE.
//
// Every layer, because a re-parent replaces the whole upstream edge and therefore
// every encoding arriving over it; once per leg, because a leg's stream is spliced
// when its upstream changed, not once for each layer the commit happened to touch.
// Splicing twice would consume a second sequence slot and read to the child as a
// one-packet gap. Caller holds f.mu.
func (s *forwardSource) commitSwitch() {
	for _, l := range s.layers {
		l.promoteOrArm()
	}
	s.spliceAll()
}

// promoteOrArm makes this layer's newest reader authoritative, or — if none newer
// exists yet — arms the layer so the next one to appear takes over on arrival.
//
// The SSRC is reset when arming because an upstream PLI carrying the OLD parent's
// SSRC for this layer would be silently ignored by the new one. Caller holds f.mu.
func (l *forwardLayer) promoteOrArm() {
	if l.nextGen > l.activeGen {
		l.activate(l.nextGen)
		return
	}
	l.awaiting = true
	l.ssrc.Store(0)
}

// activate promotes gen to this layer's authoritative generation. It does NOT
// splice: which legs to splice is the SOURCE's question (a leg on a different rung
// saw nothing happen), and the caller answers it. Caller holds f.mu.
func (l *forwardLayer) activate(gen uint64) {
	l.activeGen = gen
	l.awaiting = false
	l.ssrc.Store(l.genSSRC[gen]) // 0 when that generation's track has not arrived
	for g := range l.genSSRC {
		if g < gen {
			delete(l.genSSRC, g) // retired readers cannot come back
		}
	}
}

// newUpstreamGen registers one reader loop over one TrackRemote for src and returns
// its generation token, which the loop passes to every fanout call. The FIRST
// generation is authoritative immediately (nothing to switch from); a later one is
// silent until the Router commits the re-parent — unless a rebind already committed
// while waiting for it, in which case it takes over on arrival.
func (f *forwarder) newUpstreamGen(src, layer string) uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := f.ensureSource(src)
	l, topChanged := s.ensureLayer(layer)
	l.nextGen++
	gen := l.nextGen
	switch {
	case l.awaiting:
		// A rebind already committed while waiting for this reader to appear.
		l.activate(gen)
		s.spliceLayer(layer)
	case l.activeGen == 0:
		l.activeGen = gen
	}
	// A NEW rung that raises the source's top moves every auto leg onto it, so those
	// legs must splice. Nothing happens for a single-layer source: its one unnamed
	// layer is its top from the moment it registers, so the top never moves and no
	// leg is ever spliced for it.
	if topChanged {
		for _, o := range s.outs {
			if o.layer == "" {
				o.rw.Switch()
			}
		}
	}
	return gen
}

// registerOut is the shared bookkeeping behind addOut, addOutLive and addOutPinned.
//
// A new leg is born AUTO (layer ""), i.e. following the source's top rung, so it is
// never worse off than it would be if layers did not exist. Selection moves it from
// there — or, for a pinned leg, never does.
func (f *forwarder) registerOut(src, dst string, track rtpTrack, sender *webrtc.RTPSender, pinned bool) {
	f.mu.Lock()
	s := f.ensureSource(src)
	s.outs = append(s.outs, &forwardOut{child: dst, track: track, sender: sender, rw: &rtpRewriter{}, pinned: pinned})
	f.mu.Unlock()

	if sender == nil {
		return // nothing to drain (test double); the leg is still metered on write
	}
	// Each downstream leg counts as one outbound stream on the relay's upload meter,
	// so its aggregate rate shows the relay uploading O(children)·(source bitrate) —
	// exactly the cost the elected-SFU concentrates onto the relay so the leaves
	// stay flat. The gauge drops back when the drain exits (child gone / shutdown).
	// The gauge is raised INSIDE the spawned drain, not before it. spawn is the
	// Router's gate and drops the work outright once Run is tearing down, so a +1 out
	// here would have no -1 to pair with and would leave the meter reading legs that
	// never ran.
	f.spawn(func() {
		f.meter.addPeer(1)
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
	f.registerOut(src, dst, track, sender, false)
}

// addOutLive is addOut for a leg created AFTER the session has started — the
// mid-call case, where the track was added by AddForwardTrack on a live pc and the
// serializer folds it into the next offer. Identical bookkeeping; it exists as a
// separate name purely so the "added before Start" precondition on addOut stays a
// true, checkable statement.
func (f *forwarder) addOutLive(src, dst string, track rtpTrack, sender *webrtc.RTPSender) {
	f.registerOut(src, dst, track, sender, false)
}

// addOutPinned is addOut for a leg toward a child RELAY, which is exempt from layer
// selection and always carries the top rung.
//
// The reason is structural, not a policy preference: a relay can only forward a
// layer it RECEIVES. Sending an intermediate relay a lower rung caps its ENTIRE
// SUBTREE at that rung — including peers on perfect links, who have no way to ask
// for better. Selecting per child is therefore a leaf-only decision in this build;
// making it work multi-hop needs either subtree knowledge in the data plane or
// every layer on every relay-to-relay edge, and both are out of scope. See
// docs/DESIGN.md §8.5.
//
// It does not distinguish "before Start" from "mid-call" the way addOut and
// addOutLive do, because the bookkeeping is identical and the precondition those
// two names document is about the SESSION, not about the leg.
func (f *forwarder) addOutPinned(src, dst string, track rtpTrack, sender *webrtc.RTPSender) {
	f.registerOut(src, dst, track, sender, true)
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
func (f *forwarder) forward(src, layer string, remote *webrtc.TrackRemote) {
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

	gen := f.newUpstreamGen(src, layer)
	f.learnSSRC(src, layer, gen, uint32(remote.SSRC()))
	f.log.Info("forwarding source",
		slog.String("source", src), slog.String("layer", layer),
		slog.Any("ssrc", remote.SSRC()), slog.Uint64("gen", gen))
	// The source just started: ask it for a keyframe so any already-connected
	// downstream can begin decoding immediately rather than waiting for the next
	// natural I-frame. (Late joiners are covered by the on-connect request.)
	f.requestUpstreamKeyframe(src, layer)

	for {
		pkt, _, err := remote.ReadRTP()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				f.log.Debug("source read ended", slog.String("source", src), slog.Any("error", err))
			}
			return // upstream gone; its downstream tracks simply stop receiving writes
		}
		f.fanout(src, layer, gen, pkt)
	}
}

// learnSSRC records the SSRC of one upstream generation, promoting it to the
// source's live SSRC only if that generation is the authoritative one. Storing it
// unconditionally would point upstream PLIs at the pending parent's stream while
// the old parent is still the one being watched.
func (f *forwarder) learnSSRC(src, layer string, gen uint64, ssrc uint32) {
	f.mu.Lock()
	s := f.ensureSource(src)
	l, _ := s.ensureLayer(layer)
	l.genSSRC[gen] = ssrc
	if gen == l.activeGen {
		l.ssrc.Store(ssrc)
	}
	f.mu.Unlock()
}

// fanout writes one packet of one LAYER from upstream generation gen to every
// downstream leg of src that is currently carrying that layer, and discards it
// otherwise.
//
// TWO AXES, AND BOTH MUST AGREE. gen answers "is this reader the authoritative one"
// (a re-parent keeps two alive at once); the leg's layer answers "is this the
// encoding this child asked for". A packet that satisfies only one of them must not
// reach the wire: the first would interleave two unrelated upstreams into one leg,
// the second would splice a leg onto a rung it did not choose and then discard
// everything it did.
//
// Each leg gets its OWN rewritten copy: the outgoing sequence and timestamp are
// continuous per leg across a switch (see rtpRewriter — a layer change is the same
// discontinuity as a re-parent, so it needs no new mechanism), and the copy is what
// makes that safe when several legs share one upstream packet.
//
// THE READ LOCK IS HELD ACROSS THE WHOLE LOOP, deliberately. Snapshotting the legs
// and then deciding after unlocking leaves a window where a commit or a selection
// lands between the check and the Rewrite, and the leg rebases onto the stream that
// just lost — permanently, and silently, because a rebased leg looks perfectly
// healthy. Rewrite itself is pure CPU (a mutex and a header copy); the WriteRTP calls
// under the same lock are not — each is an SRTP encrypt plus a socket write — so the
// cost is one packet's fan-out across EVERY leg, and sync.RWMutex queues readers
// behind a waiting writer, so a child slow to drain stalls the other sources' fan-out
// as well as the commit. Judged the cheap side of the trade against a leg silently
// rebasing onto a dead stream. The lock is also why forwardOut.layer can be a plain
// field rather than an atomic.
func (f *forwarder) fanout(src, layer string, gen uint64, pkt *rtp.Packet) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	s := f.sources[src]
	if s == nil {
		return
	}
	l := s.layers[layer]
	if l == nil || gen != l.activeGen {
		return
	}

	for _, o := range s.outs {
		if s.resolve(o) != layer {
			continue // this child is watching a different rung
		}
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
	reviewable := false
	for _, p := range pkts {
		switch pkt := p.(type) {
		case *rtcp.PictureLossIndication, *rtcp.FullIntraRequest:
			// Normalise PLI and FIR to one upstream PLI — we don't need FIR's
			// per-request sequence bookkeeping, just "send a keyframe".
			//
			// For the LAYER THIS CHILD IS ACTUALLY RECEIVING. The child cannot name it
			// — it is never told which rung it is on — so the relay answers from its
			// own leg state. Asking the wrong layer's encoder produces a request the
			// sender discards with no error, and a child that stays black.
			f.requestUpstreamKeyframe(src, f.legLayer(src, dst))
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
			// The same report is the layer policy's clock. Reviewing HERE, on the
			// evidence's own arrival, is what keeps the control loop in the data
			// plane: no ticker, no injected time, and a reaction paced by the child's
			// own ~1 Hz reports rather than by the coordinator's 3 s telemetry cadence
			// and a WebSocket round trip. An EMPTY report carries no evidence and must
			// not be counted as a clean round.
			reviewable = reviewable || len(pkt.Reports) > 0
		}
	}
	if reviewable {
		f.reviewLayer(src, dst)
	}
}

// requestUpstreamKeyframe forwards a keyframe request for ONE LAYER of src to the
// ORIGINAL sender of that media. The one thing that must be right: the PLI's
// MediaSSRC is THAT LAYER's UPSTREAM SSRC, not the downstream SSRC the child's PLI
// carried and not another layer's. A child's PLI names the SSRC the relay assigned
// it; forwarding that verbatim would make the source ignore it (unknown SSRC) →
// black video with no error. With several layers there is no longer any such thing
// as "the source's SSRC", so naming the wrong one is the same failure by a shorter
// route. We build a fresh PLI with that layer's own SSRC.
func (f *forwarder) requestUpstreamKeyframe(src, layer string) {
	// Capture the mutable upstream field and the layer record UNDER the lock:
	// setUpstream and rebindUpstream rewrite the first on a same-name rejoin and on a
	// re-parent, and the layer map grows as tracks arrive, so reading either after
	// unlocking would be a data race. (ssrc / lastPLI are atomics on a record whose
	// pointer never moves, so they are safe to touch outside the lock.)
	f.mu.RLock()
	var upstream rtcpWriter
	var l *forwardLayer
	if s := f.sources[src]; s != nil {
		upstream = s.upstream
		l = s.layers[layer]
	}
	f.mu.RUnlock()
	if upstream == nil || l == nil {
		return
	}
	ssrc := l.ssrc.Load()
	if ssrc == 0 {
		return // this layer isn't flowing yet — nothing to request a keyframe of
	}

	// Throttle per (SOURCE, LAYER), not per child, on purpose: a keyframe is not
	// per-subscriber — when a layer emits one I-frame it is forwarded to EVERY
	// downstream leg watching that layer, so one request per window serves all the
	// children that just joined it. Per layer rather than per source because the
	// layers are independently encoded streams: a request for `q` is not answered by
	// `f`'s next I-frame, and sharing one window between them means a switched child
	// waits out a window for a request that is never retried (see pliThrottle).
	//
	// The clock is INJECTED (never time.Now) so this window is drivable in a test,
	// and it is advanced optimistically (before the best-effort WriteRTCP below,
	// which cannot confirm delivery anyway): a rarely-dropped request just waits one
	// window for the decoder's next PLI, which is the right trade to never storm the
	// source.
	now := f.clk.Now().UnixNano()
	last := l.lastPLI.Load()
	if last != 0 && now-last < int64(pliThrottle) {
		return
	}
	if !l.lastPLI.CompareAndSwap(last, now) {
		return // another goroutine just sent one; don't double up
	}

	if err := upstream.WriteRTCP([]rtcp.Packet{&rtcp.PictureLossIndication{MediaSSRC: ssrc}}); err != nil {
		f.log.Debug("upstream keyframe request failed",
			slog.String("source", src), slog.String("layer", layer), slog.Any("error", err))
		return
	}
	f.pliForwarded.Add(1)
	f.log.Debug("forwarded keyframe request upstream",
		slog.String("source", src), slog.String("layer", layer), slog.Any("ssrc", ssrc))
}

// requestUpstreamKeyframeAll asks for a keyframe on EVERY layer of src the relay
// currently holds. Used on a re-parent, where every layer arriving over the
// replaced edge was spliced at once and each is a separately-encoded stream that
// needs its own I-frame — one layer's keyframe does not decode another's.
func (f *forwarder) requestUpstreamKeyframeAll(src string) {
	f.mu.RLock()
	var layers []string
	if s := f.sources[src]; s != nil {
		for id := range s.layers {
			layers = append(layers, id)
		}
	}
	f.mu.RUnlock()
	sort.Strings(layers) // deterministic order; the requests are independent
	for _, id := range layers {
		f.requestUpstreamKeyframe(src, id)
	}
}

// legLayer reports the layer a leg is actually carrying — its explicit choice, or
// the source's top rung when it has made none. "" for a single-layer source and for
// any leg that does not exist, which is the same answer the shipped build gave when
// there was only ever one stream.
func (f *forwarder) legLayer(src, child string) string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	s := f.sources[src]
	if s == nil {
		return ""
	}
	o := s.outFor(child)
	if o == nil {
		return ""
	}
	return s.resolve(o)
}

// activeGen reports which reader generation may currently write one layer of src.
// Zero when the source or the layer is unknown. Exists for the two-axis tests: the
// invariant they check is about which (generation, layer) is authoritative, and
// that is not otherwise observable from outside the fan-out.
func (f *forwarder) activeGen(src, layer string) uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	s := f.sources[src]
	if s == nil || s.layers[layer] == nil {
		return 0
	}
	return s.layers[layer].activeGen
}

// selectLegLayer moves ONE leg to a different layer and reports whether anything
// changed. It is the mechanism half of layer selection; reviewLayer is the policy
// half that decides when to call it.
//
// Two things happen together and neither is optional. The leg is SPLICED
// (rw.Switch), because a different encoding is exactly the same discontinuity as a
// different upstream — an unrelated sequence and timestamp series that a keyframe
// alone does not repair. And the new layer is asked for a KEYFRAME, because until
// one arrives the leg is dropping everything: without the request the child freezes
// until that encoder's next natural I-frame, which on a looping file can be
// seconds. The request happens after the lock is released, since it takes the read
// lock itself.
//
// Only the ONE leg is spliced. A sibling on another rung saw nothing happen and
// must not be charged a drop-until-keyframe window for a decision about someone else.
func (f *forwarder) selectLegLayer(src, child, want string) bool {
	f.mu.Lock()
	s := f.sources[src]
	if s == nil {
		f.mu.Unlock()
		return false
	}
	o := s.outFor(child)
	if o == nil || s.resolve(o) == want {
		f.mu.Unlock()
		return false
	}
	o.layer = want
	o.rw.Switch()
	f.mu.Unlock()

	f.requestUpstreamKeyframe(src, want)
	return true
}

// reviewLayer re-evaluates one leg's layer against the evidence that just arrived
// from that child, and acts on the answer. Called from handleRTCP on every
// reception report — the sensor's own cadence is the loop's clock, so there is no
// timer here and nothing for the determinism gate to catch.
//
// The two sensor reads happen BEFORE f.mu is taken. Both have their own locks
// (lossTracker's, and the Router's for the RTT store), and taking them under f.mu
// would make the forwarder's lock the outer one in an ordering the Router does not
// know about. Reading first costs a slightly stale sample of a signal that moves on
// a multi-second scale, which is not a cost at all.
func (f *forwarder) reviewLayer(src, child string) {
	lossPct := f.loss.pctFor(child)
	var rttMs float64
	var rttKnown bool
	if f.rtt != nil {
		rttMs, rttKnown = f.rtt(child)
	}

	f.mu.Lock()
	s := f.sources[src]
	if s == nil {
		f.mu.Unlock()
		return
	}
	o := s.outFor(child)
	if o == nil || o.pinned {
		f.mu.Unlock()
		return // a child relay carries the top rung and is not ours to move
	}
	cur := layerChoice{Layer: s.resolve(o), Bad: o.sel.Bad, Good: o.sel.Good}
	next := selectLayer(s.ladder(), cur, layerObs{LossPct: lossPct, RTTMs: rttMs, RTTKnown: rttKnown})
	o.sel = next
	changed := next.Layer != cur.Layer
	if changed {
		o.layer = next.Layer
		o.rw.Switch()
	}
	f.mu.Unlock()

	if changed {
		f.log.Info("layer switch",
			slog.String("source", src), slog.String("child", child),
			slog.String("from", cur.Layer), slog.String("to", next.Layer),
			slog.Float64("loss_pct", lossPct))
		f.requestUpstreamKeyframe(src, next.Layer)
	}
}

// keyframeForChild proactively asks upstream for a keyframe for every source that
// child receives — called when the child's connection comes up, so a fresh joiner
// gets an I-frame immediately instead of waiting for the next natural one (or for
// the decoder to ask). No-ops for sources whose media isn't flowing yet.
func (f *forwarder) keyframeForChild(child string) {
	type want struct{ src, layer string }
	f.mu.RLock()
	var wants []want
	for _, s := range f.sources {
		if o := s.outFor(child); o != nil {
			// The layer THIS child is on, not every layer the source publishes: the
			// others are being watched by someone else, or by nobody, and a keyframe
			// they did not need costs the sender real bitrate.
			wants = append(wants, want{src: s.name, layer: s.resolve(o)})
		}
	}
	f.mu.RUnlock()
	for _, w := range wants {
		f.requestUpstreamKeyframe(w.src, w.layer)
	}
}

// removeChild prunes every forwarding leg toward a departed child, so the forward
// loop stops iterating dead legs and outs cannot grow without bound across
// join/leave churn. The child's RTCP-drain goroutine exits on its own (its sender
// closes with the child's session), which is what drops the meter gauge — this only
// trims the slices. A fresh slice is allocated rather than mutated in place so a
// concurrent fanout that captured the slice header before this call is unaffected —
// belt and braces, since fanout now holds the read lock for its whole loop.
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
