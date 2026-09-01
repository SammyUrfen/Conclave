package media

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
)

// TestPionRenegotiatesARemovalOnceConnected PINS A LIBRARY BEHAVIOUR, not our own
// code — and it REPLACES an earlier pin that asserted the opposite.
//
// The earlier version (TestPionRemoveTrackDoesNotRenegotiate) claimed pion never
// fires OnNegotiationNeeded for a RemoveTrack. Measured properly, that is wrong, and
// wrong in the direction that matters: on a CONNECTED PeerConnection pion fires
// immediately and reliably. The original measurement was taken on a pc that had
// exchanged SDP but was still establishing — which is all `-race` ever produced,
// because DTLS there takes seconds and every quiescence window expired first. Six
// runs varying only the delay before the removal put the correlation beyond doubt:
// removed while `connected` it fires within the same millisecond; removed while
// `connecting` it fires the instant the pc connects, or not at all if it never does.
//
// That is the opposite of the production regime. A relay removes a departed source's
// track from sessions that have been carrying media for minutes.
//
// So this test waits for `connected` FIRST — which is what makes the behaviour
// deterministic under both `go test` and `go test -race` — and then pins what is
// actually true. If a future pion stops firing here, that is a real change and the
// nudge in Session.RemoveTrack becomes the only mechanism, so it must fail loudly
// rather than pass by accident.
//
// The complementary half — that pion does NOT fire when the removal lands while the
// pc is not stable, which is why pendingLocalChange exists at all — is pinned by
// TestRemovalNudgeSurvivesTheAnswer, where deleting the nudge fails deterministically.
func TestPionRenegotiatesARemovalOnceConnected(t *testing.T) {
	bTr, aTr := newGatedPair("b", "a")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	answerer, err := NewSession(SessionConfig{Log: discardLog(), SelfID: "a", PeerID: "b", Transport: aTr})
	if err != nil {
		t.Fatalf("new answerer: %v", err)
	}
	defer answerer.Close()
	offerer, err := NewSession(SessionConfig{Log: discardLog(), SelfID: "b", PeerID: "a", Transport: bTr})
	if err != nil {
		t.Fatalf("new offerer: %v", err)
	}
	defer offerer.Close()

	var handlerRuns atomic.Int64
	offerer.negotiationProbe = func(bool) { handlerRuns.Add(1) }
	answerer.Start(ctx)
	offerer.Start(ctx)

	_, sender, err := offerer.AddForwardTrack("fwd-1", "conclave")
	if err != nil {
		t.Fatalf("add forward track: %v", err)
	}
	// CONNECTED, not merely negotiated. This is the whole difference between the two
	// regimes, and waiting for it is what makes the test invocation-independent:
	// under -race the handshake takes seconds, under plain `go test` a few hundred
	// milliseconds, and the assertion below holds identically once it has completed.
	waitFor(t, "the PeerConnection to connect", 60*time.Second, func() bool {
		return offerer.pc.ConnectionState() == webrtc.PeerConnectionStateConnected
	})
	if dirs := directionLines(offerer.pc.CurrentLocalDescription().SDP); len(dirs) != 1 || dirs[0] != "a=sendrecv" {
		t.Fatalf("local offer directions = %v, want exactly [a=sendrecv]", dirs)
	}

	// Quiet has to be WAITED for, never asserted: pion may legitimately fire one more
	// round on the return to stable, and a hard check here blames that on the removal.
	deadline := time.Now().Add(30 * time.Second)
	var settled int64
	for {
		settled = handlerRuns.Load()
		time.Sleep(200 * time.Millisecond)
		if handlerRuns.Load() == settled {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("negotiation never went quiet, so nothing can be attributed to the removal")
		}
	}

	// Raw pion, deliberately: Session.RemoveTrack adds a nudge of its own, and this
	// test is about what the library does unaided.
	if err := offerer.pc.RemoveTrack(sender); err != nil {
		t.Fatalf("pc.RemoveTrack: %v", err)
	}
	waitFor(t, "pion to fire negotiation-needed for the removal", 10*time.Second, func() bool {
		return handlerRuns.Load() > settled
	})

	// And the offer it asks for really does carry the removal.
	offer, err := offerer.pc.CreateOffer(nil)
	if err != nil {
		t.Fatalf("create offer after removal: %v", err)
	}
	if dirs := directionLines(offer.SDP); len(dirs) != 1 || dirs[0] != "a=recvonly" {
		t.Fatalf("offer after removal has directions %v, want exactly [a=recvonly]", dirs)
	}
}

// TestSessionRemoveTrackOffersExactlyOnceWhenConnected is the production invariant
// the finding above puts at risk: on a connected session a removal is renegotiated
// by pion AND nudged by us, so two independent sources want an offer for one change.
// Exactly one may go out — two offers for one removal is precisely the double-offer
// the serializer exists to prevent, reintroduced through its own fix.
//
// It is asserted on a CONNECTED session on purpose. The unconnected regime, where
// pion stays silent and the nudge is the only trigger, is covered separately by
// TestSessionRemoveTrack.
func TestSessionRemoveTrackOffersExactlyOnceWhenConnected(t *testing.T) {
	bTr, aTr := newGatedPair("b", "a")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	answerer, err := NewSession(SessionConfig{Log: discardLog(), SelfID: "a", PeerID: "b", Transport: aTr})
	if err != nil {
		t.Fatalf("new answerer: %v", err)
	}
	defer answerer.Close()
	offerer, err := NewSession(SessionConfig{Log: discardLog(), SelfID: "b", PeerID: "a", Transport: bTr})
	if err != nil {
		t.Fatalf("new offerer: %v", err)
	}
	defer offerer.Close()
	answerer.Start(ctx)
	offerer.Start(ctx)

	_, sender, err := offerer.AddForwardTrack("fwd-1", "conclave")
	if err != nil {
		t.Fatalf("add forward track: %v", err)
	}
	waitFor(t, "the PeerConnection to connect", 60*time.Second, func() bool {
		return offerer.pc.ConnectionState() == webrtc.PeerConnectionStateConnected
	})

	deadline := time.Now().Add(30 * time.Second)
	var before int
	for {
		before, _, _ = bTr.counts()
		time.Sleep(300 * time.Millisecond)
		if o, _, _ := bTr.counts(); o == before {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("negotiation never went quiet")
		}
	}

	if err := offerer.RemoveTrack(sender); err != nil {
		t.Fatalf("RemoveTrack: %v", err)
	}
	waitFor(t, "the renegotiating offer", 10*time.Second, func() bool {
		o, _, _ := bTr.counts()
		return o == before+1
	})
	// The window has to outlast a full offer/answer round trip, because a redundant
	// second offer would be emitted when the FIRST one's answer lands, not straight
	// away.
	stableFor(t, "exactly one offer for one removal", 2*time.Second, func() bool {
		o, _, _ := bTr.counts()
		return o == before+1
	})
	if dirs := directionLines(offerer.pc.LocalDescription().SDP); len(dirs) != 1 || dirs[0] != "a=recvonly" {
		t.Errorf("the single offer does not carry the removal: directions = %v", dirs)
	}
}

// directionLines extracts the media-direction attributes from an SDP, which is the
// only part of it these tests reason about.
func directionLines(sdp string) []string {
	var out []string
	for _, line := range strings.Split(sdp, "\n") {
		switch line = strings.TrimSpace(line); line {
		case "a=sendrecv", "a=recvonly", "a=sendonly", "a=inactive":
			out = append(out, line)
		}
	}
	return out
}

// TestNegotiationRetryResendsTheCommittedOffer pins §15.13 item 3: pion has no
// have-local-offer → SetLocal(offer) transition, so once SetLocalDescription has
// succeeded the only legal recovery is to re-send the description already applied.
// It is also the semantically right thing — the offer exists and is committed; the
// far end simply never saw it.
//
// Discrimination: every retry's SDP is compared byte-for-byte against the first. A
// retry that re-entered CreateOffer would either error out (leaving fewer attempts
// than the bound) or produce a different session-version line.
func TestNegotiationRetryResendsTheCommittedOffer(t *testing.T) {
	bTr, _ := newGatedPair("b", "a")
	bTr.sendErr = errors.New("transport down")
	clk := newFakeClock()

	offerer, err := NewSession(SessionConfig{
		Log: discardLog(), SelfID: "b", PeerID: "a", Transport: bTr, Clock: clk,
	})
	if err != nil {
		t.Fatalf("new offerer: %v", err)
	}
	defer offerer.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	offerer.Start(ctx)

	if _, _, err := offerer.AddForwardTrack("fwd-1", "conclave"); err != nil {
		t.Fatalf("add forward track: %v", err)
	}
	for i := 1; i <= NegotiationRetries; i++ {
		clk.waitCreated(t, i)
		clk.advance(NegotiationRetryDelay)
	}
	waitFor(t, "every attempt to be spent", 5*time.Second, func() bool {
		return len(bTr.offerSDPs()) == 1+NegotiationRetries
	})

	assertOneCommittedOffer(t, bTr.offerSDPs())
	if st := offerer.pc.SignalingState(); st != webrtc.SignalingStateHaveLocalOffer {
		t.Errorf("signaling state = %s after the retry ladder, want have-local-offer", st)
	}
}

// assertOneCommittedOffer checks that every SDP in the slice is the SAME offer.
//
// The comparison is on the `o=` origin line, not the whole body, and that is not a
// weakening: SDP origin carries the session id AND the session version, which
// CreateOffer increments on every call — so a retry that minted a fresh offer changes
// it, and a retry that re-sent the committed one cannot. The bodies themselves
// legitimately differ, because LocalDescription() appends the ICE candidates gathered
// since the last call, which is exactly the behaviour we want on a re-send.
func assertOneCommittedOffer(t *testing.T, sdps []string) {
	t.Helper()
	if len(sdps) < 2 {
		t.Fatalf("only %d offers to compare", len(sdps))
	}
	want := originLine(sdps[0])
	if want == "" {
		t.Fatalf("no o= line in the first offer:\n%s", sdps[0])
	}
	for i, sdp := range sdps[1:] {
		if got := originLine(sdp); got != want {
			t.Errorf("re-send %d has origin %q, want %q — a changed session version means "+
				"CreateOffer ran again, which pion forbids from have-local-offer", i+1, got, want)
		}
	}
}

// originLine returns the `o=` line of a WIRE description — the JSON-encoded
// webrtc.SessionDescription a Session actually sends — which carries the session id
// and the session version.
func originLine(wire string) string {
	var desc webrtc.SessionDescription
	if err := json.Unmarshal([]byte(wire), &desc); err != nil {
		return ""
	}
	for _, line := range strings.Split(desc.SDP, "\n") {
		if line = strings.TrimSpace(line); strings.HasPrefix(line, "o=") {
			return line
		}
	}
	return ""
}

// TestNegotiationAnswerTimeoutResends pins §15.13 item 5. The offer LANDS on the
// transport here — nothing fails — but no answer ever comes, which is exactly what a
// role inversion does to a correct far end for one window. Without the re-send the
// edge deadlocks forever; with it, the same committed offer goes again, bounded by
// the same retry ladder so the total stays finite.
func TestNegotiationAnswerTimeoutResends(t *testing.T) {
	// No answerer session exists at the other end of this pair, so the offer is
	// delivered and simply never answered.
	bTr, _ := newGatedPair("b", "a")
	clk := newFakeClock()

	offerer, err := NewSession(SessionConfig{
		Log: discardLog(), SelfID: "b", PeerID: "a", Transport: bTr, Clock: clk,
	})
	if err != nil {
		t.Fatalf("new offerer: %v", err)
	}
	defer offerer.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	offerer.Start(ctx)

	if _, _, err := offerer.AddForwardTrack("fwd-1", "conclave"); err != nil {
		t.Fatalf("add forward track: %v", err)
	}
	waitFor(t, "the first offer to be sent", 5*time.Second, func() bool {
		return len(bTr.offerSDPs()) == 1
	})

	for i := 1; i <= NegotiationRetries; i++ {
		clk.waitCreated(t, i)
		clk.advance(NegotiationAnswerTimeout)
	}
	waitFor(t, "the bounded re-sends", 5*time.Second, func() bool {
		return len(bTr.offerSDPs()) == 1+NegotiationRetries
	})
	stableFor(t, "the ladder to stop at NegotiationRetries", 300*time.Millisecond, func() bool {
		return len(bTr.offerSDPs()) == 1+NegotiationRetries && clk.createdCount() == NegotiationRetries
	})

	assertOneCommittedOffer(t, bTr.offerSDPs())
}
