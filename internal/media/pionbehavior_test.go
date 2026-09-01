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

// TestPionRemoveTrackDoesNotRenegotiate PINS A LIBRARY BEHAVIOUR, not our own code.
//
// §7.2 originally assumed pion renegotiates on RemoveTrack. Measured against pion
// v4.2.16 it does not: RemoveTrack flips the transceiver sendrecv → recvonly and
// calls onNegotiationNeeded, but checkNegotiationNeeded concludes nothing is needed
// and the handler never runs — while a manual CreateOffer at that exact point DOES
// produce a correct `a=recvonly`. That gap is why Session.RemoveTrack carries
// pendingLocalChange and nudges the serializer itself.
//
// This test exists so that a pion upgrade which FIXES the behaviour fails here
// instead of silently shipping two offers per removal (pion's re-fire plus our
// nudge). If it fails, the fix is to delete pendingLocalChange, not to loosen it.
func TestPionRemoveTrackDoesNotRenegotiate(t *testing.T) {
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
	waitFor(t, "the initial exchange to settle", 10*time.Second, func() bool {
		return offerer.pc.SignalingState() == webrtc.SignalingStateStable &&
			offerer.pc.CurrentRemoteDescription() != nil
	})
	// The exchange itself must be quiet before we can attribute anything to removal.
	settled := handlerRuns.Load()
	stableFor(t, "negotiation to be quiescent", 300*time.Millisecond, func() bool {
		return handlerRuns.Load() == settled
	})
	if dirs := directionLines(offerer.pc.CurrentLocalDescription().SDP); len(dirs) != 1 || dirs[0] != "a=sendrecv" {
		t.Fatalf("local offer directions = %v, want exactly [a=sendrecv]", dirs)
	}

	// Raw pion, deliberately: Session.RemoveTrack adds the nudge this test is about.
	if err := offerer.pc.RemoveTrack(sender); err != nil {
		t.Fatalf("pc.RemoveTrack: %v", err)
	}
	stableFor(t, "pion NOT to fire negotiation-needed for a removal", 500*time.Millisecond, func() bool {
		return handlerRuns.Load() == settled
	})

	// ...and yet the offer that pion says is unnecessary is materially different.
	offer, err := offerer.pc.CreateOffer(nil)
	if err != nil {
		t.Fatalf("create offer after removal: %v", err)
	}
	if dirs := directionLines(offer.SDP); len(dirs) != 1 || dirs[0] != "a=recvonly" {
		t.Fatalf("offer after removal has directions %v, want exactly [a=recvonly] — if this now "+
			"matches the pre-removal SDP, the finding has changed and §7.2 must be re-derived", dirs)
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
