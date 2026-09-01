package media

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"

	"github.com/SammyUrfen/conclave/internal/signaling"
)

// gatedTransport is a memTransport that counts what it was asked to send and can
// HOLD outbound frames. Holding is what makes the serializer testable at all: the
// defect only exists while an offer is outstanding, so a test needs to keep one
// outstanding while it mutates the PeerConnection.
type gatedTransport struct {
	selfID string
	out    chan<- signaling.Message
	in     <-chan signaling.Message

	mu       sync.Mutex
	gated    bool
	held     []signaling.Message
	offers   int
	answers  int
	sendErr  error // when non-nil, every Send fails with it (drives the retry path)
	attempts int
	offerSDP []string // the SDP body of every offer, in order
}

func (t *gatedTransport) Send(msg signaling.Message) error {
	msg.From = t.selfID // the server stamps identity; emulate it here
	t.mu.Lock()
	t.attempts++
	switch msg.Type {
	case signaling.TypeOffer:
		t.offers++
		t.offerSDP = append(t.offerSDP, string(msg.SDP))
	case signaling.TypeAnswer:
		t.answers++
	}
	if t.sendErr != nil {
		err := t.sendErr
		t.mu.Unlock()
		return err
	}
	if t.gated {
		t.held = append(t.held, msg)
		t.mu.Unlock()
		return nil
	}
	t.mu.Unlock()
	t.out <- msg
	return nil
}

func (t *gatedTransport) Incoming() <-chan signaling.Message { return t.in }

// release stops holding and flushes everything held so far, in order.
func (t *gatedTransport) release() {
	t.mu.Lock()
	t.gated = false
	held := t.held
	t.held = nil
	t.mu.Unlock()
	for _, msg := range held {
		t.out <- msg
	}
}

// offerSDPs returns every offer body sent so far, so a test can prove a retry
// re-sent the SAME description rather than minting a new one.
func (t *gatedTransport) offerSDPs() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]string(nil), t.offerSDP...)
}

func (t *gatedTransport) counts() (offers, answers, attempts int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.offers, t.answers, t.attempts
}

func newGatedPair(aID, bID string) (a, b *gatedTransport) {
	aIn := make(chan signaling.Message, 64)
	bIn := make(chan signaling.Message, 64)
	a = &gatedTransport{selfID: aID, out: bIn, in: aIn}
	b = &gatedTransport{selfID: bID, out: aIn, in: bIn}
	return a, b
}

// waitFor polls cond until it holds or the deadline passes. Media negotiation runs
// on pion's own goroutines, so a test can only observe it by polling.
func waitFor(t *testing.T, what string, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// stableFor asserts cond keeps holding for d — used to prove that NO further offer
// appears, which a one-shot check cannot show.
func stableFor(t *testing.T, what string, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if !cond() {
			t.Fatalf("%s stopped holding", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func discardLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// TestNegotiationSerializer is the §7.1 contract, and its first subtest is the
// IMPLEMENTER OBLIGATION the contract names: it must be run before trusting the
// claim that pion v4 re-fires OnNegotiationNeeded on the return to `stable`. If it
// did not, a manual re-run flag would be required after all.
func TestNegotiationSerializer(t *testing.T) {
	t.Run("exactly one follow-up offer for tracks added mid-flight", func(t *testing.T) {
		// "b" > "a", so the default rule makes b the offerer.
		bTr, aTr := newGatedPair("b", "a")
		bTr.gated = true // hold b's first offer so it stays outstanding

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		answerer, err := NewSession(SessionConfig{
			Log: discardLog(), SelfID: "a", PeerID: "b", Transport: aTr,
		})
		if err != nil {
			t.Fatalf("new answerer: %v", err)
		}
		defer answerer.Close()

		offerer, err := NewSession(SessionConfig{
			Log: discardLog(), SelfID: "b", PeerID: "a", Transport: bTr,
		})
		if err != nil {
			t.Fatalf("new offerer: %v", err)
		}
		defer offerer.Close()

		answerer.Start(ctx)
		offerer.Start(ctx)

		if _, _, err := offerer.AddForwardTrack("fwd-1", "conclave"); err != nil {
			t.Fatalf("add first track: %v", err)
		}
		waitFor(t, "the first offer", 5*time.Second, func() bool {
			o, _, _ := bTr.counts()
			return o >= 1
		})

		// The offer is outstanding (held). Two more tracks now: with no serializer
		// the temptation is to offer per track; with pion's own guard plus ours the
		// answer is "not yet".
		if _, _, err := offerer.AddForwardTrack("fwd-2", "conclave"); err != nil {
			t.Fatalf("add second track: %v", err)
		}
		if _, _, err := offerer.AddForwardTrack("fwd-3", "conclave"); err != nil {
			t.Fatalf("add third track: %v", err)
		}
		stableFor(t, "exactly one offer while one is outstanding", 300*time.Millisecond, func() bool {
			o, _, _ := bTr.counts()
			return o == 1
		})

		// Let the answer through. pion clears its negotiation-needed flag on the
		// return to stable and re-fires — so the two extra tracks must produce
		// EXACTLY ONE more offer, not two and not zero.
		bTr.release()
		waitFor(t, "the follow-up offer after the answer", 5*time.Second, func() bool {
			o, _, _ := bTr.counts()
			return o >= 2
		})
		stableFor(t, "exactly two offers in total", 500*time.Millisecond, func() bool {
			o, _, _ := bTr.counts()
			return o == 2
		})
	})

	t.Run("the answerer never initiates", func(t *testing.T) {
		bTr, aTr := newGatedPair("b", "a")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		answerer, err := NewSession(SessionConfig{
			Log: discardLog(), SelfID: "a", PeerID: "b", Transport: aTr,
		})
		if err != nil {
			t.Fatalf("new answerer: %v", err)
		}
		defer answerer.Close()
		answerer.Start(ctx)
		_ = bTr

		if _, _, err := answerer.AddForwardTrack("fwd-1", "conclave"); err != nil {
			t.Fatalf("add track: %v", err)
		}
		stableFor(t, "no offer from the answerer", 300*time.Millisecond, func() bool {
			o, _, _ := aTr.counts()
			return o == 0
		})
	})

	t.Run("the in-flight flag is clear before pion re-fires", func(t *testing.T) {
		// The ORDER matters and is invisible from outside: pion sets its own
		// negotiation-needed flag and THEN invokes our handler, so a handler that
		// returns early because our flag is still set loses that notification
		// permanently — pion will not fire again until the next return to stable,
		// which never comes because we never offered. The probe records what the
		// handler actually saw.
		bTr, aTr := newGatedPair("b", "a")
		bTr.gated = true

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		answerer, err := NewSession(SessionConfig{
			Log: discardLog(), SelfID: "a", PeerID: "b", Transport: aTr,
		})
		if err != nil {
			t.Fatalf("new answerer: %v", err)
		}
		defer answerer.Close()

		var mu sync.Mutex
		var observed []bool
		offerer, err := NewSession(SessionConfig{
			Log: discardLog(), SelfID: "b", PeerID: "a", Transport: bTr,
		})
		if err != nil {
			t.Fatalf("new offerer: %v", err)
		}
		defer offerer.Close()
		offerer.negotiationProbe = func(inFlight bool) {
			mu.Lock()
			observed = append(observed, inFlight)
			mu.Unlock()
		}

		answerer.Start(ctx)
		offerer.Start(ctx)

		if _, _, err := offerer.AddForwardTrack("fwd-1", "conclave"); err != nil {
			t.Fatalf("add first track: %v", err)
		}
		waitFor(t, "the first offer", 5*time.Second, func() bool {
			o, _, _ := bTr.counts()
			return o >= 1
		})
		if _, _, err := offerer.AddForwardTrack("fwd-2", "conclave"); err != nil {
			t.Fatalf("add second track: %v", err)
		}
		bTr.release()
		waitFor(t, "the follow-up offer", 5*time.Second, func() bool {
			o, _, _ := bTr.counts()
			return o >= 2
		})

		mu.Lock()
		defer mu.Unlock()
		if len(observed) < 2 {
			t.Fatalf("handler ran %d times, want at least 2", len(observed))
		}
		for i, inFlight := range observed {
			if inFlight {
				t.Fatalf("handler invocation %d saw an in-flight offer; pion only invokes it "+
					"from stable, so our own flag must already be clear or the offer is lost", i)
			}
		}
	})

	t.Run("a failing send retries and gives up after NegotiationRetries", func(t *testing.T) {
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
			t.Fatalf("add track: %v", err)
		}

		// One initial attempt, then NegotiationRetries retries — each armed off the
		// injected clock, so the test drives them instead of sleeping 1.25s.
		for i := 1; i <= NegotiationRetries; i++ {
			clk.waitCreated(t, i)
			clk.advance(NegotiationRetryDelay)
		}
		// Count OFFERS, not raw sends: the same failing transport also carries
		// trickled ICE candidates, which are not negotiation attempts.
		waitFor(t, "all attempts to be spent", 5*time.Second, func() bool {
			offers, _, _ := bTr.counts()
			return offers == 1+NegotiationRetries
		})
		// And then it STOPS: no further timer, no further attempt.
		stableFor(t, "no attempt past the bound", 300*time.Millisecond, func() bool {
			offers, _, _ := bTr.counts()
			return offers == 1+NegotiationRetries && clk.createdCount() == NegotiationRetries
		})
	})
}

// TestSessionRemoveTrack pins §7.2: a mid-call track removal renegotiates through
// the serializer, and removing from a session that is already closed is a satisfied
// postcondition rather than an error the caller must special-case at every site.
func TestSessionRemoveTrack(t *testing.T) {
	t.Run("removing from a closed session returns nil", func(t *testing.T) {
		bTr, _ := newGatedPair("b", "a")
		s, err := NewSession(SessionConfig{
			Log: discardLog(), SelfID: "b", PeerID: "a", Transport: bTr,
		})
		if err != nil {
			t.Fatalf("new session: %v", err)
		}
		_, sender, err := s.AddForwardTrack("fwd-1", "conclave")
		if err != nil {
			t.Fatalf("add forward track: %v", err)
		}
		if err := s.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		if err := s.RemoveTrack(sender); err != nil {
			t.Fatalf("RemoveTrack on a closed session = %v, want nil "+
				"(a closed session has already removed every track)", err)
		}
	})

	t.Run("removing a live track renegotiates once", func(t *testing.T) {
		bTr, aTr := newGatedPair("b", "a")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		answerer, err := NewSession(SessionConfig{
			Log: discardLog(), SelfID: "a", PeerID: "b", Transport: aTr,
		})
		if err != nil {
			t.Fatalf("new answerer: %v", err)
		}
		defer answerer.Close()
		offerer, err := NewSession(SessionConfig{
			Log: discardLog(), SelfID: "b", PeerID: "a", Transport: bTr,
		})
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
		// Wait for QUIESCENCE, not just for the first answer: pion may fire another
		// negotiation round on the return to stable, and starting the removal while
		// one is pending would make "one more offer" ambiguous.
		var before int
		waitFor(t, "the initial negotiation to settle", 10*time.Second, func() bool {
			if offerer.pc.SignalingState() != webrtc.SignalingStateStable ||
				offerer.pc.CurrentRemoteDescription() == nil {
				return false
			}
			o, _, _ := bTr.counts()
			if o != before {
				before = o
				return false
			}
			return true
		})

		if err := offerer.RemoveTrack(sender); err != nil {
			t.Fatalf("RemoveTrack: %v", err)
		}
		// The caller does NOT offer by hand: RemoveTrack fires negotiation-needed
		// and the serializer emits exactly one offer for it.
		waitFor(t, "the renegotiating offer", 5*time.Second, func() bool {
			o, _, _ := bTr.counts()
			return o == before+1
		})
		stableFor(t, "exactly one renegotiating offer", 300*time.Millisecond, func() bool {
			o, _, _ := bTr.counts()
			return o == before+1
		})
	})
}

// TestNegotiationLadderExhaustionIsReported closes the MAJOR where a session went
// permanently and SILENTLY mute.
//
// When the answer-deadline ladder ran out it simply returned: `negotiating` stayed
// true, the pc stayed in `have-local-offer`, and both serializer guards then blocked
// every future offer for the life of the session. Nothing logged at Error, and the
// claim that "a wedged session self-heals on the next tree" is false — the diff sees
// that neighbour as live, wanted and same-role, so the session survives every
// subsequent push and each AddForwardTrack lands on a pc that will never offer again.
//
// Exhaustion must therefore (a) clear the in-flight flag so the Session's own state
// is honest, and (b) TELL somebody, so the owner can re-create the edge.
func TestNegotiationLadderExhaustionIsReported(t *testing.T) {
	// The transport delivers; there is simply no answerer at the other end, which is
	// the role-inversion window that motivated the ladder in the first place.
	bTr, _ := newGatedPair("b", "a")
	clk := newFakeClock()

	failures := make(chan struct{}, 8)
	offerer, err := NewSession(SessionConfig{
		Log: discardLog(), SelfID: "b", PeerID: "a", Transport: bTr, Clock: clk,
		OnNegotiationFailed: func() { failures <- struct{}{} },
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
	waitFor(t, "the first offer", 5*time.Second, func() bool {
		o, _, _ := bTr.counts()
		return o == 1
	})
	for i := 1; i <= NegotiationRetries; i++ {
		clk.waitCreated(t, i)
		clk.advance(NegotiationAnswerTimeout)
	}
	waitFor(t, "the ladder to be spent", 5*time.Second, func() bool {
		o, _, _ := bTr.counts()
		return o == 1+NegotiationRetries
	})

	select {
	case <-failures:
	case <-time.After(5 * time.Second):
		t.Fatal("the ladder gave up without telling anyone; the edge is silently mute forever")
	}
	// Exactly once: a storm of callbacks would have the Router re-creating the edge
	// in a loop.
	select {
	case <-failures:
		t.Error("the exhaustion callback fired more than once")
	case <-time.After(200 * time.Millisecond):
	}

	offerer.mu.Lock()
	stillNegotiating := offerer.negotiating
	offerer.mu.Unlock()
	if stillNegotiating {
		t.Error("negotiating is still set after the ladder gave up: the Session's own view " +
			"of itself is wrong, and every later offer is suppressed by its own guard")
	}
}

// TestRemovalNudgeSurvivesTheAnswer closes the MAJOR where the removal renegotiation
// was usually lost.
//
// The nudge was spawned BEFORE SetRemoteDescription, so it read the signaling state
// while the pc was still `have-local-offer`, deferred, and left pendingLocalChange
// set — and pion provably never re-fires for a removal (see
// TestPionRemoveTrackDoesNotRenegotiate), so nothing ever picked it up. The departed
// source's m-line then lingered as exactly the stale sendrecv that stopPeer exists to
// clean up.
//
// Discrimination: the removal happens while an offer is OUTSTANDING, which is the
// only window in which the ordering matters, and the assertion is on the SDP that
// follows the answer. Against the old ordering no further offer is produced at all
// and this times out.
func TestRemovalNudgeSurvivesTheAnswer(t *testing.T) {
	bTr, aTr := newGatedPair("b", "a")
	bTr.gated = true // hold the first offer so the removal lands mid-negotiation

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

	_, sender, err := offerer.AddForwardTrack("fwd-gone", "conclave")
	if err != nil {
		t.Fatalf("add forward track: %v", err)
	}
	waitFor(t, "the first offer", 5*time.Second, func() bool {
		o, _, _ := bTr.counts()
		return o == 1
	})

	// Removed while the offer is in flight: the serializer defers the nudge, so the
	// ONLY thing that can still carry this removal is the answer path.
	if err := offerer.RemoveTrack(sender); err != nil {
		t.Fatalf("RemoveTrack: %v", err)
	}
	bTr.release()

	waitFor(t, "the renegotiation the removal earned", 10*time.Second, func() bool {
		o, _, _ := bTr.counts()
		return o >= 2
	})
	waitFor(t, "the removal to reach the wire", 10*time.Second, func() bool {
		desc := offerer.pc.LocalDescription()
		if desc == nil {
			return false
		}
		dirs := directionLines(desc.SDP)
		return len(dirs) == 1 && dirs[0] == "a=recvonly"
	})
}
