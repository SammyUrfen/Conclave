package media

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/SammyUrfen/conclave/internal/metrics"
	"github.com/SammyUrfen/conclave/internal/overlay"
	"github.com/SammyUrfen/conclave/internal/signaling"
)

// dialPair brings up a Hub on a throwaway server and dials two bare signaling
// clients into the same room, returning them plus the id the server assigned the
// SECOND one. It is deliberately not the meetFixture: the subject here is a single
// frame on the wire, and the far end must be a client with no Router at all so that
// what it receives is exactly what was sent — nothing consumed, nothing answered.
func dialPair(t *testing.T, ctx context.Context, room, near, far string) (nearC, farC *signaling.Client, farID string) {
	t.Helper()
	hub := signaling.NewHub(discardLog())
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ws", hub.ServeWS)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	farC, err := signaling.Dial(ctx, discardLog(), srv.URL, room, far)
	if err != nil {
		t.Fatalf("%s dial: %v", far, err)
	}
	t.Cleanup(func() { _ = farC.Close() })
	// The far end joins first so its own TypeJoined carries its server-assigned id
	// in To — the address the promoter would send TypeBackupPromote to.
	select {
	case msg, ok := <-farC.Incoming():
		if !ok || msg.Type != signaling.TypeJoined {
			t.Fatalf("%s first frame = %+v (open=%v), want joined", far, msg, ok)
		}
		farID = msg.To
	case <-time.After(10 * time.Second):
		t.Fatalf("%s never received its joined frame", far)
	}

	nearC, err = signaling.Dial(ctx, discardLog(), srv.URL, room, near)
	if err != nil {
		t.Fatalf("%s dial: %v", near, err)
	}
	t.Cleanup(func() { _ = nearC.Close() })
	return nearC, farC, farID
}

// awaitBarrier drains c until the sentinel TypeCandidate arrives, failing if a
// TypeBackupPromote shows up first.
//
// The barrier is what makes "the frame was not sent" a real assertion rather than a
// guess about how long to wait. A peer's outbound frames leave through one write
// pump in queue order and the Hub relays each connection's frames in the order it
// reads them, so a sentinel queued AFTER the promote's send point cannot overtake
// it: if the sentinel has arrived and no promote has, no promote was ever queued.
func awaitBarrier(t *testing.T, c *signaling.Client) {
	t.Helper()
	deadline := time.After(15 * time.Second)
	for {
		select {
		case msg, ok := <-c.Incoming():
			if !ok {
				t.Fatal("the far end's signaling client closed before the barrier arrived")
			}
			switch msg.Type {
			case signaling.TypeBackupPromote:
				// WRONG BEHAVIOUR: this is the glare. Our session to this peer kept
				// the OFFERER role the tree gave it (the override was discarded), and
				// the frame asks the far end to offer as well. pion v4 has no rollback
				// out of have-local-offer (§5.12), so if the far end holds no session
				// back and honours the request, the edge wedges with no error and no
				// timeout that heals it — the single unrecoverable failure on this
				// path.
				t.Fatal("the promoter sent TypeBackupPromote after its answerer role was " +
					"discarded: it is still the offerer and has just asked the far end to offer too")
			case signaling.TypeCandidate:
				return
			}
		case <-deadline:
			t.Fatal("the barrier frame never arrived; the negative assertion above proved nothing")
		}
	}
}

// TestPromoteFrameIsWithheldWhenTheAnswererRoleWasDiscarded pins the one failure on
// the backup edge that does not heal.
//
// Sequence. c's topology illegally names its own CHILD d as its backup parent, so c
// already holds a session to d — as the OFFERER, since a relay offers toward a leaf.
// c's parent dies, c promotes d, and startPeerOpt returns at its already-exists
// guard BEFORE it reads opts.offerer: the answerer role c asked for is dropped and c
// stays the offerer. Sending TypeBackupPromote anyway asks d to offer as well, and a
// d holding no session back will do exactly that. Both ends then sit in
// have-local-offer, which pion v4 cannot leave (§5.12): no error, no timeout, no
// repair. Every other failure on this path — a refused frame, a lost frame, a dead
// peer — is a ReparentConnectTimeout that heals.
//
// Reachability. assignBackups never names a neighbour as a backup and Validate
// enforces it, but Router.applyTopology fences a pushed tree without ever calling
// Validate (§8.3), and a hand-authored -topology file is a supported mode. So the
// tree below is one Validate would reject, on purpose.
//
// Two assertions, two mutations:
//
//   - No TypeBackupPromote reaches d. Restore the unconditional send (the shipped
//     shape: the frame goes out whether or not startPeerOpt created the session) and
//     this fails — it is the assertion that fails against the pre-fix code.
//   - The promotion still lands in the existing failure ladder: the connect deadline
//     fires and reports OK:false with the connect-timeout reason. Withhold the frame
//     by returning early from the viaBackup arm, or otherwise skip the failure
//     report, and this fails — a promotion that goes quiet is worse than one that
//     glares, because the coordinator never learns to repair it.
func TestPromoteFrameIsWithheldWhenTheAnswererRoleWasDiscarded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cc, dc, dID := dialPair(t, ctx, "promote-glare", "c", "d")

	log, out := captureLog()
	clk := newFakeClock()
	reports := make(chan metrics.Reparented, 4)
	r := NewRouter(log, cc, RouterConfig{
		SelfName: "c", Managed: true, Clock: clk,
		OnReparented: func(rep metrics.Reparented) {
			select {
			case reports <- rep:
			default:
			}
		},
	})
	t.Cleanup(r.closeAll)

	// a→b→c→d, and c's backup is illegally its own child d.
	r.topo = &overlay.Topology{
		Epoch: 1, Rev: 1, Root: "a",
		Edges: []overlay.Edge{
			{Parent: "a", Child: "b"}, {Parent: "b", Child: "c"}, {Parent: "c", Child: "d"},
		},
		Backups: []overlay.Backup{{Node: "c", Parent: "d"}},
	}
	if sess := linkDerivedPeer(t, r, dID, "d"); !sess.Offerer() {
		t.Fatal("fixture: c must already hold an OFFERER session to d — the glare only exists " +
			"when the role in force is the opposite of the one the promotion wants")
	}
	r.setParent("b", "")

	r.onParentLost(ctx, "b")

	// Queued from this goroutine, so it is strictly behind anything startReparent
	// sent. See awaitBarrier.
	if err := cc.Send(signaling.Message{Type: signaling.TypeCandidate, To: dID}); err != nil {
		t.Fatalf("send barrier frame: %v", err)
	}
	awaitBarrier(t, dc)

	// The discard itself is already pinned by TestRoleOverrideIsNeverSilentlyDiscarded;
	// asserting it here too is what makes the negative above meaningful rather than
	// vacuous — a promotion that never started would also send no frame.
	if r.sessionByName("d") == nil || !r.sessionByName("d").Offerer() {
		t.Fatal("fixture: c's session to d is no longer the offerer, so this run did not " +
			"exercise the discarded-override case at all")
	}
	if !warnedRoleDiscard(out.String(), "new_parent=d", "wanted_offerer=false") {
		t.Errorf("no WARN naming the peer and the role that was not applied; log was:\n%s", out.String())
	}

	if r.rp == nil || r.rp.phase != rpOpening {
		t.Fatalf("no re-parent left in flight: rp=%+v — withholding the frame must leave the "+
			"outcome in the ReparentConnectTimeout ladder, not abandon it early", r.rp)
	}

	// Drive the ladder the Run goroutine would: the connect deadline is the rung a
	// refused or lost promote already lands on, and a withheld one must land there too.
	clk.advance(ReparentConnectTimeout)
	select {
	case ev := <-r.internal:
		r.handleInternal(ctx, ev)
	case <-time.After(10 * time.Second):
		t.Fatal("the connect deadline never fired: nothing owns the outcome of a withheld promote")
	}

	select {
	case rep := <-reports:
		if rep.OK {
			t.Fatalf("a promotion whose role was never applied reported success: %+v", rep)
		}
		if rep.Reason != "new parent never connected" {
			t.Errorf("Reparented.Reason = %q, want the connect-timeout rung of the ladder", rep.Reason)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no Reparented report: the promotion went silent instead of failing")
	}

	if got := r.currentParent(); got != "b" {
		t.Errorf("c's parent-of-record is %q after abandoning the promotion, want b", got)
	}
}
