package media

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"

	"github.com/SammyUrfen/conclave/internal/metrics"
	"github.com/SammyUrfen/conclave/internal/overlay"
	"github.com/SammyUrfen/conclave/internal/signaling"
)

// pushTo sends topo to ONE named peer. The fixture's push() gives every peer the
// same tree, which is right for a coordinator but useless for the cases below: the
// whole point of a backup edge is that the two ends can momentarily disagree about
// the tree, and the failure modes worth testing live in that disagreement.
func (f *meetFixture) pushTo(t *testing.T, name string, topo *overlay.Topology) {
	t.Helper()
	payload, err := json.Marshal(topo)
	if err != nil {
		t.Fatalf("marshal topology: %v", err)
	}
	if err := f.coord.Send(signaling.Message{
		Type: signaling.TypeTopology, To: f.idOf(t, name), Payload: payload,
	}); err != nil {
		t.Fatalf("push topology to %s: %v", name, err)
	}
}

// killParentEdge makes child's Router believe its parent edge died, by posting the
// state transition pion's own OnState callback posts. It drives the identical code
// path while staying off pion's ~25s ICE failure timer.
func killParentEdge(f *meetFixture, child, parent string) {
	f.routers[child].postEvent(routerEvent{
		kind: evPeerState, peerName: parent, state: webrtc.PeerConnectionStateFailed,
	})
}

// TestBackupPromotionMakesTheNewParentTheOfferer is the structural half of the
// backup-edge defect.
//
// A backup edge is not in the tree, so Topology.Offers says nothing about it and the
// two ends have to settle the role some other way. The original rule was "whoever
// promotes, offers" — which is glare-free but puts the offer on the wrong end: only
// an OFFER can add forwarded m-lines, so an answering parent structurally cannot
// publish the tracks the child promoted it FOR. The rule is inverted here: the child
// asks (TypeBackupPromote) and ANSWERS; the authorized parent OFFERS.
//
// Mutations these two assertions catch:
//
//   - Restore opts.offerer = &yes on the promoter in startReparent and the first
//     assertion fails: the child is the offerer again, which is exactly the shipped
//     defect §9.5 recorded.
//   - Accept the promote frame but create the parent's session as the answerer (the
//     role deliver's offer path gives it) and the second assertion fails, with the
//     first still green — so the two ends are pinned independently and a change that
//     inverted only one of them, leaving BOTH answering or BOTH offering, cannot pass.
//
// Both fail against the pre-fix code.
func TestBackupPromotionMakesTheNewParentTheOfferer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	f := newMeetFixture(t, ctx, "promote-role", map[string]RouterConfig{
		"a": {},
		"b": {},
		// d gives the backup parent something real to forward, so the promoted edge
		// is one that must carry media rather than one exempt from the evidence rule.
		"d": {SendMedia: true},
		"c": {SendMedia: true},
	})

	f.announce(t, 1)
	topo := tree("a", [2]string{"a", "b"}, [2]string{"a", "d"}, [2]string{"b", "c"})
	topo.Backups = []overlay.Backup{{Node: "c", Parent: "a"}}
	f.push(t, topo)

	waitFor(t, "the tree to converge", 40*time.Second, func() bool {
		return allConnected(f.routers["a"].Stats(), 2) &&
			allConnected(f.routers["b"].Stats(), 2) &&
			allConnected(f.routers["c"].Stats(), 1)
	})

	killParentEdge(f, "c", "b")

	waitFor(t, "both ends of the backup edge to hold a session", 40*time.Second, func() bool {
		return f.routers["c"].hasSession("a") && f.routers["a"].hasSession("c")
	})

	if f.offererToward(t, "c", "a") {
		t.Error("the promoting child is the offerer on the backup edge: the parent then " +
			"answers, and an answerer cannot add the forwarded m-lines the child promoted it for")
	}
	if !f.offererToward(t, "a", "c") {
		t.Error("the backup parent is the answerer on the edge it was promoted onto, so its " +
			"first exchange cannot carry a single forwarded track")
	}
}

// TestBackupPromotionCarriesEveryForwardedSource is the behavioural half: the point
// of inverting the offerer is that the new parent's FIRST offer carries the whole
// forwarded track set.
//
// The shape is chosen so that m-line reuse cannot rescue an answering parent. Two
// peers (d and e) publish behind the backup parent, so the promoted edge owes c two
// forwarded tracks, while c's own offer carries exactly one video m-line. An
// answerer can fold at most that one m-line's worth of sending into its answer and
// has no way to add the other — so pre-fix c receives at most one of the two sources,
// indefinitely, and the meet only heals once the coordinator ratifies and the parent
// renegotiates as offerer.
//
// The NAMES are load-bearing. Topology.Offers breaks a relay-vs-relay tie on
// self > peer, so the root is "z" and the middle relay "m": the root offers on the
// in-tree edge and can therefore publish BOTH forwarded tracks toward m. Named a and
// b instead, the middle relay would win the tiebreak and the root would be the
// answerer on an edge that owes two tracks — the same m-line arithmetic, on an edge
// this change is explicitly not allowed to touch, and the fixture would be measuring
// that instead of the backup edge.
//
// Mutation: restoring the child-offers rule drops Tracks[z] to at most 1 and this
// test fails. Note the OK-report assertion alone would NOT discriminate — one
// arriving track satisfies the media-evidence clause — which is why the assertion is
// on the COUNT. That is exactly why the pre-existing TestRouterPromotesBackupParent
// passed while the live meet failed: its shape had one source behind the backup.
func TestBackupPromotionCarriesEveryForwardedSource(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	reports := make(chan metrics.Reparented, 4)
	f := newMeetFixture(t, ctx, "promote-media", map[string]RouterConfig{
		"z": {},
		"m": {},
		"d": {SendMedia: true},
		"e": {SendMedia: true},
		"c": {SendMedia: true, OnReparented: func(rep metrics.Reparented) {
			select {
			case reports <- rep:
			default:
			}
		}},
	})

	f.announce(t, 1)
	topo := tree("z",
		[2]string{"z", "m"}, [2]string{"z", "d"}, [2]string{"z", "e"}, [2]string{"m", "c"})
	topo.Backups = []overlay.Backup{{Node: "c", Parent: "z"}}
	f.push(t, topo)

	mID := f.idOf(t, "m")
	waitFor(t, "c to receive both sources through its primary parent", 60*time.Second, func() bool {
		return allConnected(f.routers["c"].Stats(), 1) && f.routers["c"].Stats().Tracks[mID] >= 2
	})

	killParentEdge(f, "c", "m")

	select {
	case rep := <-reports:
		if !rep.OK {
			t.Fatalf("backup promotion reported failure: %+v", rep)
		}
		if rep.From != "m" || rep.To != "z" {
			t.Errorf("Reparented{From:%q,To:%q}, want m\u2192z", rep.From, rep.To)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("no Reparented report: the peer never promoted its backup parent")
	}

	zID := f.idOf(t, "z")
	waitFor(t, "both forwarded sources to arrive over the promoted edge", 60*time.Second, func() bool {
		return f.routers["c"].Stats().Tracks[zID] >= 2
	})
}

// TestBackupPromoteThatNeverLandsFailsClosed covers the two ways the new frame can
// fail to produce an edge, and pins that neither one strands the child.
//
// The fixture pushes DIFFERENT trees to the two ends: c is told a is its backup, a is
// told nothing of the sort. That is one topology push apart from the real race — a
// backup parent holding an older Rev that lacks the assignment — and it stands in
// equally well for a promote frame lost on the wire, because the child cannot tell
// the two apart: in both cases no offer ever comes.
//
// Three assertions, three mutations:
//
//   - The child holds an ANSWERER session while it waits. Restore the child-offers
//     rule and this fails; it is the assertion that fails against the pre-fix code.
//   - The unauthorized parent creates NO session. Drop the acceptsBackupChild gate
//     from the promote handler and a stranger's frame conjures a session — an edge
//     outside the tree that neither end's coordinator knows about, and the §7.5a
//     admission rule's whole point is that it fails closed.
//   - The child still lands in the ReparentConnectTimeout ladder and keeps its old
//     parent-of-record. Wire the new path so the child waits for an offer without
//     arming the connect deadline (the obvious shape, since the child no longer
//     initiates anything) and it hangs forever: no report, no retry, no repair.
func TestBackupPromoteThatNeverLandsFailsClosed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	reports := make(chan metrics.Reparented, 4)
	f := newMeetFixture(t, ctx, "promote-refused", map[string]RouterConfig{
		"a": {},
		"b": {},
		"d": {SendMedia: true},
		"c": {SendMedia: true, OnReparented: func(rep metrics.Reparented) {
			select {
			case reports <- rep:
			default:
			}
		}},
	})

	f.announce(t, 1)
	withBackup := tree("a", [2]string{"a", "b"}, [2]string{"a", "d"}, [2]string{"b", "c"})
	withBackup.Backups = []overlay.Backup{{Node: "c", Parent: "a"}}
	noBackup := tree("a", [2]string{"a", "b"}, [2]string{"a", "d"}, [2]string{"b", "c"})
	for _, name := range []string{"b", "c", "d"} {
		f.pushTo(t, name, withBackup)
	}
	f.pushTo(t, "a", noBackup)

	waitFor(t, "the tree to converge", 40*time.Second, func() bool {
		return allConnected(f.routers["a"].Stats(), 2) &&
			allConnected(f.routers["b"].Stats(), 2) &&
			allConnected(f.routers["c"].Stats(), 1)
	})

	killParentEdge(f, "c", "b")

	waitFor(t, "c to open its side of the backup edge", 30*time.Second, func() bool {
		return f.routers["c"].hasSession("a")
	})
	if f.offererToward(t, "c", "a") {
		t.Error("the promoting child is the offerer on the backup edge; it must ask and answer, " +
			"because only the parent's offer can carry the forwarded m-lines")
	}
	if f.routers["a"].hasSession("c") {
		t.Error("a peer that its own topology does not name as this child's backup created a " +
			"session anyway: the admission rule must fail closed")
	}

	select {
	case rep := <-reports:
		if rep.OK {
			t.Fatalf("a promotion nobody answered reported success: %+v", rep)
		}
		if rep.Reason != "new parent never connected" {
			t.Errorf("Reparented.Reason = %q, want the connect-timeout rung of the ladder", rep.Reason)
		}
	case <-time.After(90 * time.Second):
		t.Fatal("no Reparented report: an unanswered promote left the child waiting forever")
	}

	if got := f.routers["c"].currentParent(); got != "b" {
		t.Errorf("c's parent-of-record is %q after abandoning the promotion, want b — reality is "+
			"still the old parent, and recording anything else makes the next push read the move "+
			"back as if it had been realised", got)
	}
}
