package media

import (
	"reflect"
	"testing"

	"github.com/SammyUrfen/conclave/internal/overlay"
)

// TestDiffTopologyKeepsAPromotedBackupChild closes the window inside the failover
// path itself.
//
// §7.5a lets a peer accept a child that promoted it as a backup parent, and that
// edge deliberately does NOT exist in the tree — it exists precisely because the
// tree is momentarily wrong. But the diff removes every live neighbour the tree does
// not name, so ANY unrelated push arriving before the coordinator ratifies drops the
// edge: the child that has just survived losing its parent loses its replacement
// too, for a reason that has nothing to do with it.
//
// The exemption has to be narrow in both directions, so this table asserts the
// exemption AND all three of its exits. A blanket "never remove a stranger" would
// pass the first row and fail the second, which is the whole point of having it.
func TestDiffTopologyKeepsAPromotedBackupChild(t *testing.T) {
	// a is the root; c hangs off b and has just promoted a as its backup parent, so
	// a holds a live session to c that the tree does not describe.
	base := func() *overlay.Topology {
		return &overlay.Topology{
			Epoch: 1, Rev: 1, Root: "a",
			Edges: []overlay.Edge{
				{Parent: "a", Child: "b"}, {Parent: "a", Child: "d"}, {Parent: "b", Child: "c"},
			},
			Backups: []overlay.Backup{{Node: "c", Parent: "a"}},
		}
	}
	// What a's Router holds: real edges to b and d, plus the unmodelled edge to c,
	// recorded with the parent c was failing over FROM.
	//
	// roles["c"] is TRUE because a is the OFFERER on a promoted backup edge: the
	// child asks with TypeBackupPromote and answers, and only an offer can carry the
	// forwarded m-lines. Modelling it as the answerer — the role this edge had before
	// the inversion — describes a state acceptBackupChild can no longer produce, and
	// it silently changes the ratification row's answer below.
	live := func(backupFrom string) liveState {
		st := liveState{
			roles:     map[string]bool{"b": false, "d": true, "c": true},
			relayEdge: map[string]bool{"b": true, "d": true, "c": true},
			peerRelay: map[string]bool{"b": true, "d": false, "c": false},
			legs:      []leg{{src: "c", child: "d"}, {src: "d", child: "b"}, {src: "d", child: "c"}},
		}
		if backupFrom != "" {
			st.backupChild = map[string]string{"c": backupFrom}
		}
		return st
	}

	movedElsewhere := base()
	movedElsewhere.Edges = []overlay.Edge{
		{Parent: "a", Child: "b"}, {Parent: "a", Child: "d"}, {Parent: "d", Child: "c"},
	}
	reassigned := base()
	reassigned.Backups = []overlay.Backup{{Node: "c", Parent: "d"}}
	ratified := base()
	ratified.Edges = []overlay.Edge{
		{Parent: "a", Child: "b"}, {Parent: "a", Child: "d"}, {Parent: "a", Child: "c"},
	}

	cases := []struct {
		name         string
		next         *overlay.Topology
		live         liveState
		wantRemove   []string
		wantRecreate []string
	}{
		{
			// THE WINDOW. Nothing about c changed; the push is about something else
			// entirely. The promoted edge must survive it.
			name: "an unrelated push leaves the promoted edge alone",
			next: base(), live: live("b"),
			wantRemove: nil,
		},
		{
			// The control that keeps the exemption honest: an identical session with
			// no backup warrant behind it is still a stranger, and strangers go.
			name: "a live session with no backup warrant is still dropped",
			next: base(), live: live(""),
			wantRemove: []string{"c"},
		},
		{
			// Exit 1 — the coordinator ratified: the edge is now a real tree edge,
			// so the exemption is not consulted at all and the ordinary machinery
			// takes over. c is NOT re-created, and that is the offerer inversion
			// paying for itself: a already offers on the promoted edge and a offers
			// on a real one too, so ratification costs no interruption at all. Before
			// the inversion this row re-created c, because the edge had been built as
			// the answerer side of something the tree did not name.
			name: "ratification hands the edge to the normal machinery",
			next: ratified, live: live("b"),
			// b appears because moving c off it made b a leaf, and that genuinely
			// changes the track set b publishes on its edge to a. It is collateral of
			// the tree change, not of the exemption.
			wantRemove: nil, wantRecreate: []string{"b"},
		},
		{
			// Exit 2 — the coordinator placed c somewhere else. It has acted on the
			// failure, our edge is obsolete, and holding it would mean uploading to
			// a peer that is not ours.
			name: "the coordinator placing the child elsewhere ends the exemption",
			next: movedElsewhere, live: live("b"),
			// Same collateral, both ways: b stopped being a relay and d started.
			wantRemove: []string{"c"}, wantRecreate: []string{"b", "d"},
		},
		{
			// Exit 3 — the warrant itself is withdrawn. The assignment is what
			// authorised the edge (§7.5a), so without it there is nothing to stand on.
			name: "the coordinator reassigning the backup ends the exemption",
			next: reassigned, live: live("b"),
			wantRemove: []string{"c"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := diffTopology("a", tc.next, tc.live)
			if !reflect.DeepEqual(got.remove, tc.wantRemove) {
				t.Errorf("remove = %v, want %v", got.remove, tc.wantRemove)
			}
			if !reflect.DeepEqual(got.recreate, tc.wantRecreate) {
				t.Errorf("recreate = %v, want %v", got.recreate, tc.wantRecreate)
			}
		})
	}
}
