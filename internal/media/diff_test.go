package media

import (
	"reflect"
	"testing"

	"github.com/SammyUrfen/conclave/internal/overlay"
)

// chain builds a topology from parent→child pairs, deriving Root the way the
// coordinator would, so the tests read as trees rather than as edge lists.
func tree(root string, edges ...[2]string) *overlay.Topology {
	t := &overlay.Topology{Epoch: 1, Rev: 1, Root: root}
	for _, e := range edges {
		t.Edges = append(t.Edges, overlay.Edge{Parent: e[0], Child: e[1]})
	}
	return t
}

// TestDiffTopology is the §7.4 diff, extracted as a pure function precisely so the
// hard cases can be pinned without a PeerConnection anywhere near them.
func TestDiffTopology(t *testing.T) {
	cases := []struct {
		name string
		self string
		next *overlay.Topology
		live liveState
		want topoDiff
	}{
		{
			name: "the first topology opens every neighbour",
			self: "b",
			next: tree("a", [2]string{"a", "b"}, [2]string{"b", "c"}),
			live: liveState{},
			want: topoDiff{add: []string{"a", "c"}},
		},
		{
			name: "a converged tree is a no-op",
			self: "b",
			next: tree("a", [2]string{"a", "b"}, [2]string{"b", "c"}),
			live: liveState{
				parent: "a",
				// b and a are both relays here, so the name tie-break makes b the
				// offerer toward a; b is the relay on the (b,c) edge. Both sessions
				// were built as relay edges.
				roles:     map[string]bool{"a": true, "c": true},
				relayEdge: map[string]bool{"a": true, "c": true},
				peerRelay: map[string]bool{"a": true, "c": false},
				// a is a relay, so it publishes no media of its own and there is no
				// (a -> c) leg to carry. The only transit source here is c, upward.
				legs: []leg{{src: "c", child: "a"}},
			},
			want: topoDiff{},
		},
		{
			name: "a dropped neighbour is closed",
			self: "a",
			next: tree("a", [2]string{"a", "b"}),
			live: liveState{
				roles:     map[string]bool{"b": true, "c": true},
				relayEdge: map[string]bool{"b": true, "c": true},
			},
			want: topoDiff{remove: []string{"c"}},
		},
		{
			// C11: `a` keeps its session to `b`, but `b` stops being a relay, which
			// flips who may offer on that surviving edge. A diff that only looks at
			// the neighbour SET sees no change at all — and the session stays baked
			// as an answerer that can never publish its new forwarded m-lines. No
			// error, no log line, no media.
			name: "a surviving edge whose offerer role inverted is re-created",
			self: "a",
			next: tree("a", [2]string{"a", "b"}, [2]string{"a", "c"}),
			live: liveState{
				// Under a→b→c both endpoints were relays, so the tie-break gave the
				// offer to b: a's live session is baked as the ANSWERER.
				roles:     map[string]bool{"b": false},
				relayEdge: map[string]bool{"b": true},
				peerRelay: map[string]bool{"b": true}, // b WAS a relay under a→b→c
				legs:      nil,
			},
			want: topoDiff{add: []string{"c"}, recreate: []string{"b"}},
		},
		{
			name: "a re-parent keeps the old parent out of the removals",
			self: "c",
			next: tree("a", [2]string{"a", "b"}, [2]string{"a", "c"}),
			live: liveState{parent: "b", roles: map[string]bool{"b": false}},
			want: topoDiff{reparent: true, oldParent: "b", newParent: "a"},
		},
		{
			// A new sibling means every SURVIVING child needs one more forwarded
			// leg; the new neighbour's own legs are built by its session setup, so
			// they must not appear here or they would be added twice.
			name: "a new source adds one leg per surviving child",
			self: "a",
			next: tree("a", [2]string{"a", "b"}, [2]string{"a", "c"}, [2]string{"a", "d"}),
			live: liveState{
				roles:     map[string]bool{"b": true, "c": true},
				relayEdge: map[string]bool{"b": true, "c": true},
				legs:      []leg{{src: "b", child: "c"}, {src: "c", child: "b"}},
			},
			want: topoDiff{
				add:     []string{"d"},
				addLegs: []leg{{src: "d", child: "b"}, {src: "d", child: "c"}},
			},
		},
		{
			name: "a leaf that becomes a relay opens its children",
			self: "b",
			next: tree("a", [2]string{"a", "b"}, [2]string{"b", "c"}, [2]string{"b", "d"}),
			live: liveState{
				parent: "a", roles: map[string]bool{"a": false},
				relayEdge: map[string]bool{"a": false}, // b was a LEAF when this edge was built
				peerRelay: map[string]bool{"a": true},  // a was already the relay
			},
			want: topoDiff{add: []string{"c", "d"}, recreate: []string{"a"}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := diffTopology(tc.self, tc.next, tc.live)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("diffTopology()\n got %+v\nwant %+v", got, tc.want)
			}
		})
	}
}

// TestDiffTopologyDetachedSelf pins the degenerate input: a tree that does not name
// this peer at all must close everything rather than half-apply.
func TestDiffTopologyDetachedSelf(t *testing.T) {
	got := diffTopology("z", tree("a", [2]string{"a", "b"}),
		liveState{parent: "a", roles: map[string]bool{"a": false, "b": true}})
	want := topoDiff{remove: []string{"a", "b"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("diffTopology()\n got %+v\nwant %+v", got, want)
	}
}
