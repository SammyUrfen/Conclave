package media

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"

	"github.com/SammyUrfen/conclave/internal/metrics"
	"github.com/SammyUrfen/conclave/internal/overlay"
)

// TestRouterRealizedTopology is the §6.6 input, asserted end to end on a live meet.
//
// Discrimination is the point and it is structural: the topology is authored in
// NAMES (a, b, c, d) while the server assigns unrelated runtime IDS (p2, p3, …), and
// Router.Stats — the only public view before this — is keyed by id. So an
// implementation that forwards ids straight through, or that resolves them wrongly,
// produces "p3" where this test demands "b" and fails on every single assertion. The
// test also asserts explicitly that no returned label is any peer's id.
//
// The tree is shaped to pin two more things at once:
//   - a's children are declared in edge order c-then-b, but must be REPORTED b-then-c.
//     Emitting them in edge order (or in map order) fails, which is exactly the
//     failure that would make a rebuilt Topology.Edges depend on nothing but a hash
//     seed.
//   - b is a relay WITH a parent, so parent and children are read from the same peer
//     map in one call and cannot be confused for one another.
func TestRouterRealizedTopology(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	f := newMeetFixture(t, ctx, "realized", map[string]RouterConfig{
		"a": {},
		"b": {},
		"c": {SendMedia: true},
		"d": {},
	})
	f.announce(t, 1)
	// Edge order is deliberately NOT sorted: {a,c} before {a,b}.
	f.push(t, tree("a", [2]string{"a", "c"}, [2]string{"a", "b"}, [2]string{"b", "d"}))

	waitFor(t, "the tree to converge", 40*time.Second, func() bool {
		return allConnected(f.routers["a"].Stats(), 2) &&
			allConnected(f.routers["b"].Stats(), 2) &&
			allConnected(f.routers["c"].Stats(), 1) &&
			allConnected(f.routers["d"].Stats(), 1)
	})

	cases := []struct {
		self         string
		wantParent   string
		wantChildren []metrics.ChildLink
	}{
		{
			self: "a", wantParent: "",
			// Sorted ascending, NOT in the edge order the tree declared them.
			wantChildren: []metrics.ChildLink{{Name: "b", State: "connected"}, {Name: "c", State: "connected"}},
		},
		{self: "b", wantParent: "a", wantChildren: []metrics.ChildLink{{Name: "d", State: "connected"}}},
		{self: "c", wantParent: "a", wantChildren: nil},
		{self: "d", wantParent: "b", wantChildren: nil},
	}

	for _, tc := range cases {
		t.Run(tc.self, func(t *testing.T) {
			parent, parentState, children := f.routers[tc.self].Realized()
			if parent != tc.wantParent {
				t.Errorf("parent = %q, want %q (NAMES, not runtime ids)", parent, tc.wantParent)
			}
			wantState := "connected"
			if tc.wantParent == "" {
				wantState = "" // the root has no parent edge to describe
			}
			if parentState != wantState {
				t.Errorf("parentState = %q, want %q", parentState, wantState)
			}
			if !reflect.DeepEqual(children, tc.wantChildren) {
				t.Errorf("children = %+v, want %+v", children, tc.wantChildren)
			}

			// Nothing reported may be a runtime id. This is the assertion that fails
			// loudest against an id-keyed implementation, including one that happens
			// to look right on a fixture where names and ids coincide.
			for _, name := range []string{"a", "b", "c", "d"} {
				id := f.idOf(t, name)
				if parent == id {
					t.Errorf("parent = %q, which is %s's runtime id, not its name", parent, name)
				}
				for _, ch := range children {
					if ch.Name == id {
						t.Errorf("child %q is %s's runtime id, not its name", ch.Name, name)
					}
				}
			}
		})
	}

	// Whatever Realized reports must survive Normalize unchanged, or the sender and
	// the receiver disagree about a frame that is supposed to be byte-identical
	// between two peers reporting the same edges.
	_, _, children := f.routers["a"].Realized()
	h := metrics.Heartbeat{Name: "a", Children: children}
	before := append([]metrics.ChildLink(nil), h.Children...)
	h.Normalize()
	if !reflect.DeepEqual(before, h.Children) {
		t.Errorf("Normalize reordered Realized's output:\n got %+v\nwant %+v", h.Children, before)
	}
}

// TestRouterSelfID pins the Phase 6 accessor: an announcement names the coordinator
// by runtime ID, so a peer comparing it against its -name can never recognise
// itself. The assertion is that SelfID is the server's id and specifically NOT the
// topology name.
func TestRouterSelfID(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	f := newMeetFixture(t, ctx, "selfid", map[string]RouterConfig{"a": {}, "b": {}})
	f.announce(t, 1)

	for _, name := range []string{"a", "b"} {
		got := f.routers[name].SelfID()
		if want := f.idOf(t, name); got != want {
			t.Errorf("%s.SelfID() = %q, want the server-assigned %q", name, got, want)
		}
		if got == name {
			t.Errorf("%s.SelfID() returned the topology name; an announcement is keyed by id", name)
		}
	}
}

// TestRouterRealizedReportsUnconnectedEdges is the mid-handover case §6.6 must
// reconstruct correctly, and it is deterministic on purpose — arranging a genuinely
// half-connected pion session on a timer would be a flake generator.
//
// Two properties, both of which a naive implementation gets wrong in opposite
// directions:
//
//   - A child whose PeerConnection has NOT reached connected must still be reported,
//     with its true state. Filtering on "connected" hides exactly the edges a new
//     coordinator needs to see, and filtering on the current TREE reports edges that
//     were commanded but never realized.
//   - A re-parent's PENDING new parent must NOT be reported as a child. It has a
//     live session and is not the realized parent, so the obvious "every session
//     that is not my parent" rule mislabels it — and a coordinator rebuilding from
//     that gets an edge pointing the wrong way.
func TestRouterRealizedReportsUnconnectedEdges(t *testing.T) {
	r := NewRouter(discardLog(), nil, RouterConfig{SelfName: "b", Managed: true})
	r.topo = &overlay.Topology{
		Epoch: 1, Rev: 1, Root: "a",
		Edges: []overlay.Edge{{Parent: "a", Child: "b"}, {Parent: "b", Child: "d"}},
	}

	// Sessions are built directly over an in-memory transport whose far end never
	// answers, so every PeerConnection stays in its initial state for the whole test.
	link := func(t *testing.T, id, peerName string) {
		t.Helper()
		tr, _ := newGatedPair(id, "self")
		sess, err := NewSession(SessionConfig{
			Log: discardLog(), SelfID: "self", PeerID: id, Transport: tr,
		})
		if err != nil {
			t.Fatalf("new session for %s: %v", peerName, err)
		}
		t.Cleanup(func() { _ = sess.Close() })
		r.learnPeer(id, peerName)
		r.mu.Lock()
		r.peers[id] = &peerLink{session: sess, cancel: func() {}}
		r.mu.Unlock()
	}
	link(t, "p-a", "a") // the realized parent
	link(t, "p-d", "d") // a child that has not connected
	link(t, "p-z", "z") // the pending new parent of a re-parent in flight

	r.mu.Lock()
	r.parentInUse = "a"
	r.pendingParent = "z"
	r.mu.Unlock()

	parent, parentState, children := r.Realized()
	if parent != "a" {
		t.Errorf("parent = %q, want a", parent)
	}
	if parentState != webrtc.PeerConnectionStateNew.String() {
		t.Errorf("parentState = %q, want %q — an unconnected parent edge must report its "+
			"TRUE state, not be blanked", parentState, webrtc.PeerConnectionStateNew)
	}
	want := []metrics.ChildLink{{Name: "d", State: webrtc.PeerConnectionStateNew.String()}}
	if !reflect.DeepEqual(children, want) {
		t.Errorf("children = %+v, want %+v — an unconnected child must be reported and the "+
			"pending new parent must not be", children, want)
	}
}

// TestRouterRealizedChildOrder makes the ordering guarantee a DETERMINISTIC
// assertion rather than a probabilistic one.
//
// The integration test above pins the order too, but with two children an unsorted
// implementation still passes about half the time — Go randomises map iteration per
// range, so a two-element map is a coin flip. Here the peer map holds five children
// inserted in reverse order and Realized is called many times: an implementation
// that returns map order has a 1/120 chance of looking sorted on any one call, so
// across the repeats it is caught with certainty, while a sorted implementation
// passes every time.
//
// Ordering is not cosmetic. A new coordinator rebuilds Topology.Edges from these
// frames, and edge order is a replayed invariant of the stability-preserving builder
// — so an unordered slice here would make the first tree of every epoch depend on
// nothing but the hash seed.
func TestRouterRealizedChildOrder(t *testing.T) {
	r := NewRouter(discardLog(), nil, RouterConfig{SelfName: "b", Managed: true})
	r.mu.Lock()
	r.parentInUse = "a"
	r.mu.Unlock()

	// Inserted in descending order, so "insertion order" and "sorted" disagree.
	for _, name := range []string{"e", "d", "c", "b2", "a2"} {
		tr, _ := newGatedPair("p-"+name, "self")
		sess, err := NewSession(SessionConfig{
			Log: discardLog(), SelfID: "self", PeerID: "p-" + name, Transport: tr,
		})
		if err != nil {
			t.Fatalf("new session for %s: %v", name, err)
		}
		t.Cleanup(func() { _ = sess.Close() })
		r.learnPeer("p-"+name, name)
		r.mu.Lock()
		r.peers["p-"+name] = &peerLink{session: sess, cancel: func() {}}
		r.mu.Unlock()
	}

	want := []string{"a2", "b2", "c", "d", "e"}
	const repeats = 30 // (1/120)^-1 per call; 30 makes a map-order implementation certain to fail
	for i := 0; i < repeats; i++ {
		_, _, children := r.Realized()
		got := make([]string, 0, len(children))
		for _, ch := range children {
			got = append(got, ch.Name)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("call %d: children = %v, want %v (ascending by name)", i, got, want)
		}
	}
}
