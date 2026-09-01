package media

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/SammyUrfen/conclave/internal/metrics"
	"github.com/SammyUrfen/conclave/internal/overlay"
)

// drain empties a report channel so a later assertion sees only new reports.
func drain(ch chan metrics.Reparented) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

// TestDiffTopologyLeafBecomesRelay is the CRITICAL case, and the names are chosen to
// land on the FAILING side of the comparison that hid it.
//
// A session is built one way for a leaf and another for a relay: only the relay path
// runs setupRelayEdge, which is the sole place setUpstream is called. So a peer
// promoted from leaf to relay must RE-CREATE its surviving parent edge, or that edge
// keeps a session that has no upstream registered and no forward loop — its new
// child negotiates the forwarded m-line and receives nothing, forever, with no error
// and no PLI path.
//
// The offerer role masks this exactly half the time. When the promoted peer sorts
// ABOVE its parent the role inverts and the edge is re-created for that reason, so
// everything works by luck. `alice` < `dave` puts us on the other half: the role does
// not flip, and a diff that only watches roles reports nothing to do.
func TestDiffTopologyLeafBecomesRelay(t *testing.T) {
	cases := []struct {
		name string
		self string
		next *overlay.Topology
		live liveState
		want topoDiff
	}{
		{
			// THE BUG. alice < dave, so Offers does not change on the dave edge.
			name: "a promoted leaf re-creates its parent edge even when the role does not flip",
			self: "alice",
			next: tree("dave", [2]string{"dave", "alice"}, [2]string{"alice", "bob"}),
			live: liveState{
				parent: "dave",
				roles:  map[string]bool{"dave": false},
				// The edge was built while alice was a LEAF: no setupRelayEdge ran.
				relayEdge: map[string]bool{"dave": false},
			},
			want: topoDiff{add: []string{"bob"}, recreate: []string{"dave"}},
		},
		{
			// The mirror the reviewer found working: zeb > dave, so the role flips
			// and the edge was already being re-created. Same outcome, different
			// reason — which is the point: both halves must behave identically.
			name: "the same promotion on the other side of the name comparison",
			self: "zeb",
			next: tree("dave", [2]string{"dave", "zeb"}, [2]string{"zeb", "bob"}),
			live: liveState{
				parent:    "dave",
				roles:     map[string]bool{"dave": false},
				relayEdge: map[string]bool{"dave": false},
			},
			want: topoDiff{add: []string{"bob"}, recreate: []string{"dave"}},
		},
		{
			// A relay DEMOTED to a leaf has the same problem in reverse: its session
			// carries forwarded m-lines it must no longer publish.
			name: "a demoted relay re-creates its parent edge too",
			self: "alice",
			next: tree("dave", [2]string{"dave", "alice"}, [2]string{"dave", "bob"}),
			live: liveState{
				parent:    "dave",
				roles:     map[string]bool{"dave": false, "bob": true},
				relayEdge: map[string]bool{"dave": true, "bob": true},
			},
			want: topoDiff{remove: []string{"bob"}, recreate: []string{"dave"}},
		},
		{
			// Control: relay-ness unchanged and role unchanged ⇒ nothing to do. If
			// this row ever starts asking for a re-creation, the check has become a
			// churn generator rather than a fix.
			name: "an unchanged role on an unchanged relay is left alone",
			self: "alice",
			next: tree("dave", [2]string{"dave", "alice"}, [2]string{"alice", "bob"}),
			live: liveState{
				parent:    "dave",
				roles:     map[string]bool{"dave": false, "bob": true},
				relayEdge: map[string]bool{"dave": true, "bob": true},
				legs:      []leg{{src: "bob", child: "dave"}},
			},
			want: topoDiff{},
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

// TestForwardLegsCarryEveryTransitSource is the MAJOR: an edge is not limited to one
// stream, and a source is identified by WHO ORIGINATED it, not by which neighbour
// handed it over.
//
// Keying a forwardSource by the neighbour collapses every track arriving on that
// edge into one record, and only the first reader generation is ever activated — so
// at the default -max-depth 2 a middle relay receives two forwarded tracks from the
// root and silently discards one, and its whole subtree loses that participant.
//
// The tree below is exactly that shape: root `dave` with children `alice` (a relay)
// and the leaves `zoe` and `fred`; `alice` has child `bob`. `alice` must forward BOTH
// zoe and fred to bob, and must expect both of them from its parent edge.
func TestForwardLegsCarryEveryTransitSource(t *testing.T) {
	topo := tree("dave",
		[2]string{"dave", "alice"}, [2]string{"dave", "zoe"},
		[2]string{"dave", "fred"}, [2]string{"alice", "bob"})

	t.Run("the middle relay forwards both far-side sources down and its own child up", func(t *testing.T) {
		want := []leg{
			{src: "bob", child: "dave"}, // upward: alice's own subtree
			{src: "fred", child: "bob"}, // downward: BOTH of the root's other leaves,
			{src: "zoe", child: "bob"},  // which is the pair the old model collapsed
		}
		got := wantedLegs(topo, "alice")
		sortLegs(got)
		sortLegs(want)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("wantedLegs(alice)\n got %+v\nwant %+v", got, want)
		}
	})

	t.Run("the root sends each side only what it cannot already see", func(t *testing.T) {
		want := []leg{
			{src: "fred", child: "alice"},
			{src: "zoe", child: "alice"},
			{src: "fred", child: "zoe"},
			{src: "zoe", child: "fred"},
		}
		got := wantedLegs(topo, "dave")
		sortLegs(got)
		sortLegs(want)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("wantedLegs(dave)\n got %+v\nwant %+v", got, want)
		}
	})

	t.Run("a relay publishes no media of its own, so it is never a source", func(t *testing.T) {
		for _, l := range append(wantedLegs(topo, "dave"), wantedLegs(topo, "alice")...) {
			if l.src == "dave" || l.src == "alice" {
				t.Errorf("leg %+v names a relay as a source; a relay adds no outbound track "+
					"of its own, so that m-line could never carry a packet", l)
			}
		}
	})

	t.Run("a leaf forwards nothing", func(t *testing.T) {
		if got := wantedLegs(topo, "bob"); got != nil {
			t.Errorf("wantedLegs(bob) = %+v, want none", got)
		}
	})

	t.Run("each edge expects the sources that actually arrive over it", func(t *testing.T) {
		cases := []struct {
			self, neighbour string
			want            []string
		}{
			// alice's parent edge carries the far side of the tree: both leaves.
			{self: "alice", neighbour: "dave", want: []string{"fred", "zoe"}},
			// alice's child edge carries only bob's own media.
			{self: "alice", neighbour: "bob", want: []string{"bob"}},
			// dave's edge to alice carries alice's subtree, which is just bob.
			{self: "dave", neighbour: "alice", want: []string{"bob"}},
			{self: "dave", neighbour: "zoe", want: []string{"zoe"}},
		}
		for _, tc := range cases {
			got := sourcesFrom(topo, tc.self, tc.neighbour)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("sourcesFrom(%s, %s) = %v, want %v", tc.self, tc.neighbour, got, tc.want)
			}
		}
	})
}

// TestForwarderTwoSourcesOverOneEdge is the forwarder half of the same MAJOR: two
// DISTINCT sources arriving over one upstream session must both flow. Under the
// old neighbour-keyed model they shared a record and only the first reader
// generation was ever activated, so the second was dropped by fanout with no log.
func TestForwarderTwoSourcesOverOneEdge(t *testing.T) {
	f := newForwarder(discardLog(), &uploadMeter{}, func(func()) {}, newFakeClock())
	parent := &captureRTCP{}
	// One session, two sources — the shape a middle relay actually sees.
	f.setUpstream("zoe", parent)
	f.setUpstream("fred", parent)
	toZoe := &captureTrack{}
	toFred := &captureTrack{}
	f.addOutLive("zoe", "bob", toZoe, nil)
	f.addOutLive("fred", "bob", toFred, nil)

	genZoe := f.newUpstreamGen("zoe")
	genFred := f.newUpstreamGen("fred")
	f.fanout("zoe", genZoe, vp8Pkt(10, 9000, true))
	f.fanout("fred", genFred, vp8Pkt(500, 45000, true))
	f.fanout("zoe", genZoe, vp8Pkt(11, 12000, false))
	f.fanout("fred", genFred, vp8Pkt(501, 48000, false))

	if got := len(toZoe.snapshot()); got != 2 {
		t.Errorf("zoe's leg carried %d packets, want 2", got)
	}
	if got := len(toFred.snapshot()); got != 2 {
		t.Errorf("fred's leg carried %d packets, want 2 — the second source on a shared "+
			"upstream must not be silently discarded", got)
	}
}

// TestStartReparentLeavesAnInFlightMoveAlone is the MAJOR that disables ALL failover:
// superseding before resolving the new target means an unresolvable name tears down
// the in-flight re-parent and then returns early, leaving r.rp non-nil, cancelled,
// naming a closed session and with no deadline armed. onParentLost then sees a move
// "already in progress" and refuses to promote a backup, so the next parent death is
// unrecoverable until some later push happens to name a different parent.
func TestStartReparentLeavesAnInFlightMoveAlone(t *testing.T) {
	ctx := context.Background()

	t.Run("an unresolvable target does not disturb a move in flight", func(t *testing.T) {
		r := NewRouter(discardLog(), nil, RouterConfig{SelfName: "c", Managed: true})
		r.topo = tree("a", [2]string{"a", "b"}, [2]string{"b", "c"})
		r.learnPeer("p-a", "a")
		r.setParent("b", "")

		r.startReparent(ctx, "b", "a", false) // resolvable: a is in the roster
		if r.rp == nil {
			t.Fatal("no re-parent started for a resolvable target")
		}
		inFlight := r.rp

		r.startReparent(ctx, "b", "ghost", false) // NOT in the roster
		if r.rp != inFlight {
			t.Fatalf("an unresolvable target replaced the in-flight move: rp = %+v", r.rp)
		}
		if _, _, _ = r.Realized(); r.pendingParent != "a" {
			t.Errorf("pendingParent = %q, want a — the in-flight target must survive", r.pendingParent)
		}
	})

	t.Run("a failed start leaves no zombie blocking failover", func(t *testing.T) {
		reports := make(chan metrics.Reparented, 4)
		r := NewRouter(discardLog(), nil, RouterConfig{
			SelfName: "c", Managed: true,
			OnReparented: func(rep metrics.Reparented) { reports <- rep },
		})
		r.topo = &overlay.Topology{
			Epoch: 1, Rev: 1, Root: "a",
			Edges:   []overlay.Edge{{Parent: "a", Child: "b"}, {Parent: "b", Child: "c"}},
			Backups: []overlay.Backup{{Node: "c", Parent: "a"}},
		}
		r.setParent("b", "")

		r.startReparent(ctx, "b", "ghost", false) // nothing resolves; nothing in flight
		drain(reports)
		if r.rp != nil {
			t.Fatalf("a failed start left a zombie re-parent: %+v", r.rp)
		}
		if r.pendingParent != "" {
			t.Errorf("pendingParent = %q after a failed start, want empty", r.pendingParent)
		}

		// The proof that the zombie MATTERS: with one left behind, onParentLost sees
		// a move already under way and refuses to promote the backup, so the parent
		// dying is unrecoverable. It must instead reach the backup path.
		r.onParentLost(ctx, "b")
		select {
		case rep := <-reports:
			if rep.Reason == "" || rep.From != "b" {
				t.Errorf("unexpected failover report: %+v", rep)
			}
			if got := r.currentTopo().BackupOf("c"); got != "" {
				t.Errorf("backup %q was not consumed; the promotion never ran", got)
			}
		default:
			t.Fatal("parent loss produced no failover attempt at all: a dangling re-parent " +
				"is still swallowing it")
		}
	})
}

// TestRouterPromotesLeafToRelay is the CRITICAL and the multi-source MAJOR end to
// end, on the half of the name comparison where both bugs are live.
//
// `alice` < `dave`, so when alice gains a child the offerer role on the alice–dave
// edge does NOT flip: dave is the sole relay before, both are relays after, and the
// tie-break still names dave. Nothing about the neighbour set changes either. So a
// diff that watches only roles and neighbours reports nothing to do on that edge,
// alice's session keeps the shape it was built with as a LEAF — no upstream
// registered, no forward loop, its parent's tracks already owned by the counting
// sink — and bob negotiates its forwarded m-lines and receives nothing at all. No
// error, no log, no PLI. Had the names been the other way round the role would have
// flipped, the edge would have been re-created, and everything would have worked.
//
// The same run covers the second bug: dave sends alice TWO forwarded tracks (zoe's
// and fred's), so bob must end up receiving BOTH. Keying a forwarding source by the
// NEIGHBOUR it arrived from collapses them into one record of which only the first
// reader generation is ever activated, and bob sees exactly one participant.
//
// Discrimination is therefore quantitative: 0 tracks against the CRITICAL, 1 against
// the MAJOR, 2 only when both are fixed.
func TestRouterPromotesLeafToRelay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	f := newMeetFixture(t, ctx, "promote", map[string]RouterConfig{
		"dave":  {},                // root relay
		"alice": {},                // leaf first, relay second — the peer under test
		"zoe":   {SendMedia: true}, // two senders, so the promoted relay must carry
		"fred":  {SendMedia: true}, // more than one source over its single parent edge
		"bob":   {},                // joins the tree only in the second topology
	})
	f.announce(t, 1)

	// alice is a LEAF here, so its parent edge is built by the non-relay path.
	topo1 := tree("dave",
		[2]string{"dave", "alice"}, [2]string{"dave", "zoe"}, [2]string{"dave", "fred"})
	f.push(t, topo1)
	waitFor(t, "the star to converge with alice as a leaf", 60*time.Second, func() bool {
		return allConnected(f.routers["dave"].Stats(), 3) &&
			allConnected(f.routers["alice"].Stats(), 1) &&
			receivedAny(f.routers["alice"].Stats())
	})
	if got := f.routers["alice"].isRelayNow(); got {
		t.Fatal("alice is already a relay; the promotion under test never happens")
	}

	// PROMOTION: alice gains a child, and nothing else about its parent edge changes.
	topo2 := tree("dave",
		[2]string{"dave", "alice"}, [2]string{"dave", "zoe"},
		[2]string{"dave", "fred"}, [2]string{"alice", "bob"})
	topo2.Rev = 2
	f.push(t, topo2)

	waitFor(t, "bob to attach to the promoted relay", 60*time.Second, func() bool {
		return f.routers["alice"].isRelayNow() && allConnected(f.routers["bob"].Stats(), 1)
	})
	if got := f.offererToward(t, "alice", "dave"); got {
		t.Fatal("alice became the offerer toward dave, so the role DID flip and this run " +
			"is exercising the half of the comparison that already worked")
	}

	aliceID := f.idOf(t, "alice")
	waitFor(t, "bob to receive BOTH sources through the promoted relay", 60*time.Second, func() bool {
		return f.routers["bob"].Stats().Tracks[aliceID] >= 2
	})

	// bob's only session is to alice, so every packet it holds transited the peer
	// that was a leaf a moment ago.
	if got := len(f.routers["bob"].Stats().Peers); got != 1 {
		t.Errorf("bob holds %d sessions, want 1 (the promoted relay only)", got)
	}
	// And the relay really is translating keyframe requests upstream, which is the
	// path that is dead when setUpstream never ran.
	waitFor(t, "the promoted relay to forward a keyframe request upstream", 30*time.Second, func() bool {
		return f.routers["alice"].fwd.PLIForwarded() > 0
	})
}
