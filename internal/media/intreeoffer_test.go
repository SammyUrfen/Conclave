package media

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestInTreeAnswererStrandsSourcesPastTheOfferedMLines PINS A DEFECT THAT IS STILL
// THERE. It is a characterization test, not a specification: it asserts what this
// build does, so the behaviour cannot drift under anyone silently.
//
// THE DEFECT, and it is PRE-EXISTING. Topology.Offers breaks a relay-vs-relay tie on
// `self > peer`, so on the in-tree edge between root "a" and middle relay "m" the
// MIDDLE relay wins and the ROOT answers. Only an offer can create m-lines (§5.12),
// so the answering root can publish forwarded tracks only into m-lines that m's offer
// already carried. Past that count the remaining sources are stranded INDEFINITELY —
// no error, no log line, just participants missing from a whole subtree, and the loss
// propagates one hop further to the leaf behind m.
//
// THE BOUND IS AN ACCIDENT, NOT A FIX. Nobody set out to raise it. The quality-layer
// work (§7.5) made the OFFERING side add len(layerLadder) receive slots so a
// multi-layer origin has somewhere to publish its rungs, and those slots are equally
// somewhere an answering relay can fold a forwarded track. Measured on this fixture,
// holding six sources behind the answering root and varying how many forwarded legs
// m carries back up the same edge:
//
//	m's legs up | sources owed | sources m received
//	          1 |            6 |                  4
//	          2 |            6 |                  5
//	          3 |            6 |                  6   (uncapped — owed == bound)
//
// So the bound is len(layerLadder) + len(legsToward(m, root)), i.e. exactly the
// m-lines m's own offer carried that the root can send on — NOT len(layerLadder), and
// not a constant. This fixture gives m one leg up (leaf "c"), so the bound is 4.
//
// DELETE THIS TEST WHEN Offers ITSELF IS FIXED — when an in-tree edge no longer makes
// the node owing forwarded tracks the answerer. Do NOT delete it by raising the
// numbers; a larger bound is still a bound.
func TestInTreeAnswererStrandsSourcesPastTheOfferedMLines(t *testing.T) {
	// inTreeAnswererBound is how many forwarded tracks the ANSWERING root can get
	// onto the wire toward m: the len(layerLadder) receive slots m offers plus the
	// one forwarded leg m carries back up (leaf "c"). Both terms are measured above.
	inTreeAnswererBound := len(layerLadder) + 1

	// probe runs root → m → c with `sources` publishers hanging off the root, waits
	// for m to reach `want` forwarded sources over its edge to the root and for that
	// to settle, and reports what m and the leaf c behind it ended up with.
	probe := func(t *testing.T, room, root string, sources, want int) (int, int) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
		defer cancel()

		pubs := []string{"d", "e", "g", "h", "i", "j"}[:sources]
		cfgs := map[string]RouterConfig{root: {}, "m": {}, "c": {SendMedia: true}}
		edges := [][2]string{{root, "m"}, {"m", "c"}}
		for _, p := range pubs {
			cfgs[p] = RouterConfig{SendMedia: true}
			edges = append(edges, [2]string{root, p})
		}

		f := newMeetFixture(t, ctx, room, cfgs)
		f.announce(t, 1)
		f.push(t, tree(root, edges...))

		waitFor(t, "the tree to converge", 90*time.Second, func() bool {
			return allConnected(f.routers[root].Stats(), sources+1) &&
				allConnected(f.routers["m"].Stats(), 2) &&
				allConnected(f.routers["c"].Stats(), 1)
		})
		rootID, mID := f.idOf(t, root), f.idOf(t, "m")
		// Reaching `want` is only half the assertion — the question this test exists
		// to answer is whether the NEXT source ever follows, so settle well past the
		// point where a renegotiation would have landed before reading the count.
		waitFor(t, fmt.Sprintf("m to receive %d forwarded sources", want), 90*time.Second, func() bool {
			return f.routers["m"].Stats().Tracks[rootID] >= want
		})
		stableFor(t, "m's forwarded source count to settle", 5*time.Second, func() bool {
			return f.routers["m"].Stats().Tracks[rootID] >= want
		})
		t.Logf("%s offers toward m = %v; m offers toward %s = %v",
			root, f.offererToward(t, root, "m"), root, f.offererToward(t, "m", root))
		return f.routers["m"].Stats().Tracks[rootID], f.routers["c"].Stats().Tracks[mID]
	}

	// "a" < "m", so m offers and a answers — the defective arrangement.
	t.Run("at the bound the answering root now delivers every source", func(t *testing.T) {
		n := inTreeAnswererBound
		gotM, gotC := probe(t, "intree-at-bound", "a", n, n)
		if gotM != n {
			t.Errorf("m receives %d of %d forwarded sources from the ANSWERING root a, want %d — "+
				"the layer work's receive slots are what make this many fit, so a drop here means "+
				"that accident has been undone and the stranding got WORSE", gotM, n, n)
		}
		if gotC != n {
			t.Errorf("c receives %d of %d sources behind m, want %d — whatever reaches m must "+
				"reach the leaf behind it, since m offers on that edge and can add m-lines freely",
				gotC, n, n)
		}
	})

	t.Run("above the bound the answering root still strands sources", func(t *testing.T) {
		n := inTreeAnswererBound + 2
		gotM, gotC := probe(t, "intree-above-bound", "a", n, inTreeAnswererBound)
		if gotM != inTreeAnswererBound {
			t.Errorf("m receives %d of %d forwarded sources from the ANSWERING root a, want the "+
				"still-defective %d — if this is now %d the in-tree offerer rule has been fixed "+
				"and this whole test should be deleted", gotM, n, inTreeAnswererBound, n)
		}
		if gotC != inTreeAnswererBound {
			t.Errorf("c receives %d of %d sources, want %d — the stranding is supposed to "+
				"propagate one hop down, and a different number here means it is being masked "+
				"somewhere else rather than measured", gotC, n, inTreeAnswererBound)
		}
	})

	// "z" > "m", so the root offers and can create as many forwarded m-lines as it
	// needs. Without this control both subtests above would also pass on a build that
	// forwards nothing at all, and neither would be measuring the offerer rule.
	t.Run("the offering root delivers every source, past the bound", func(t *testing.T) {
		n := inTreeAnswererBound + 2
		gotM, gotC := probe(t, "intree-offering-root", "z", n, n)
		if gotM != n {
			t.Errorf("m receives %d of %d forwarded sources from the OFFERING root z, want %d — "+
				"an offerer has no bound, so this failing means the fixture stopped measuring "+
				"the offerer rule", gotM, n, n)
		}
		if gotC != n {
			t.Errorf("c receives %d of %d sources behind an OFFERING root, want %d", gotC, n, n)
		}
	})
}
