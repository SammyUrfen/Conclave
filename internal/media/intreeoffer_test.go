package media

import (
	"context"
	"testing"
	"time"
)

// TestInTreeAnswererCannotPublishASecondForwardedTrack PINS A KNOWN DEFECT. It
// asserts the WRONG number on purpose, so the defect cannot change under anyone
// silently, and it goes red the day the defect is fixed. Read the failure as "this
// is now correct — delete this test", not as a regression.
//
// The defect. Topology.Offers breaks a relay-vs-relay tie on `self > peer`, so on
// the in-tree edge between root "a" and middle relay "m" the MIDDLE relay wins and
// the ROOT answers. The root owes m two forwarded sources (d and e), and an answerer
// can fold at most the one video m-line m's offer already carries into its answer —
// it has no way to add the second. So m receives 1 of 2, indefinitely, and c
// inherits the loss one hop further down.
//
// It is NOT the backup-edge defect and it is not new: measured identically at
// `main` (686ed7b) and on this branch. It is why the backup-edge media fixture names
// its root "z" and its middle relay "m" — those names make the root win the tiebreak
// and offer, so that fixture measures the backup edge instead of this.
//
// Discrimination. The control half is the same shape with the root renamed so it
// offers; it gets both sources. A fix that made the root re-negotiate as an answerer,
// or that put the in-tree edge in the `invert` bucket on a track-set change rather
// than a role change, flips the first subtest and leaves the second green.
func TestInTreeAnswererCannotPublishASecondForwardedTrack(t *testing.T) {
	// probe runs one four-deep shape — root → middle relay → leaf, with two
	// publishers on the root — and reports how many forwarded sources reach the
	// middle relay over its edge to the root.
	probe := func(t *testing.T, room, root string) int {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()

		f := newMeetFixture(t, ctx, room, map[string]RouterConfig{
			root: {},
			"m":  {},
			"d":  {SendMedia: true},
			"e":  {SendMedia: true},
			"c":  {SendMedia: true},
		})
		f.announce(t, 1)
		f.push(t, tree(root,
			[2]string{root, "m"}, [2]string{root, "d"}, [2]string{root, "e"},
			[2]string{"m", "c"}))

		waitFor(t, "the tree to converge", 60*time.Second, func() bool {
			return allConnected(f.routers[root].Stats(), 3) &&
				allConnected(f.routers["m"].Stats(), 2) &&
				allConnected(f.routers["c"].Stats(), 1)
		})
		rootID := f.idOf(t, root)
		// At least one source has to arrive before the count means anything; the
		// question is whether the SECOND one ever follows, so settle past the point
		// where a renegotiation would have landed.
		waitFor(t, "the first forwarded source to reach m", 60*time.Second, func() bool {
			return f.routers["m"].Stats().Tracks[rootID] >= 1
		})
		stableFor(t, "m's forwarded source count to settle", 5*time.Second, func() bool {
			return f.routers["m"].Stats().Tracks[rootID] >= 1
		})
		t.Logf("%s offers toward m = %v; m offers toward %s = %v",
			root, f.offererToward(t, root, "m"), root, f.offererToward(t, "m", root))
		return f.routers["m"].Stats().Tracks[rootID]
	}

	t.Run("the answering root delivers only one of two sources", func(t *testing.T) {
		// "a" < "m", so m offers and a answers.
		if got := probe(t, "intree-answering-root", "a"); got != 1 {
			t.Errorf("m receives %d of 2 forwarded sources from the ANSWERING root a, "+
				"want the known-defective 1 — if this is now 2 the in-tree offerer rule "+
				"has been fixed and this test should be deleted", got)
		}
	})

	t.Run("the offering root delivers both", func(t *testing.T) {
		// "z" > "m", so the root offers and can add both forwarded m-lines.
		if got := probe(t, "intree-offering-root", "z"); got != 2 {
			t.Errorf("m receives %d of 2 forwarded sources from the OFFERING root z, want 2 — "+
				"without this control the subtest above would also pass on a build that "+
				"forwards nothing at all", got)
		}
	})
}
