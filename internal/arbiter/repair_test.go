package arbiter_test

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/SammyUrfen/conclave/internal/arbiter"
)

// repairGrace is metrics.RebuildWindow, restated as a literal because the arbiter must
// not export its own copy. If the two ever disagree these tests fail, which is the
// point of asserting the boundary at 2 s and 3 s rather than "eventually".
const repairGrace = 3 * time.Second

// TestAnnouncementRepair covers the ruling that closes the lost-announcement hole: a
// peer whose fence lags is repaired by a UNICAST replay of the current announcement,
// indefinitely, without a timer and without a membership change.
func TestAnnouncementRepair(t *testing.T) {
	// setup elects alice, then leaves bob permanently behind at epoch 0 — the state a
	// dropped announcement produces.
	setup := func(t *testing.T) *harness {
		t.Helper()
		h := newHarness(t, electing())
		h.join("m", "p1", strong("alice"))
		h.join("m", "p2", mid("bob"))
		h.wantCoord("m", "alice", 1)
		return h
	}

	t.Run("no repair inside the propagation window", func(t *testing.T) {
		h := setup(t)
		// A normal handover leaves every peer briefly behind. Repairing there would
		// fire on every legitimate election.
		h.clk.Advance(repairGrace - time.Second)
		h.beat("m", "p2")

		if got := h.ann.allRepairs(); len(got) != 0 {
			t.Fatalf("repairs = %d, want 0 inside RebuildWindow", len(got))
		}
		if got := h.pub.allRepairs(); len(got) != 0 {
			t.Fatalf("repair events = %d, want 0 inside RebuildWindow", len(got))
		}
	})

	t.Run("a lagging peer is repaired once the window has passed", func(t *testing.T) {
		h := setup(t)
		h.clk.Advance(repairGrace)
		h.beat("m", "p2")

		got := h.ann.repairsTo("p2")
		if len(got) != 1 {
			t.Fatalf("repairs to p2 = %d, want 1", len(got))
		}
		// Verbatim matters: the peer must adopt exactly the announcement it missed,
		// or two peers end up disagreeing about WHY the incumbent holds the role.
		if !reflect.DeepEqual(got[0], h.ann.forRoom("m")[0]) {
			t.Errorf("repair = %+v, want the original announcement verbatim %+v",
				got[0], h.ann.forRoom("m")[0])
		}
	})

	t.Run("the repair is unicast, never broadcast", func(t *testing.T) {
		h := setup(t)
		before := len(h.ann.forRoom("m"))
		h.clk.Advance(repairGrace)
		for i := 0; i < 5; i++ {
			h.stepLagging("m", "p2")
		}

		if got := len(h.ann.forRoom("m")); got != before {
			t.Errorf("broadcasts = %d, want %d — one wedged peer must not cost N frames", got, before)
		}
		for _, s := range h.ann.allRepairs() {
			if s.peerID != "p2" {
				t.Errorf("repair addressed to %q, want only the lagging peer p2", s.peerID)
			}
		}
	})

	t.Run("repair continues indefinitely with no cap or backoff", func(t *testing.T) {
		h := setup(t)
		h.clk.Advance(repairGrace)
		for i := 0; i < 30; i++ {
			h.stepLagging("m", "p2")
		}

		// One per lagging beat past the window; the condition is indefinite, so the
		// repair is too.
		if got := len(h.ann.repairsTo("p2")); got != 30 {
			t.Errorf("repairs = %d, want 30 — one per lagging beat, no cap, no backoff", got)
		}
	})

	t.Run("a caught-up peer is never repaired", func(t *testing.T) {
		h := setup(t)
		h.clk.Advance(repairGrace)
		h.elapseCurrent("m", 5*time.Second)

		if got := h.ann.allRepairs(); len(got) != 0 {
			t.Fatalf("repairs = %d, want 0 for a peer at the current epoch", len(got))
		}
	})

	t.Run("a broken repair send does not fault the arbiter", func(t *testing.T) {
		h := setup(t)
		h.ann.setFail(errors.New("socket wedged"))
		h.clk.Advance(repairGrace)
		h.beat("m", "p2")
		h.wantCoord("m", "alice", 1)
	})
}

// TestRepairEventsAreTransitionsOnly is the discrimination test for the dashboard half
// of the ruling: 1 Hz of wire repair must produce TWO events per episode, not one per
// beat. An implementation that published per beat passes every test above and fails
// this one.
func TestRepairEventsAreTransitionsOnly(t *testing.T) {
	h := newHarness(t, electing())
	h.join("m", "p1", strong("alice"))
	h.join("m", "p2", mid("bob"))
	h.clk.Advance(repairGrace)

	// Ten lagging beats.
	for i := 0; i < 10; i++ {
		h.stepLagging("m", "p2")
	}
	if got := h.pub.allRepairs(); len(got) != 1 {
		t.Fatalf("repair events after 10 lagging beats = %d, want 1 (entry only)", len(got))
	}
	if got := len(h.ann.repairsTo("p2")); got != 10 {
		t.Fatalf("wire repairs = %d, want 10 — the WIRE repeats even though the LOG does not", got)
	}

	// bob finally adopts, and keeps beating.
	h.elapseCurrent("m", 5*time.Second)

	got := h.pub.allRepairs()
	if len(got) != 2 {
		t.Fatalf("repair events = %d, want 2 (entering and leaving the lagging state)", len(got))
	}
	if got[0].resolved || !got[1].resolved {
		t.Errorf("resolved flags = %v/%v, want false then true", got[0].resolved, got[1].resolved)
	}
	if got[0].name != "bob" || got[0].roomID != "m" {
		t.Errorf("event = %+v, want bob in meet m", got[0])
	}
	if got[0].meetEpoch != 1 || got[0].peerEpoch != 0 {
		t.Errorf("event = %+v, want peer_epoch 0 behind meet_epoch 1", got[0])
	}
}

// TestRepairIsNotAnElection: the election log's value is that it lists real leadership
// changes. A repair reaching PublishElection would drown exactly the log the Phase 6
// demo points at.
func TestRepairIsNotAnElection(t *testing.T) {
	h := newHarness(t, electing())
	h.join("m", "p1", strong("alice"))
	h.join("m", "p2", mid("bob"))
	h.clk.Advance(repairGrace)
	for i := 0; i < 10; i++ {
		h.stepLagging("m", "p2")
	}

	if got := h.pub.all(); len(got) != 1 {
		t.Errorf("published elections = %d, want 1 (the bootstrap only)", len(got))
	}
}

// TestDemotedCoordinatorIsRepaired is ruling 1b: the peer that used to coordinate and
// missed its own demotion is reached by the same mechanism, with no new frame.
func TestDemotedCoordinatorIsRepaired(t *testing.T) {
	h := newHarness(t, electing())
	h.join("m", "p1", mid("alice"))
	h.join("m", "p2", strong("bob"))
	h.elapseCurrent("m", 61*time.Second)
	h.wantCoord("m", "bob", 2)

	// alice never processed the handover: she still believes she holds epoch 1 and is
	// still pushing under it.
	h.clk.Advance(repairGrace)
	h.beatWith("m", "p1", 1, 9)

	got := h.ann.repairsTo("p1")
	if len(got) != 1 {
		t.Fatalf("repairs to the demoted coordinator = %d, want 1", len(got))
	}
	if got[0].Epoch != 2 || got[0].CoordinatorID != "p2" {
		t.Errorf("repair = %+v, want the current term (epoch 2, coordinator p2)", got[0])
	}
}

// TestVacancyRepairsAStaleCoordinator ties the two rulings together: a vacancy is the
// only announcement that can tell a live coordinator to stop, and the repair path is
// what delivers it to a peer that missed the broadcast.
func TestVacancyRepairsAStaleCoordinator(t *testing.T) {
	h := newHarness(t, electing())
	h.join("m", "p1", strong("alice"))
	h.join("m", "p2", unwilling("bob"))
	h.leave("m", "p1")

	// alice rejoins still believing she holds epoch 1 (a rejoin after a partition).
	h.join("m", "p3", unwilling("alice2"))
	h.clk.Advance(repairGrace)
	h.beatWith("m", "p3", 1, 4)

	got := h.ann.repairsTo("p3")
	if len(got) != 1 {
		t.Fatalf("repairs = %d, want 1", len(got))
	}
	if got[0].Reason != arbiter.ReasonVacated || got[0].Coordinator != "" {
		t.Errorf("repair = %+v, want the retained vacancy", got[0])
	}
}
