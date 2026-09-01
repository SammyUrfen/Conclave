package arbiter_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/SammyUrfen/conclave/internal/arbiter"
	"github.com/SammyUrfen/conclave/internal/overlay"
)

// The four telemetry archetypes every scenario is built from. The numbers are
// chosen so the score bands they land in are far apart enough to make an assertion
// unambiguous, and so that "modest" sits BELOW the incumbent floor while being less
// than PromoteMarginScore away from "floor" — which is what lets the demotion path
// be tested in isolation from the promotion path.
//
//	strong ≈ 0.85 + uptime      (headroom everywhere)
//	mid    ≈ 0.425 + uptime     (a decent home link)
//	modest ≈ 0.36 + uptime      (mediocre, still above the floor at zero uptime)
//	floor  ≈ 0.17 + uptime      (below DemoteBelowScore at any uptime)
func strong(name string) peerOpts {
	return peerOpts{name: name, uploadKbps: 5000, coordinatable: true}
}

func mid(name string) peerOpts {
	return peerOpts{name: name, cpuPct: 50, rttMs: 150, lossPct: 5, uploadKbps: 5000, coordinatable: true}
}

func modest(name string) peerOpts {
	return peerOpts{name: name, cpuPct: 60, rttMs: 180, lossPct: 5, uploadKbps: 5000, coordinatable: true}
}

func floorPeer(name string) peerOpts {
	return peerOpts{name: name, cpuPct: 80, rttMs: 240, lossPct: 8, uploadKbps: 5000, coordinatable: true}
}

func unwilling(name string) peerOpts {
	o := strong(name)
	o.coordinatable = false
	return o
}

func turnBound(name string) peerOpts {
	o := strong(name)
	o.nat = overlay.NATRelayed
	return o
}

// TestBootstrap covers the first row of the PLAN §6.2 trigger table.
func TestBootstrap(t *testing.T) {
	t.Run("first eligible peer is elected immediately at epoch 1", func(t *testing.T) {
		h := newHarness(t, electing())
		h.join("m", "p1", strong("alice"))

		h.wantCoord("m", "alice", 1)
		got := h.ann.forRoom("m")
		if len(got) != 1 {
			t.Fatalf("announcements = %d, want 1", len(got))
		}
		if got[0].Reason != arbiter.ReasonBootstrap {
			t.Errorf("reason = %q, want %q", got[0].Reason, arbiter.ReasonBootstrap)
		}
		if got[0].CoordinatorID != "p1" || got[0].Prev != "" {
			t.Errorf("announcement = %+v, want CoordinatorID p1 and no Prev", got[0])
		}
		if got[0].RoomID != "m" {
			t.Errorf("RoomID = %q, want m", got[0].RoomID)
		}
		if !got[0].IssuedAt.Equal(epoch0) {
			t.Errorf("IssuedAt = %v, want %v (the injected clock)", got[0].IssuedAt, epoch0)
		}
	})

	t.Run("a peer that never reported is never elected", func(t *testing.T) {
		h := newHarness(t, electing())
		h.joinSilent("m", "p1", "alice")
		h.elapse("m", 5*time.Second)

		h.wantCoord("m", "", 0)
		if got := h.ann.forRoom("m"); len(got) != 0 {
			t.Fatalf("announcements = %d, want 0 for a peer that never declared coordinatable", len(got))
		}
	})

	t.Run("no coordinatable peer means no coordinator and no announcement", func(t *testing.T) {
		h := newHarness(t, electing())
		h.join("m", "p1", unwilling("alice"))
		h.join("m", "p2", unwilling("bob"))
		h.elapse("m", 30*time.Second)

		h.wantCoord("m", "", 0)
		if got := h.ann.forRoom("m"); len(got) != 0 {
			t.Fatalf("announcements = %d, want 0 when nobody is coordinatable", len(got))
		}
		if m := h.meet("m"); m.Members != 2 {
			t.Errorf("Members = %d, want 2 — the meet still exists, it just has no coordinator", m.Members)
		}
	})

	t.Run("a TURN-bound peer is skipped in favour of a direct one", func(t *testing.T) {
		h := newHarness(t, electing())
		h.join("m", "p1", turnBound("alice"))
		h.join("m", "p2", strong("bob"))

		h.wantCoord("m", "bob", 1)
	})

	t.Run("the arbiter takes the role itself when Coordinate is set", func(t *testing.T) {
		cfg := electing()
		cfg.Coordinate = true
		h := newHarness(t, cfg)
		h.join("m", "p1", strong("alice"))

		m := h.meet("m")
		if !m.ArbiterIsCoord {
			t.Fatalf("ArbiterIsCoord = false, want true at bootstrap with -coordinate")
		}
		if m.Coordinator != "" || m.CoordinatorID != arbiter.DefaultArbiterID {
			t.Errorf("meet = %+v, want empty Coordinator name and CoordinatorID %q",
				m, arbiter.DefaultArbiterID)
		}
	})
}

// TestAnnounceOnJoin is the regression test for the defect that silently blackholed
// the most ordinary case in the system: a peer joining a meet that already has a
// coordinator was never told who it was, so its fence rejected every topology push
// forever while the dashboard rendered it healthy (PLAN §8 routing rule 5).
func TestAnnounceOnJoin(t *testing.T) {
	t.Run("a joiner is announced to, at the UNCHANGED epoch", func(t *testing.T) {
		h := newHarness(t, electing())
		h.join("m", "p1", strong("alice"))
		before := len(h.ann.forRoom("m"))

		h.join("m", "p2", mid("bob"))

		got := h.ann.forRoom("m")
		if len(got) != before+1 {
			t.Fatalf("announcements = %d, want %d — a join must re-announce", len(got), before+1)
		}
		last := got[len(got)-1]
		if last.Epoch != 1 {
			t.Errorf("re-announced epoch = %d, want 1 unchanged: a join is not an election", last.Epoch)
		}
		if last.Coordinator != "alice" || last.CoordinatorID != "p1" {
			t.Errorf("re-announcement = %+v, want the sitting coordinator alice/p1", last)
		}
	})

	t.Run("a re-announcement is byte-identical to the original", func(t *testing.T) {
		h := newHarness(t, electing())
		h.join("m", "p1", strong("alice"))
		first := h.ann.forRoom("m")[0]
		h.join("m", "p2", mid("bob"))
		second := h.lastAnn("m")

		if !reflect.DeepEqual(first, second) {
			t.Errorf("re-announcement = %+v, want it identical to %+v so it is idempotent at the peer",
				second, first)
		}
	})

	t.Run("a leave re-announces too", func(t *testing.T) {
		h := newHarness(t, electing())
		h.join("m", "p1", strong("alice"))
		h.join("m", "p2", mid("bob"))
		h.join("m", "p3", mid("carol"))
		before := len(h.ann.forRoom("m"))

		h.leave("m", "p3") // a non-coordinator leaving is not an election

		got := h.ann.forRoom("m")
		if len(got) != before+1 {
			t.Fatalf("announcements = %d, want %d — a leave must re-announce", len(got), before+1)
		}
		if got[len(got)-1].Epoch != 1 {
			t.Errorf("epoch = %d, want 1: a leaf leaving is not an election", got[len(got)-1].Epoch)
		}
	})

	t.Run("a re-announcement is not published as an election", func(t *testing.T) {
		h := newHarness(t, electing())
		h.join("m", "p1", strong("alice"))
		h.join("m", "p2", mid("bob"))
		h.join("m", "p3", mid("carol"))

		if got := h.pub.all(); len(got) != 1 {
			t.Fatalf("published elections = %d, want 1 (the bootstrap only)", len(got))
		}
	})

	t.Run("a peer partitioned across several epochs is repaired when it rejoins", func(t *testing.T) {
		h := newHarness(t, electing())
		h.join("m", "p1", mid("alice"))
		h.join("m", "p2", strong("bob"))
		h.join("m", "p3", mid("carol"))

		// carol goes quiet: alice and bob run an election without her.
		h.elapseOnly("m", 61*time.Second, "p1", "p2")
		h.wantCoord("m", "bob", 2)

		// Her socket finally closes and she rejoins. She must be told the
		// current authority or she rejects every push forever.
		h.leave("m", "p3")
		h.join("m", "p3", mid("carol"))

		last := h.lastAnn("m")
		if last.Epoch != 2 || last.CoordinatorID != "p2" {
			t.Errorf("announcement after rejoin = %+v, want epoch 2 coordinator p2", last)
		}
	})
}

// TestFailover covers the "Failure" row: immediate, no dwell, no MinTerm.
func TestFailover(t *testing.T) {
	t.Run("a closed socket fails the coordinator over immediately", func(t *testing.T) {
		h := newHarness(t, electing())
		h.join("m", "p1", strong("alice"))
		h.join("m", "p2", mid("bob"))

		h.leave("m", "p1")

		h.wantCoord("m", "bob", 2)
		last := h.lastAnn("m")
		if last.Reason != arbiter.ReasonFailover {
			t.Errorf("reason = %q, want %q", last.Reason, arbiter.ReasonFailover)
		}
		if last.Prev != "alice" {
			t.Errorf("Prev = %q, want alice", last.Prev)
		}
	})

	t.Run("MinTermDuration does not block a failure", func(t *testing.T) {
		h := newHarness(t, electing())
		h.join("m", "p1", strong("alice"))
		h.join("m", "p2", mid("bob"))
		h.elapse("m", 2*time.Second) // far inside MinTermDuration

		h.leave("m", "p1")

		h.wantCoord("m", "bob", 2)
	})

	t.Run("a silent coordinator is failed over by the arbiter's OWN liveness view", func(t *testing.T) {
		h := newHarness(t, electing())
		h.join("m", "p1", strong("alice"))
		h.join("m", "p2", mid("bob"))
		h.wantCoord("m", "alice", 1)

		// alice's socket never closes and nothing else reports her death: her
		// own health FSM would have died with her. Only bob keeps beating.
		h.elapseOnly("m", 12*time.Second, "p2")

		h.wantCoord("m", "bob", 2)
		if last := h.lastAnn("m"); last.Reason != arbiter.ReasonFailover {
			t.Errorf("reason = %q, want %q", last.Reason, arbiter.ReasonFailover)
		}
	})

	t.Run("a coordinator that resumes beating does not take the role back", func(t *testing.T) {
		h := newHarness(t, electing())
		h.join("m", "p1", strong("alice"))
		h.join("m", "p2", mid("bob"))
		h.elapseOnly("m", 12*time.Second, "p2")
		h.wantCoord("m", "bob", 2)

		h.elapse("m", 10*time.Second) // alice is beating again, and is fitter

		h.wantCoord("m", "bob", 2) // MinTermDuration still applies to a voluntary move
	})

	t.Run("a failure with no eligible replacement ANNOUNCES the vacancy at a bumped epoch", func(t *testing.T) {
		h := newHarness(t, electing())
		h.join("m", "p1", strong("alice"))
		h.join("m", "p2", unwilling("bob"))

		h.leave("m", "p1")

		m := h.meet("m")
		if m.Coordinator != "" || m.CoordinatorID != "" || m.ArbiterIsCoord {
			t.Fatalf("meet = %+v, want a vacancy", m)
		}
		// Going silent is NOT enough: silence cannot tell a live-but-demoted
		// coordinator to stop, and it fences a joiner to nothing rather than to the
		// true state. Only a higher epoch naming nobody does both.
		if m.Epoch != 2 {
			t.Fatalf("Epoch = %d, want 2 — the vacancy is fenced by a BUMPED epoch", m.Epoch)
		}
		last := h.lastAnn("m")
		if last.Reason != arbiter.ReasonVacated {
			t.Fatalf("reason = %q, want %q", last.Reason, arbiter.ReasonVacated)
		}
		if last.Epoch != 2 || last.Coordinator != "" || last.CoordinatorID != "" {
			t.Errorf("announcement = %+v, want epoch 2 naming nobody", last)
		}
		if last.Prev != "alice" {
			t.Errorf("Prev = %q, want alice", last.Prev)
		}
	})

	t.Run("the vacancy is retained and replayed to a joiner", func(t *testing.T) {
		h := newHarness(t, electing())
		h.join("m", "p1", strong("alice"))
		h.join("m", "p2", unwilling("bob"))
		h.leave("m", "p1")
		before := len(h.ann.forRoom("m"))

		h.join("m", "p4", unwilling("dan"))

		got := h.ann.forRoom("m")
		if len(got) != before+1 {
			t.Fatalf("announcements = %d, want %d — the vacancy must be replayed", len(got), before+1)
		}
		last := got[len(got)-1]
		if last.Reason != arbiter.ReasonVacated || last.Epoch != 2 {
			t.Errorf("replayed = %+v, want the retained vacancy at epoch 2", last)
		}
	})

	t.Run("a vacancy is announced once, not once per membership change", func(t *testing.T) {
		h := newHarness(t, electing())
		h.join("m", "p1", strong("alice"))
		h.join("m", "p2", unwilling("bob"))
		h.leave("m", "p1")
		h.join("m", "p4", unwilling("dan"))
		h.leave("m", "p4")

		if got := h.meet("m").Epoch; got != 2 {
			t.Errorf("Epoch = %d, want 2 — re-entering a vacancy already announced burns no epoch", got)
		}
	})

	t.Run("an eligible peer appearing later takes the vacant role", func(t *testing.T) {
		h := newHarness(t, electing())
		h.join("m", "p1", strong("alice"))
		h.join("m", "p2", unwilling("bob"))
		h.leave("m", "p1")

		h.join("m", "p3", mid("carol"))

		h.wantCoord("m", "carol", 3) // 1 bootstrap, 2 vacancy, 3 the new term
		if last := h.lastAnn("m"); last.Reason != arbiter.ReasonBootstrap {
			t.Errorf("reason = %q, want %q — the meet had no coordinator to fail over from",
				last.Reason, arbiter.ReasonBootstrap)
		}
	})

	t.Run("the last member leaving clears the coordinator", func(t *testing.T) {
		h := newHarness(t, electing())
		h.join("m", "p1", strong("alice"))
		h.leave("m", "p1")

		m := h.meet("m")
		if m.Coordinator != "" || m.Members != 0 {
			t.Fatalf("meet = %+v, want an empty, uncoordinated meet", m)
		}
		if got := h.ann.forRoom("m"); len(got) != 1 {
			t.Errorf("announcements = %d, want 1 — there is nobody left to announce to", len(got))
		}
	})
}

// TestPromotion covers the "Promotion" row and both of its brakes.
func TestPromotion(t *testing.T) {
	t.Run("a materially fitter challenger takes over after dwell and MinTerm", func(t *testing.T) {
		h := newHarness(t, electing())
		h.join("m", "p1", mid("alice"))
		h.join("m", "p2", strong("bob"))

		h.elapse("m", 61*time.Second)

		h.wantCoord("m", "bob", 2)
		last := h.lastAnn("m")
		if last.Reason != arbiter.ReasonPromotion {
			t.Errorf("reason = %q, want %q", last.Reason, arbiter.ReasonPromotion)
		}
		if last.Prev != "alice" {
			t.Errorf("Prev = %q, want alice", last.Prev)
		}
	})

	t.Run("MinTermDuration blocks a handover the dwell has already cleared", func(t *testing.T) {
		h := newHarness(t, electing())
		h.join("m", "p1", mid("alice"))
		h.join("m", "p2", strong("bob"))

		h.elapse("m", 30*time.Second) // past ElectionDwell, inside MinTermDuration

		h.wantCoord("m", "alice", 1)
	})

	t.Run("ElectionDwell blocks a handover MinTerm has already cleared", func(t *testing.T) {
		cfg := electing()
		cfg.MinTerm = -1 // disabled, isolating the dwell
		h := newHarness(t, cfg)
		h.join("m", "p1", mid("alice"))
		h.join("m", "p2", strong("bob"))

		h.elapse("m", 19*time.Second)
		h.wantCoord("m", "alice", 1)

		h.elapse("m", 2*time.Second)
		h.wantCoord("m", "bob", 2)
	})

	t.Run("a lapsing condition re-arms the dwell", func(t *testing.T) {
		cfg := electing()
		cfg.MinTerm = -1
		h := newHarness(t, cfg)
		h.join("m", "p1", mid("alice"))
		h.join("m", "p2", strong("bob"))

		h.elapse("m", 15*time.Second)
		// bob's link degrades: the margin vanishes and the dwell must restart.
		h.report("m", "p2", floorPeer("bob"))
		h.elapse("m", 10*time.Second)
		h.report("m", "p2", strong("bob"))
		h.elapse("m", 15*time.Second)
		h.wantCoord("m", "alice", 1)

		h.elapse("m", 6*time.Second)
		h.wantCoord("m", "bob", 2)
	})

	t.Run("a marginal improvement never moves the role", func(t *testing.T) {
		h := newHarness(t, electing())
		h.join("m", "p1", modest("alice"))
		h.join("m", "p2", mid("bob")) // ~0.09 fitter: inside PromoteMarginScore

		h.elapse("m", 5*time.Minute)

		h.wantCoord("m", "alice", 1)
	})
}

// TestDemotion covers the "Demotion" row: an absolute floor, not a margin.
func TestDemotion(t *testing.T) {
	t.Run("an incumbent below the floor is replaced without the promotion margin", func(t *testing.T) {
		h := newHarness(t, electing())
		h.join("m", "p1", floorPeer("alice"))
		h.join("m", "p2", modest("bob")) // fitter, but by less than PromoteMarginScore

		h.elapse("m", 61*time.Second)

		h.wantCoord("m", "bob", 2)
		if last := h.lastAnn("m"); last.Reason != arbiter.ReasonDemotion {
			t.Errorf("reason = %q, want %q", last.Reason, arbiter.ReasonDemotion)
		}
	})

	t.Run("an incumbent below the floor with no replacement keeps the role", func(t *testing.T) {
		h := newHarness(t, electing())
		h.join("m", "p1", floorPeer("alice"))
		h.join("m", "p2", unwilling("bob"))

		h.elapse("m", 3*time.Minute)

		h.wantCoord("m", "alice", 1) // a bad coordinator beats no coordinator
	})

	t.Run("demotion respects ElectionDwell", func(t *testing.T) {
		cfg := electing()
		cfg.MinTerm = -1
		h := newHarness(t, cfg)
		h.join("m", "p1", floorPeer("alice"))
		h.join("m", "p2", modest("bob"))

		h.elapse("m", 19*time.Second)
		h.wantCoord("m", "alice", 1)
		h.elapse("m", 2*time.Second)
		h.wantCoord("m", "bob", 2)
	})
}

// TestDemotedThenRepromoted is the adversarial round trip: a peer loses the role on
// the floor rule and wins it back later. It is where an epoch counter that resets,
// or a dwell that is not cleared on a transition, shows up.
func TestDemotedThenRepromoted(t *testing.T) {
	h := newHarness(t, electing())
	h.join("m", "p1", mid("alice"))
	h.join("m", "p2", strong("bob"))

	h.elapse("m", 61*time.Second)
	h.wantCoord("m", "bob", 2)

	// bob's machine falls apart.
	h.report("m", "p2", floorPeer("bob"))
	h.elapse("m", 61*time.Second)
	h.wantCoord("m", "alice", 3)

	// bob recovers and is materially fitter again.
	h.report("m", "p2", strong("bob"))
	h.elapse("m", 61*time.Second)
	h.wantCoord("m", "bob", 4)

	assertEpochsUniqueAndIncreasing(t, h, "m")
	reasons := []arbiter.Reason{}
	for _, a := range h.ann.forRoom("m") {
		reasons = append(reasons, a.Reason)
	}
	want := []arbiter.Reason{
		arbiter.ReasonBootstrap, arbiter.ReasonBootstrap, // bootstrap + re-announce on bob's join
		arbiter.ReasonPromotion, arbiter.ReasonDemotion, arbiter.ReasonPromotion,
	}
	if !reflect.DeepEqual(reasons, want) {
		t.Errorf("reasons = %v, want %v", reasons, want)
	}
}

// TestEpochDiscipline pins PLAN §6.4: the arbiter is the single writer, and it never
// issues an epoch twice for one meet.
func TestEpochDiscipline(t *testing.T) {
	t.Run("epochs are per meet and never leak across them", func(t *testing.T) {
		h := newHarness(t, electing())
		h.join("alpha", "a1", strong("alice"))
		h.join("beta", "b1", strong("bob"))
		h.join("beta", "b2", mid("carol"))

		// Three elections in beta must not touch alpha's counter.
		h.leave("beta", "b1")
		h.join("beta", "b3", strong("dave"))
		h.leave("beta", "b2")

		h.wantCoord("alpha", "alice", 1)
		for _, a := range h.ann.forRoom("alpha") {
			if a.RoomID != "alpha" {
				t.Errorf("announcement addressed to %q on alpha's stream", a.RoomID)
			}
		}
		if got := h.meet("beta").Epoch; got <= 1 {
			t.Fatalf("beta epoch = %d, want > 1 after three elections", got)
		}
		assertEpochsUniqueAndIncreasing(t, h, "alpha")
		assertEpochsUniqueAndIncreasing(t, h, "beta")
	})

	t.Run("a failed broadcast still consumes its epoch", func(t *testing.T) {
		h := newHarness(t, electing())
		h.ann.setFail(errors.New("every socket is wedged"))
		h.join("m", "p1", strong("alice"))
		h.wantCoord("m", "alice", 1)

		// The broadcast failed, but rolling the counter back would let a second
		// coordinator hold epoch 1 later — the one thing the fence forbids.
		h.ann.setFail(nil)
		h.join("m", "p2", mid("bob"))

		last := h.lastAnn("m")
		if last.Epoch != 1 || last.CoordinatorID != "p1" {
			t.Fatalf("repair announcement = %+v, want epoch 1 coordinator p1", last)
		}
		h.leave("m", "p1")
		if got := h.meet("m").Epoch; got != 2 {
			t.Errorf("epoch after failover = %d, want 2 — 1 was consumed and may never be reissued", got)
		}
	})

	t.Run("a stale heartbeat from a demoted coordinator changes nothing", func(t *testing.T) {
		h := newHarness(t, electing())
		h.join("m", "p1", mid("alice"))
		h.join("m", "p2", strong("bob"))
		h.elapse("m", 61*time.Second)
		h.wantCoord("m", "bob", 2)
		before := len(h.ann.forRoom("m"))

		// alice's control loop has not noticed the handover yet and is still
		// beating (and pushing) under epoch 1.
		h.beatWith("m", "p1", 1, 7)

		h.wantCoord("m", "bob", 2)
		if got := len(h.ann.forRoom("m")); got != before {
			t.Errorf("announcements = %d, want %d: a stale actor must not provoke one", got, before)
		}
	})
}

// assertEpochsUniqueAndIncreasing is the ambiguity-window guarantee from PLAN §6.7
// expressed as a property: two coordinators can never hold the same epoch because
// the arbiter never issues one twice, and never issues them out of order.
func assertEpochsUniqueAndIncreasing(t *testing.T, h *harness, roomID string) {
	t.Helper()
	seen := map[uint64]string{}
	var prev uint64
	for i, a := range h.ann.forRoom(roomID) {
		if a.Epoch < prev {
			t.Fatalf("announcement %d: epoch %d went backwards from %d", i, a.Epoch, prev)
		}
		if holder, ok := seen[a.Epoch]; ok && holder != a.CoordinatorID {
			t.Fatalf("epoch %d issued to both %q and %q", a.Epoch, holder, a.CoordinatorID)
		}
		seen[a.Epoch] = a.CoordinatorID
		prev = a.Epoch
	}
}

// TestReorderedAnnouncements checks the arbiter's half of the fence contract: every
// announcement it issues carries a strictly greater epoch than the last, so a peer
// applying the PLAN §6.5 rule converges on the newest one no matter what order the
// network delivers them in.
func TestReorderedAnnouncements(t *testing.T) {
	h := newHarness(t, electing())
	h.join("m", "p1", mid("alice"))
	h.join("m", "p2", strong("bob"))
	h.elapse("m", 61*time.Second)
	h.leave("m", "p2")

	elections := []arbiter.Announcement{}
	for _, a := range h.ann.forRoom("m") {
		if len(elections) == 0 || a.Epoch > elections[len(elections)-1].Epoch {
			elections = append(elections, a)
		}
	}
	if len(elections) < 3 {
		t.Fatalf("distinct epochs = %d, want at least 3", len(elections))
	}
	newest := elections[len(elections)-1]

	// fence is the peer-side rule from PLAN §6.5, modelled here so the test asserts
	// against the contract rather than against a future peer implementation.
	type fence struct {
		curEpoch uint64
		coordID  string
	}
	for _, order := range [][]int{{0, 1, 2}, {2, 1, 0}, {1, 2, 0}, {2, 0, 1}} {
		f := fence{}
		for _, i := range order {
			if a := elections[i]; a.Epoch > f.curEpoch {
				f.curEpoch, f.coordID = a.Epoch, a.CoordinatorID
			}
		}
		if f.curEpoch != newest.Epoch || f.coordID != newest.CoordinatorID {
			t.Errorf("delivery order %v converged on epoch %d/%q, want %d/%q",
				order, f.curEpoch, f.coordID, newest.Epoch, newest.CoordinatorID)
		}
	}
}

// TestTiesBreakDeterministically pins the tiebreak so a fitness tie cannot make the
// election depend on map iteration or join order.
func TestTiesBreakDeterministically(t *testing.T) {
	// Both challengers are byte-identical in telemetry AND join at the same virtual
	// instant, so their UptimeSec is equal too — a genuine tie, not a near one.
	run := func(t *testing.T, first, second string) string {
		t.Helper()
		h := newHarness(t, electing())
		h.join("m", "p0", floorPeer("incumbent"))
		h.join("m", "p1", strong(first))
		h.join("m", "p2", strong(second))
		h.elapse("m", 61*time.Second)
		return h.meet("m").Coordinator
	}

	t.Run("lowest name wins", func(t *testing.T) {
		if got := run(t, "alice", "bob"); got != "alice" {
			t.Errorf("coordinator = %q, want alice (ties break by name ascending)", got)
		}
	})

	t.Run("join order does not decide a tie", func(t *testing.T) {
		if got := run(t, "bob", "alice"); got != "alice" {
			t.Errorf("coordinator = %q, want alice regardless of join order", got)
		}
	})

	t.Run("repeated runs agree", func(t *testing.T) {
		for i := 0; i < 20; i++ {
			if got := run(t, "alice", "bob"); got != "alice" {
				t.Fatalf("run %d: coordinator = %q, want alice", i, got)
			}
		}
	})
}

// TestForceElection covers the demo surface (PLAN §9.6): it bypasses both brakes.
func TestForceElection(t *testing.T) {
	force := func(t *testing.T, h *harness, roomID, name string) error {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), syncWait)
		defer cancel()
		return h.a.ForceElection(ctx, roomID, name)
	}

	t.Run("a named peer takes the role immediately", func(t *testing.T) {
		h := newHarness(t, electing())
		h.join("m", "p1", strong("alice"))
		h.join("m", "p2", floorPeer("bob"))

		if err := force(t, h, "m", "bob"); err != nil {
			t.Fatalf("ForceElection: %v", err)
		}
		h.wantCoord("m", "bob", 2)
		if last := h.lastAnn("m"); last.Reason != arbiter.ReasonManual {
			t.Errorf("reason = %q, want %q", last.Reason, arbiter.ReasonManual)
		}
	})

	t.Run("an empty name picks the best candidate", func(t *testing.T) {
		h := newHarness(t, electing())
		h.join("m", "p1", floorPeer("alice"))
		h.join("m", "p2", strong("bob"))

		if err := force(t, h, "m", ""); err != nil {
			t.Fatalf("ForceElection: %v", err)
		}
		h.wantCoord("m", "bob", 2)
	})

	t.Run("an unknown meet is an error", func(t *testing.T) {
		h := newHarness(t, electing())
		if err := force(t, h, "nope", ""); !errors.Is(err, arbiter.ErrMeetNotFound) {
			t.Errorf("err = %v, want ErrMeetNotFound", err)
		}
	})

	t.Run("an unknown name is an error and changes nothing", func(t *testing.T) {
		h := newHarness(t, electing())
		h.join("m", "p1", strong("alice"))
		if err := force(t, h, "m", "ghost"); !errors.Is(err, arbiter.ErrNoCandidate) {
			t.Errorf("err = %v, want ErrNoCandidate", err)
		}
		h.wantCoord("m", "alice", 1)
	})

	t.Run("forcing the sitting coordinator is a no-op, not a new epoch", func(t *testing.T) {
		h := newHarness(t, electing())
		h.join("m", "p1", strong("alice"))
		if err := force(t, h, "m", "alice"); err != nil {
			t.Fatalf("ForceElection: %v", err)
		}
		h.wantCoord("m", "alice", 1)
	})
}

// TestMeetRegistry covers the /api/meets surface (PLAN §9.3) the dashboard consumes.
func TestMeetRegistry(t *testing.T) {
	create := func(t *testing.T, h *harness, id string) (arbiter.Meet, error) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), syncWait)
		defer cancel()
		return h.a.CreateMeet(ctx, id)
	}

	t.Run("meet id validation", func(t *testing.T) {
		cases := []struct {
			id string
			ok bool
		}{
			{"standup", true},
			{"a", true},
			{"a-b_c9", true},
			{strings.Repeat("a", 64), true},
			{strings.Repeat("a", 65), false},
			{"", false},
			{"Standup", false},
			{"-lead", false},
			{"_lead", false},
			{"has space", false},
			{"has/slash", false},
		}
		for _, tc := range cases {
			t.Run(tc.id, func(t *testing.T) {
				h := newHarness(t, electing())
				_, err := create(t, h, tc.id)
				if tc.ok && err != nil {
					t.Fatalf("CreateMeet(%q) = %v, want success", tc.id, err)
				}
				if !tc.ok && tc.id != "" && !errors.Is(err, arbiter.ErrInvalidMeetID) {
					t.Fatalf("CreateMeet(%q) = %v, want ErrInvalidMeetID", tc.id, err)
				}
			})
		}
	})

	t.Run("an empty id is generated, not rejected", func(t *testing.T) {
		h := newHarness(t, electing())
		m, err := create(t, h, "")
		if err != nil {
			t.Fatalf("CreateMeet(\"\") = %v, want a generated id", err)
		}
		if m.ID == "" {
			t.Fatal("generated meet id is empty")
		}
		second, err := create(t, h, "")
		if err != nil || second.ID == m.ID {
			t.Fatalf("second generated id = %q (err %v), want a distinct id", second.ID, err)
		}
	})

	t.Run("a duplicate id is refused", func(t *testing.T) {
		h := newHarness(t, electing())
		if _, err := create(t, h, "standup"); err != nil {
			t.Fatalf("CreateMeet: %v", err)
		}
		if _, err := create(t, h, "standup"); !errors.Is(err, arbiter.ErrMeetExists) {
			t.Errorf("err = %v, want ErrMeetExists", err)
		}
	})

	t.Run("an unknown meet is an error", func(t *testing.T) {
		h := newHarness(t, electing())
		ctx, cancel := context.WithTimeout(context.Background(), syncWait)
		defer cancel()
		if _, err := h.a.GetMeet(ctx, "nope"); !errors.Is(err, arbiter.ErrMeetNotFound) {
			t.Errorf("err = %v, want ErrMeetNotFound", err)
		}
	})

	t.Run("a peer joining an uncreated meet creates it", func(t *testing.T) {
		h := newHarness(t, electing())
		h.join("adhoc", "p1", strong("alice"))
		if m := h.meet("adhoc"); m.ID != "adhoc" || m.Members != 1 {
			t.Errorf("meet = %+v, want adhoc with 1 member", m)
		}
	})

	t.Run("ListMeets is newest first, then id ascending", func(t *testing.T) {
		h := newHarness(t, electing())
		if _, err := create(t, h, "beta"); err != nil {
			t.Fatal(err)
		}
		if _, err := create(t, h, "alpha"); err != nil { // same instant as beta
			t.Fatal(err)
		}
		h.clk.Advance(time.Minute)
		if _, err := create(t, h, "gamma"); err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), syncWait)
		defer cancel()
		got, err := h.a.ListMeets(ctx)
		if err != nil {
			t.Fatalf("ListMeets: %v", err)
		}
		ids := []string{}
		for _, m := range got {
			ids = append(ids, m.ID)
		}
		if want := []string{"gamma", "alpha", "beta"}; !reflect.DeepEqual(ids, want) {
			t.Errorf("ids = %v, want %v", ids, want)
		}
	})
}

// TestMeetDerivation checks that the arbiter reports the REALIZED subnet, rebuilt
// from the heartbeats it already terminates — it may not import the coordinator, and
// asking the coordinator would reintroduce the circularity the liveness view exists
// to avoid.
func TestMeetDerivation(t *testing.T) {
	t.Run("no realized tree reports depth -1", func(t *testing.T) {
		h := newHarness(t, electing())
		h.join("m", "p1", strong("alice"))
		h.beat("m", "p1")

		m := h.meet("m")
		if m.Depth != -1 || len(m.Relays) != 0 {
			t.Errorf("meet = %+v, want Depth -1 and no relays", m)
		}
	})

	t.Run("relays and depth come from realized heartbeats", func(t *testing.T) {
		h := newHarness(t, electing())
		h.join("m", "p1", strong("alice"))
		h.join("m", "p2", mid("bob"))
		h.join("m", "p3", mid("carol"))
		h.join("m", "p4", mid("dave"))

		// alice -> bob -> carol, alice -> dave
		h.beatTree("m", "p1", "", []string{"bob", "dave"}, 1, 4)
		h.beatTree("m", "p2", "alice", []string{"carol"}, 1, 4)
		h.beatTree("m", "p3", "bob", nil, 1, 4)
		h.beatTree("m", "p4", "alice", nil, 1, 3)

		m := h.meet("m")
		if want := []string{"alice", "bob"}; !reflect.DeepEqual(m.Relays, want) {
			t.Errorf("Relays = %v, want %v (ascending)", m.Relays, want)
		}
		if m.Depth != 2 {
			t.Errorf("Depth = %d, want 2 hops root->leaf", m.Depth)
		}
	})

	// Rev is a coordinator counter, not an observable. A heartbeat-derived rev would be
	// a fabricated number wearing an authoritative name, so the field must not exist —
	// which, like the upload exclusion, can only be asserted against the type.
	t.Run("Meet carries no Rev", func(t *testing.T) {
		mt := reflect.TypeOf(arbiter.Meet{})
		for i := 0; i < mt.NumField(); i++ {
			if n := mt.Field(i).Name; strings.EqualFold(n, "rev") {
				t.Errorf("Meet.%s: rev is the coordinator's counter and is not observable here", n)
			}
		}
	})
}

// TestIngestionIsDefensive: every one of these arrives from the network.
func TestIngestionIsDefensive(t *testing.T) {
	t.Run("a malformed payload is dropped, not fatal", func(t *testing.T) {
		h := newHarness(t, electing())
		h.join("m", "p1", strong("alice"))
		h.a.Metrics("m", "p1", []byte("{not json"))
		h.a.Heartbeat("m", "p1", []byte(""))
		h.sync()
		h.wantCoord("m", "alice", 1)
	})

	t.Run("a frame for an unknown peer is ignored", func(t *testing.T) {
		h := newHarness(t, electing())
		h.join("m", "p1", strong("alice"))
		h.a.Heartbeat("m", "ghost", []byte(`{"name":"ghost","seq":1}`))
		h.sync()
		if m := h.meet("m"); m.Members != 1 {
			t.Errorf("Members = %d, want 1: a frame must not conjure a member", m.Members)
		}
	})

	t.Run("a leave for an unknown peer or meet is ignored", func(t *testing.T) {
		h := newHarness(t, electing())
		h.a.PeerLeft("nope", "ghost")
		h.a.PeerLeft("m", "ghost")
		h.sync()
	})

	t.Run("callbacks do not block after shutdown", func(t *testing.T) {
		h := newHarness(t, electing())
		h.join("m", "p1", strong("alice"))
		h.stop()
		select {
		case <-h.done:
		case <-deadline(t):
			t.Fatal("Run did not return")
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			h.a.PeerJoined("m", "p2", "bob")
			h.a.PeerLeft("m", "p2")
			h.a.Metrics("m", "p1", []byte(`{}`))
			h.a.Heartbeat("m", "p1", []byte(`{}`))
		}()
		select {
		case <-done:
		case <-deadline(t):
			t.Fatal("an Observer callback blocked after the arbiter stopped")
		}
	})

	t.Run("a query after shutdown returns an error rather than hanging", func(t *testing.T) {
		h := newHarness(t, electing())
		h.stop()
		<-h.done
		ctx, cancel := context.WithTimeout(context.Background(), syncWait)
		defer cancel()
		if _, err := h.a.ListMeets(ctx); err == nil {
			t.Error("ListMeets after shutdown = nil error, want one")
		}
	})
}

// TestElectDisabled: with -elect off the arbiter never hands the role to a peer.
func TestElectDisabled(t *testing.T) {
	t.Run("no election happens at all", func(t *testing.T) {
		h := newHarness(t, arbiter.Config{ArbiterID: arbiter.DefaultArbiterID})
		h.join("m", "p1", strong("alice"))
		h.elapse("m", 5*time.Minute)

		if got := h.ann.forRoom("m"); len(got) != 0 {
			t.Fatalf("announcements = %d, want 0 with -elect off and -coordinate off", len(got))
		}
	})

	t.Run("the arbiter coordinates and never hands over", func(t *testing.T) {
		h := newHarness(t, arbiter.Config{Coordinate: true, ArbiterID: arbiter.DefaultArbiterID})
		h.join("m", "p1", strong("alice"))
		h.elapse("m", 5*time.Minute)

		m := h.meet("m")
		if !m.ArbiterIsCoord || m.Epoch != 1 {
			t.Errorf("meet = %+v, want the arbiter coordinating at epoch 1 forever", m)
		}
	})

	t.Run("with both, the arbiter hands over on the DWELL alone", func(t *testing.T) {
		cfg := electing()
		cfg.Coordinate = true
		h := newHarness(t, cfg)
		h.join("m", "p1", strong("alice"))
		if !h.meet("m").ArbiterIsCoord {
			t.Fatal("want the arbiter to hold the role at bootstrap")
		}

		// MinTermDuration gates only transitions that COULD flap. The arbiter does
		// not compete for the role, so arbiter->peer happens at most once per meet
		// and gating it just costs a minute in a stepping-stone configuration.
		h.elapseCurrent("m", 19*time.Second)
		if !h.meet("m").ArbiterIsCoord {
			t.Fatal("handed over before ElectionDwell elapsed")
		}

		h.elapseCurrent("m", 2*time.Second)
		m := h.meet("m")
		if m.ArbiterIsCoord || m.Coordinator != "alice" || m.Epoch != 2 {
			t.Errorf("meet = %+v, want alice coordinating at epoch 2 after ~20s, not 60s", m)
		}
	})

	t.Run("a peer-to-peer handover is still gated by MinTermDuration", func(t *testing.T) {
		cfg := electing()
		cfg.Coordinate = true
		h := newHarness(t, cfg)
		h.join("m", "p1", mid("alice"))
		h.elapseCurrent("m", 21*time.Second) // arbiter -> alice, ungated
		h.wantCoord("m", "alice", 2)

		h.join("m", "p2", strong("bob"))
		h.elapseCurrent("m", 30*time.Second) // dwell cleared, term floor not
		h.wantCoord("m", "alice", 2)

		h.elapseCurrent("m", 31*time.Second)
		h.wantCoord("m", "bob", 3)
	})
}

// TestConfigDefaults: a zero Config must be usable and must mean the frozen values.
func TestConfigDefaults(t *testing.T) {
	h := newHarness(t, arbiter.Config{Elect: true})
	h.join("m", "p1", mid("alice"))
	h.join("m", "p2", strong("bob"))

	h.elapse("m", 59*time.Second)
	h.wantCoord("m", "alice", 1) // MinTermDuration still holds
	h.elapse("m", 2*time.Second)
	h.wantCoord("m", "bob", 2)

	if got := h.lastAnn("m").CoordinatorID; got != "p2" {
		t.Errorf("CoordinatorID = %q, want p2", got)
	}
}
