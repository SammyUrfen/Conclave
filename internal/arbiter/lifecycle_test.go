package arbiter_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/SammyUrfen/conclave/internal/arbiter"
	"github.com/SammyUrfen/conclave/internal/policy"
)

func create(t *testing.T, h *harness, id string) (arbiter.Meet, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), syncWait)
	defer cancel()
	return h.a.CreateMeet(ctx, id)
}

// TestMeetReaping covers the lifecycle that turns POST /api/meets from a one-shot fill
// into an actual bound: empty -> MeetTTL -> reaped -> bounded tombstone ring.
func TestMeetReaping(t *testing.T) {
	t.Run("an empty meet survives until MeetTTL", func(t *testing.T) {
		h := newHarness(t, electing())
		h.join("m", "p1", strong("alice"))
		h.leave("m", "p1")

		h.clk.Advance(arbiter.MeetTTL - time.Second)
		if got := h.meetIDs(); !reflect.DeepEqual(got, []string{"m"}) {
			t.Fatalf("meets = %v, want [m] — reaped before MeetTTL", got)
		}
	})

	t.Run("an empty meet is reaped past MeetTTL and leaves a tombstone", func(t *testing.T) {
		h := newHarness(t, electing())
		h.join("m", "p1", strong("alice"))
		h.join("m", "p2", mid("bob"))
		h.leave("m", "p1")
		h.leave("m", "p2")

		h.clk.Advance(arbiter.MeetTTL)
		if got := h.meetIDs(); len(got) != 0 {
			t.Fatalf("meets = %v, want none", got)
		}
		ended := h.ended()
		if len(ended) != 1 {
			t.Fatalf("tombstones = %d, want 1", len(ended))
		}
		got := ended[0]
		if got.ID != "m" || got.PeakMembers != 2 || got.FinalEpoch == 0 {
			t.Errorf("tombstone = %+v, want id m, peak 2, a non-zero final epoch", got)
		}
		if got.Elections < 1 {
			t.Errorf("Elections = %d, want at least the bootstrap", got.Elections)
		}
		if !got.EndedAt.After(got.CreatedAt) {
			t.Errorf("EndedAt %v is not after CreatedAt %v", got.EndedAt, got.CreatedAt)
		}
	})

	t.Run("a rejoin inside the TTL cancels the reap and keeps the epoch", func(t *testing.T) {
		h := newHarness(t, electing())
		h.join("m", "p1", strong("alice"))
		epoch := h.meet("m").Epoch
		h.leave("m", "p1")

		h.clk.Advance(arbiter.MeetTTL - time.Second)
		h.join("m", "p2", mid("bob"))
		h.clk.Advance(arbiter.MeetTTL)
		h.sync()

		if got := h.meetIDs(); !reflect.DeepEqual(got, []string{"m"}) {
			t.Fatalf("meets = %v, want [m] — emptiness must be CONTINUOUS to reap", got)
		}
		if got := h.meet("m").Epoch; got < epoch {
			t.Errorf("Epoch = %d, want >= %d — a lull does not reset the counter", got, epoch)
		}
	})

	t.Run("reaping is lazy, driven by a mutation or a listing", func(t *testing.T) {
		h := newHarness(t, electing())
		h.join("m", "p1", strong("alice"))
		h.leave("m", "p1")
		h.clk.Advance(arbiter.MeetTTL)

		// No timer runs. The listing itself is what sweeps.
		if got := h.meetIDs(); len(got) != 0 {
			t.Fatalf("meets = %v, want none after a listing swept them", got)
		}
	})

	t.Run("the tombstone ring is bounded and FIFO", func(t *testing.T) {
		h := newHarness(t, electing())
		for i := 0; i < arbiter.MaxEndedMeets+5; i++ {
			id := fmt.Sprintf("m%02d", i)
			h.join(id, "p1", strong("alice"))
			h.leave(id, "p1")
			h.clk.Advance(arbiter.MeetTTL + time.Second)
			h.meetIDs() // sweep
		}

		ended := h.ended()
		if len(ended) != arbiter.MaxEndedMeets {
			t.Fatalf("tombstones = %d, want %d", len(ended), arbiter.MaxEndedMeets)
		}
		for _, e := range ended {
			if e.ID == "m00" {
				t.Errorf("oldest tombstone m00 survived; the ring must evict oldest-first")
			}
		}
		// Newest first, so an operator sees the demo they just ran at the top.
		for i := 1; i < len(ended); i++ {
			if ended[i-1].EndedAt.Before(ended[i].EndedAt) {
				t.Errorf("tombstones are not newest-first at index %d", i)
			}
		}
	})

	t.Run("a reaped id may be created again", func(t *testing.T) {
		h := newHarness(t, electing())
		h.join("m", "p1", strong("alice"))
		h.leave("m", "p1")
		h.clk.Advance(arbiter.MeetTTL)
		h.meetIDs()

		if _, err := create(t, h, "m"); err != nil {
			t.Fatalf("CreateMeet after reap = %v, want success", err)
		}
	})
}

// TestMaxMeets bounds the only unauthenticated write surface in the system.
func TestMaxMeets(t *testing.T) {
	t.Run("create is refused past MaxMeets", func(t *testing.T) {
		h := newHarness(t, electing())
		for i := 0; i < arbiter.MaxMeets; i++ {
			if _, err := create(t, h, fmt.Sprintf("m%03d", i)); err != nil {
				t.Fatalf("CreateMeet %d: %v", i, err)
			}
		}
		if _, err := create(t, h, "onemore"); !errors.Is(err, arbiter.ErrTooManyMeets) {
			t.Errorf("err = %v, want ErrTooManyMeets", err)
		}
	})

	t.Run("reaping makes room again", func(t *testing.T) {
		h := newHarness(t, electing())
		for i := 0; i < arbiter.MaxMeets; i++ {
			if _, err := create(t, h, fmt.Sprintf("m%03d", i)); err != nil {
				t.Fatalf("CreateMeet %d: %v", i, err)
			}
		}
		h.clk.Advance(arbiter.MeetTTL)
		if _, err := create(t, h, "onemore"); err != nil {
			t.Errorf("CreateMeet after the TTL expired = %v, want success", err)
		}
	})

	t.Run("a peer joining is never refused", func(t *testing.T) {
		h := newHarness(t, electing())
		for i := 0; i < arbiter.MaxMeets; i++ {
			if _, err := create(t, h, fmt.Sprintf("m%03d", i)); err != nil {
				t.Fatalf("CreateMeet %d: %v", i, err)
			}
		}
		// The Hub has already admitted this peer. Refusing to track it would leave a
		// meet with live members the arbiter cannot see — silently uncoordinated,
		// which is worse than exceeding a bound meant for the REST surface.
		h.join("overflow", "p1", strong("alice"))
		h.wantCoord("overflow", "alice", 1)
	})
}

// TestGeneratedMeetID pins the frozen id format.
func TestGeneratedMeetID(t *testing.T) {
	h := newHarness(t, electing())
	seen := map[string]bool{}
	for i := 0; i < 50; i++ {
		m, err := create(t, h, "")
		if err != nil {
			t.Fatalf("CreateMeet(\"\"): %v", err)
		}
		if !strings.HasPrefix(m.ID, "m-") || len(m.ID) != 10 {
			t.Fatalf("generated id %q, want \"m-\" plus 8 characters", m.ID)
		}
		// Crockford base32 excludes i, l, o, and u so an id read aloud is unambiguous.
		if strings.ContainsAny(m.ID[2:], "ilou") {
			t.Errorf("generated id %q contains a Crockford-excluded letter", m.ID)
		}
		if !policy.ValidMeetID(m.ID) {
			t.Errorf("generated id %q does not satisfy policy.ValidMeetID", m.ID)
		}
		if seen[m.ID] {
			t.Fatalf("generated id %q twice", m.ID)
		}
		seen[m.ID] = true
	}
}

// TestMeetIDValidationDelegatesToPolicy: the arbiter must not carry its own copy of the
// boundary matcher. Two copies of a security-relevant matcher drift, and the drift's
// failure mode is silent.
func TestMeetIDValidationDelegatesToPolicy(t *testing.T) {
	for _, id := range []string{"standup", "a", strings.Repeat("a", 64), strings.Repeat("a", 65),
		"Standup", "-lead", "has space", "has/slash", "_lead"} {
		t.Run(id, func(t *testing.T) {
			h := newHarness(t, electing())
			_, err := create(t, h, id)
			rejected := errors.Is(err, arbiter.ErrInvalidMeetID)
			if want := !policy.ValidMeetID(id); rejected != want {
				t.Errorf("CreateMeet(%q) rejected = %v, want %v (policy.ValidMeetID says %v)",
					id, rejected, want, policy.ValidMeetID(id))
			}
		})
	}
}

// TestNoVolunteersIsAnnouncedToTheOperator is the discrimination test for failing
// closed LOUDLY. With no peer opting in, election never fires and the meet stays on the
// arbiter forever — correct, but indistinguishable from a bug unless something says so.
func TestNoVolunteersIsAnnouncedToTheOperator(t *testing.T) {
	const warn = "no peer has volunteered to coordinate"

	t.Run("a meet with no reports yet stays quiet", func(t *testing.T) {
		h := newHarness(t, electing())
		h.joinSilent("m", "p1", "alice")
		h.joinSilent("m", "p2", "bob")
		h.elapse("m", 30*time.Second)

		if n := h.logs.count(warn); n != 0 {
			t.Errorf("warns = %d, want 0 — a young meet has not failed, it is waiting", n)
		}
	})

	t.Run("reports in hand with nobody willing warns once", func(t *testing.T) {
		h := newHarness(t, electing())
		h.join("m", "p1", unwilling("alice"))
		h.join("m", "p2", unwilling("bob"))
		h.elapse("m", 30*time.Second)

		// Thirty beats, one warn: the transition, not the sample.
		if n := h.logs.count(warn); n != 1 {
			t.Errorf("warns = %d, want exactly 1 (emitted on the transition)", n)
		}
	})

	t.Run("the state clears when someone volunteers and can warn again", func(t *testing.T) {
		h := newHarness(t, electing())
		h.join("m", "p1", unwilling("alice"))
		h.elapse("m", 5*time.Second)
		if n := h.logs.count(warn); n != 1 {
			t.Fatalf("warns = %d, want 1", n)
		}

		h.report("m", "p1", strong("alice"))
		h.wantCoord("m", "alice", 1)

		h.report("m", "p1", unwilling("alice"))
		h.elapse("m", 5*time.Second)
		if n := h.logs.count(warn); n != 2 {
			t.Errorf("warns = %d, want 2 — re-entering the state is a new transition", n)
		}
	})

	t.Run("a meet that already has a peer coordinator does not warn", func(t *testing.T) {
		h := newHarness(t, electing())
		h.join("m", "p1", strong("alice"))
		h.elapseCurrent("m", 30*time.Second)

		if n := h.logs.count(warn); n != 0 {
			t.Errorf("warns = %d, want 0 — a volunteer exists and holds the role", n)
		}
	})
}
