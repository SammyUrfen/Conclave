package overlay

import "testing"

// TestFenceZeroValueObeysNobody pins the state a peer starts and rejoins in: until an
// arbiter announcement is adopted, nothing is authorised. A peer that has not been told
// who is in charge obeys nobody.
func TestFenceZeroValueObeysNobody(t *testing.T) {
	var f Fence
	ok, reason := f.Accept("anyone", &Topology{Epoch: 1, Rev: 1, Root: "r"})
	if ok {
		t.Fatal("the zero Fence accepted a topology; it must authorise nothing")
	}
	if reason == "" {
		t.Error("Accept returned no reason; the dashboard's stale_rejected event needs one")
	}
}

// TestFenceAdoptAnnouncement covers the ONLY method that may raise Epoch — the entire
// mechanism of the safety argument.
func TestFenceAdoptAnnouncement(t *testing.T) {
	var f Fence
	if !f.AdoptAnnouncement(3, "coord-a") {
		t.Fatal("first announcement was not adopted")
	}
	if f.Epoch != 3 || f.CoordinatorID != "coord-a" || f.Rev != 0 {
		t.Fatalf("after adopt: %+v, want epoch 3, coord-a, rev 0", f)
	}

	// Apply something so Rev is non-zero, then prove a new term resets it.
	f.Applied(&Topology{Epoch: 3, Rev: 7})
	if f.Rev != 7 {
		t.Fatalf("Applied did not advance Rev: %+v", f)
	}
	if !f.AdoptAnnouncement(4, "coord-b") {
		t.Fatal("a higher-epoch announcement was not adopted")
	}
	if f.Epoch != 4 || f.CoordinatorID != "coord-b" || f.Rev != 0 {
		t.Fatalf("after re-adopt: %+v, want epoch 4, coord-b, rev 0 (a new term starts at rev 0)", f)
	}

	// Stale and duplicate announcements must not touch the fence at all.
	before := f
	for _, epoch := range []uint64{4, 3, 0} {
		if f.AdoptAnnouncement(epoch, "impostor") {
			t.Errorf("adopted a non-advancing announcement (epoch %d)", epoch)
		}
		if f != before {
			t.Fatalf("a rejected announcement mutated the fence: %+v, was %+v", f, before)
		}
	}
}

// TestFenceAccept is the authorization table. The load-bearing row is "higher epoch":
// rejecting it is the asymmetry that makes the fence mean anything, and accepting it
// would let any peer promote itself by stamping a bigger number.
func TestFenceAccept(t *testing.T) {
	base := Fence{Epoch: 5, CoordinatorID: "coord", Rev: 2}

	tests := []struct {
		name string
		from string
		topo *Topology
		want bool
	}{
		{"the sitting coordinator, next rev", "coord", &Topology{Epoch: 5, Rev: 3}, true},
		{"a rev far ahead within the term", "coord", &Topology{Epoch: 5, Rev: 99}, true},
		{"someone else entirely", "impostor", &Topology{Epoch: 5, Rev: 3}, false},
		{"a duplicate rev", "coord", &Topology{Epoch: 5, Rev: 2}, false},
		{"a reordered, older rev", "coord", &Topology{Epoch: 5, Rev: 1}, false},
		{"an older epoch", "coord", &Topology{Epoch: 4, Rev: 99}, false},
		// The privilege-escalation case: a self-promoting peer stamps a higher epoch.
		{"a HIGHER epoch is rejected, not adopted", "coord", &Topology{Epoch: 6, Rev: 1}, false},
		{"the maximum epoch is still rejected", "coord", &Topology{Epoch: StaticEpoch, Rev: 1}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := base
			ok, reason := f.Accept(tt.from, tt.topo)
			if ok != tt.want {
				t.Errorf("Accept = %v (%q), want %v", ok, reason, tt.want)
			}
			if !ok && reason == "" {
				t.Error("a rejection carried no reason")
			}
			if f != base {
				t.Errorf("Accept mutated the fence (%+v, was %+v); only Applied may advance Rev", f, base)
			}
		})
	}
}

// TestFenceAppliedIsSeparateFromAccept pins why the two are not one call: the caller
// accepts, attempts the fallible media work, and advances ONLY on success — so a failed
// apply is retried by the next push instead of being silently skipped.
func TestFenceAppliedIsSeparateFromAccept(t *testing.T) {
	f := Fence{Epoch: 5, CoordinatorID: "coord", Rev: 2}
	topo := &Topology{Epoch: 5, Rev: 3}

	if ok, _ := f.Accept("coord", topo); !ok {
		t.Fatal("setup: rev 3 should be acceptable")
	}
	// The media work failed, so Applied is NOT called. The same push must still be
	// acceptable when the coordinator retries it.
	if ok, _ := f.Accept("coord", topo); !ok {
		t.Fatal("a push that was accepted but not applied must remain acceptable")
	}
	f.Applied(topo)
	if f.Rev != 3 {
		t.Fatalf("Rev = %d, want 3", f.Rev)
	}
	if ok, _ := f.Accept("coord", topo); ok {
		t.Error("an already-applied rev was accepted twice")
	}
}

// TestFenceReset covers the rejoin rule: a peer that reconnects drops all authority and
// waits to be told again, which is what closes the arbiter-restart hole.
func TestFenceReset(t *testing.T) {
	f := Fence{Epoch: 9, CoordinatorID: "coord", Rev: 4}
	f.Reset()
	if (f != Fence{}) {
		t.Fatalf("after Reset: %+v, want the zero value", f)
	}
	if ok, _ := f.Accept("coord", &Topology{Epoch: 9, Rev: 5}); ok {
		t.Error("a reset fence still accepted its old coordinator; it must wait for an announcement")
	}
	// And a lower epoch than the one it held before is adoptable again — the arbiter
	// restarted and is minting from 1.
	if !f.AdoptAnnouncement(1, "coord") {
		t.Error("a reset fence refused a fresh low epoch; the arbiter-restart hole is open")
	}
}
