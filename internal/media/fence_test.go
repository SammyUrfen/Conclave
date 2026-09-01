package media

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/SammyUrfen/conclave/internal/metrics"
	"github.com/SammyUrfen/conclave/internal/overlay"
	"github.com/SammyUrfen/conclave/internal/signaling"
)

// fenceRouter builds a managed Router with NO signaling client and NO roster. Every
// assertion below is about the decision to apply a pushed tree, which happens before
// a single id is resolved — so an empty roster keeps the whole test off pion and off
// the network while exercising the real applyTopology path.
func fenceRouter(t *testing.T, self string) *Router {
	t.Helper()
	return NewRouter(discardLog(), nil, RouterConfig{SelfName: self, Managed: true})
}

func topoPayload(t *testing.T, topo *overlay.Topology) []byte {
	t.Helper()
	raw, err := json.Marshal(topo)
	if err != nil {
		t.Fatalf("marshal topology: %v", err)
	}
	return raw
}

// TestRouterFenceAuthorization is the §6.5 AUTHORIZATION half, which the ordering
// check cannot express.
//
// Discrimination is the whole point here. Every case below except the last is
// REJECTED, and the "not the announced coordinator" case is the one an ordering-only
// gate ACCEPTS: `Topology.Supersedes` asks "is this tree newer", which is true no
// matter who sent it. That is the privilege-escalation shape — a peer that stamps a
// tree naming itself root supersedes everything and would be universally obeyed.
// Running this suite against a Supersedes-gated applyTopology fails on
// "a sender that is not the announced coordinator" and on both epoch cases.
func TestRouterFenceAuthorization(t *testing.T) {
	const coord = "p-coord"
	ctx := context.Background()

	// One tree, reused: only the fence state and the sender vary, so nothing about
	// the tree itself can explain a difference in outcome.
	tree := func(epoch, rev uint64) *overlay.Topology {
		return &overlay.Topology{
			Epoch: epoch, Rev: rev, Root: "a",
			Edges: []overlay.Edge{{Parent: "a", Child: "b"}},
		}
	}

	cases := []struct {
		name string
		// adopt, when non-zero, is the announcement the peer has adopted.
		adoptEpoch uint64
		adoptCoord string
		// rejoin replays a TypeJoined AFTER adopting, which must drop authority.
		rejoin bool
		from   string
		topo   *overlay.Topology
		want   bool
	}{
		{
			name: "unfenced peer obeys nobody",
			from: coord, topo: tree(1, 1), want: false,
		},
		{
			name:       "a sender that is not the announced coordinator is rejected",
			adoptEpoch: 1, adoptCoord: coord,
			from: "p-impostor", topo: tree(1, 1), want: false,
		},
		{
			name:       "an epoch ahead of the announcement is rejected",
			adoptEpoch: 1, adoptCoord: coord,
			from: coord, topo: tree(9, 1), want: false,
		},
		{
			name:       "a stale epoch is rejected",
			adoptEpoch: 3, adoptCoord: coord,
			from: coord, topo: tree(2, 99), want: false,
		},
		{
			name:       "a rejoin drops adopted authority",
			adoptEpoch: 1, adoptCoord: coord, rejoin: true,
			from: coord, topo: tree(1, 1), want: false,
		},
		{
			name:       "the announced coordinator at the current epoch is accepted",
			adoptEpoch: 1, adoptCoord: coord,
			from: coord, topo: tree(1, 1), want: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := fenceRouter(t, "b")
			if tc.adoptEpoch != 0 {
				if !r.AdoptCoordinator(tc.adoptEpoch, tc.adoptCoord) {
					t.Fatalf("AdoptCoordinator(%d, %q) refused a fresh announcement", tc.adoptEpoch, tc.adoptCoord)
				}
			}
			if tc.rejoin {
				r.handle(ctx, signaling.Message{Type: signaling.TypeJoined, To: "p-self"})
			}

			before := r.StaleRejected()
			r.applyTopology(ctx, tc.from, topoPayload(t, tc.topo))

			applied := r.currentTopo() != nil
			if applied != tc.want {
				t.Fatalf("topology applied = %v, want %v (fence=%+v)", applied, tc.want, r.Fence())
			}
			if rejections := r.StaleRejected() - before; (rejections == 1) == tc.want {
				t.Errorf("staleRejected moved by %d with applied=%v; a rejection MUST be counted "+
					"and an acceptance must not be", rejections, applied)
			}
			if tc.want && r.Fence().Rev != tc.topo.Rev {
				t.Errorf("fence Rev = %d after applying rev %d; the watermark must advance or the "+
					"same tree is applied forever", r.Fence().Rev, tc.topo.Rev)
			}
		})
	}
}

// TestRouterFenceAdvancesOnlyOnApply pins the Accept/Applied split: the watermark
// moves when a tree is realised, never merely because one was seen, and a replay of
// the same rev is then rejected.
func TestRouterFenceAdvancesOnlyOnApply(t *testing.T) {
	const coord = "p-coord"
	ctx := context.Background()
	r := fenceRouter(t, "b")
	r.AdoptCoordinator(2, coord)

	topo := &overlay.Topology{Epoch: 2, Rev: 5, Root: "a", Edges: []overlay.Edge{{Parent: "a", Child: "b"}}}
	r.applyTopology(ctx, coord, topoPayload(t, topo))
	if got := r.Fence().Rev; got != 5 {
		t.Fatalf("fence Rev = %d after a successful apply, want 5", got)
	}

	before := r.StaleRejected()
	r.applyTopology(ctx, coord, topoPayload(t, topo)) // exact replay
	if r.StaleRejected() != before+1 {
		t.Error("a replayed push was not rejected and counted")
	}

	// A malformed payload must not touch the watermark either.
	r.applyTopology(ctx, coord, []byte("{not json"))
	if got := r.Fence().Rev; got != 5 {
		t.Errorf("fence Rev = %d after a malformed payload, want 5", got)
	}
}

// TestRouterAdoptCoordinator pins the one method allowed to RAISE authority, and the
// asymmetry that makes the fence mean anything.
func TestRouterAdoptCoordinator(t *testing.T) {
	r := fenceRouter(t, "b")
	if !r.AdoptCoordinator(1, "p-a") {
		t.Fatal("a first announcement was not adopted")
	}
	if r.AdoptCoordinator(1, "p-b") {
		t.Error("a duplicate epoch was adopted; a replayed announcement must not re-point the fence")
	}
	if got := r.Fence().CoordinatorID; got != "p-a" {
		t.Errorf("CoordinatorID = %q after a refused announcement, want it untouched at p-a", got)
	}
	if !r.AdoptCoordinator(2, "p-b") {
		t.Error("a higher epoch was not adopted")
	}
	if f := r.Fence(); f.Epoch != 2 || f.CoordinatorID != "p-b" || f.Rev != 0 {
		t.Errorf("fence = %+v after a handover, want epoch 2 / p-b / rev 0", f)
	}
}

// TestRouterCoordinatorFrameReachesTheHost pins the seam: media may not import
// arbiter, so it hands the raw announcement body up and the host decodes it. Without
// this the frame is silently dropped and no peer is ever fenced.
func TestRouterCoordinatorFrameReachesTheHost(t *testing.T) {
	var got []byte
	r := NewRouter(discardLog(), nil, RouterConfig{
		SelfName: "b", Managed: true,
		OnCoordinator: func(payload []byte) { got = payload },
	})
	r.handle(context.Background(), signaling.Message{
		Type: signaling.TypeCoordinator, Payload: []byte(`{"epoch":7,"coordinator_id":"p-a"}`),
	})
	if string(got) != `{"epoch":7,"coordinator_id":"p-a"}` {
		t.Fatalf("OnCoordinator got %q, want the raw announcement body", got)
	}
}

// TestRouterDisableBackupPolarity pins the §15.13 rename. The whole reason the field
// was inverted is that a caller who forgets it must get the SAFE behaviour, so the
// test that matters is the ZERO VALUE one: a RouterConfig with nothing set must still
// promote a backup parent. Against the old `Backup bool` this case fails, because the
// zero value silently disabled failover.
func TestRouterDisableBackupPolarity(t *testing.T) {
	topo := &overlay.Topology{
		Epoch: 1, Rev: 1, Root: "a",
		Edges:   []overlay.Edge{{Parent: "a", Child: "r"}, {Parent: "r", Child: "c"}},
		Backups: []overlay.Backup{{Node: "c", Parent: "a"}},
	}
	cases := []struct {
		name          string
		disableBackup bool
		wantAttempt   bool
	}{
		{name: "the zero value promotes", disableBackup: false, wantAttempt: true},
		{name: "DisableBackup opts out", disableBackup: true, wantAttempt: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reports := make(chan metrics.Reparented, 4)
			r := NewRouter(discardLog(), nil, RouterConfig{
				SelfName: "c", Managed: true, DisableBackup: tc.disableBackup,
				OnReparented: func(rep metrics.Reparented) { reports <- rep },
			})
			r.topo = topo
			r.parentInUse = "r"

			r.onParentLost(context.Background(), "r")

			select {
			case rep := <-reports:
				if rep.OK {
					t.Fatalf("reported success with no roster to attach to: %+v", rep)
				}
				// The REASON is the discriminator: "disabled" means the promotion was
				// never attempted; anything else means it was tried and could not be
				// completed in this rosterless fixture.
				attempted := rep.Reason != "backup promotion disabled"
				if attempted != tc.wantAttempt {
					t.Errorf("attempted = %v (reason %q), want %v", attempted, rep.Reason, tc.wantAttempt)
				}
			default:
				t.Fatal("no Reparented report at all")
			}

			// §5.6 step 3: a promotion clears the local backup so a second failure
			// reports at once instead of re-targeting the parent it already holds.
			cleared := r.currentTopo().BackupOf("c") == ""
			if cleared != tc.wantAttempt {
				t.Errorf("local backup cleared = %v, want %v", cleared, tc.wantAttempt)
			}
		})
	}
}

// TestRouterBackupChildAdmission pins §7.5a rule 2: the ONLY warrant for an edge the
// tree does not contain is the receiver's own fence-accepted Backups assignment, and
// it fails closed on anything else.
func TestRouterBackupChildAdmission(t *testing.T) {
	withBackup := &overlay.Topology{
		Epoch: 1, Rev: 1, Root: "a",
		Edges:   []overlay.Edge{{Parent: "a", Child: "r"}, {Parent: "r", Child: "c"}},
		Backups: []overlay.Backup{{Node: "c", Parent: "a"}},
	}
	noBackup := &overlay.Topology{
		Epoch: 1, Rev: 1, Root: "a",
		Edges: []overlay.Edge{{Parent: "a", Child: "r"}, {Parent: "r", Child: "c"}},
	}
	elsewhere := &overlay.Topology{
		Epoch: 1, Rev: 1, Root: "a",
		Edges:   []overlay.Edge{{Parent: "a", Child: "r"}, {Parent: "r", Child: "c"}},
		Backups: []overlay.Backup{{Node: "c", Parent: "r"}},
	}

	cases := []struct {
		name   string
		self   string
		topo   *overlay.Topology
		sender string
		want   bool
	}{
		{name: "we are the assigned backup", self: "a", topo: withBackup, sender: "p-c", want: true},
		{name: "no assignment at this rev fails closed", self: "a", topo: noBackup, sender: "p-c", want: false},
		{name: "someone else is the assigned backup", self: "a", topo: elsewhere, sender: "p-c", want: false},
		{name: "an unknown sender is never admitted", self: "a", topo: withBackup, sender: "p-ghost", want: false},
		{name: "no topology at all admits nobody", self: "a", topo: nil, sender: "p-c", want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := NewRouter(discardLog(), nil, RouterConfig{SelfName: tc.self, Managed: true})
			r.topo = tc.topo
			r.learnPeer("p-c", "c")
			if got := r.acceptsBackupChild(tc.sender); got != tc.want {
				t.Errorf("acceptsBackupChild(%q) = %v, want %v", tc.sender, got, tc.want)
			}
		})
	}
}
