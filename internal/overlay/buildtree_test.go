package overlay

import (
	"encoding/json"
	"sort"
	"strings"
	"testing"
)

// stamp fills the (Epoch, Rev) a build must carry, so the table cases below only
// have to spell out the fields they are actually about.
func stamp(c Constraints) Constraints {
	if c.Epoch == 0 {
		c.Epoch = 1
	}
	if c.Rev == 0 {
		c.Rev = 1
	}
	return c
}

// mustBuild builds and fails the test on error. Used where the build is the setup
// for the assertion, not the assertion itself.
func mustBuild(t *testing.T, nodes []Node, prev *Topology, c Constraints) *Topology {
	t.Helper()
	topo, err := BuildTree(nodes, prev, stamp(c))
	if err != nil {
		t.Fatalf("BuildTree: %v", err)
	}
	return topo
}

// TestBuildTree drives the greedy builder over hand-picked fleets and asserts the
// result with the independent Validate oracle (never by re-deriving the expected
// tree, which would just re-implement the heuristic). Each case pins one property
// the builder must honour.
func TestBuildTree(t *testing.T) {
	tests := []struct {
		name      string
		nodes     []Node
		prev      *Topology
		cons      Constraints
		wantErr   bool
		wantEdges int
		// check runs extra structural assertions on a successful build.
		check func(t *testing.T, topo *Topology)
	}{
		{
			name:      "single node has no edges",
			nodes:     []Node{{Name: "a", UploadKbps: 4000}},
			cons:      Constraints{Root: "a", MaxDepth: 2, StreamKbps: 1000},
			wantEdges: 0,
		},
		{
			name: "star: strong root takes every leaf",
			nodes: []Node{
				{Name: "root", UploadKbps: 8000},
				{Name: "b", UploadKbps: 1000},
				{Name: "c", UploadKbps: 1000},
				{Name: "d", UploadKbps: 1000},
			},
			cons:      Constraints{Root: "root", MaxDepth: 2, StreamKbps: 2000}, // root cap = 4
			wantEdges: 3,
			check: func(t *testing.T, topo *Topology) {
				for _, leaf := range []string{"b", "c", "d"} {
					if topo.ParentOf(leaf) != "root" {
						t.Errorf("%s parent = %q, want root", leaf, topo.ParentOf(leaf))
					}
				}
			},
		},
		{
			name: "capacity forces a second level (depth 2 chain)",
			nodes: []Node{
				{Name: "root", UploadKbps: 2000}, // cap 1
				{Name: "mid", UploadKbps: 2000},  // cap 1 — strongest non-root, attaches first
				{Name: "leaf", UploadKbps: 0},    // cap 0
			},
			cons:      Constraints{Root: "root", MaxDepth: 2, StreamKbps: 2000},
			wantEdges: 2,
			check: func(t *testing.T, topo *Topology) {
				// root can hold exactly one child; the other node must hang below it.
				if topo.ParentOf("mid") != "root" {
					t.Errorf("mid parent = %q, want root", topo.ParentOf("mid"))
				}
				if topo.ParentOf("leaf") != "mid" {
					t.Errorf("leaf parent = %q, want mid (root is full)", topo.ParentOf("leaf"))
				}
			},
		},
		{
			name: "TURN node is a forced leaf even with huge upload",
			nodes: []Node{
				{Name: "root", UploadKbps: 8000},
				{Name: "turn", UploadKbps: 100000, NAT: NATRelayed}, // massive budget, but TURN ⇒ cap 0
				{Name: "leaf", UploadKbps: 1000},
			},
			cons:      Constraints{Root: "root", MaxDepth: 2, StreamKbps: 2000},
			wantEdges: 2,
			check: func(t *testing.T, topo *Topology) {
				if topo.IsRelay("turn") {
					t.Error("TURN node became a relay; must be a forced leaf")
				}
			},
		},
		{
			name: "RTT breaks a tie toward the closer relay",
			nodes: []Node{
				{Name: "root", UploadKbps: 2000}, // cap 1
				{Name: "r1", UploadKbps: 4000},   // cap 2, attaches to root
				{Name: "r2", UploadKbps: 4000},   // cap 2 — but root is full after r1
				{Name: "leaf", UploadKbps: 0, RTT: map[string]float64{"r1": 50, "r2": 5}},
			},
			cons:      Constraints{Root: "root", MaxDepth: 3, StreamKbps: 2000},
			wantEdges: 3,
			check: func(t *testing.T, topo *Topology) {
				// r1 (strongest, ties with r2 by upload but wins by name) attaches to
				// root first, filling it. r2 then attaches to r1 (only relay with room).
				// leaf's candidates are r1 and r2; it must pick r2 (RTT 5 < 50).
				if p := topo.ParentOf("leaf"); p != "r2" {
					t.Errorf("leaf parent = %q, want r2 (lower RTT)", p)
				}
			},
		},
		{
			name: "provisional node attaches as a leaf without objection",
			nodes: []Node{
				{Name: "root", UploadKbps: 8000},
				{Name: "guess", Provisional: true}, // no telemetry yet ⇒ 0 upload
			},
			cons:      Constraints{Root: "root", MaxDepth: 2, StreamKbps: 2000},
			wantEdges: 1,
		},
		{
			name: "previous parent vanished: the orphan re-attaches",
			nodes: []Node{
				{Name: "root", UploadKbps: 8000},
				{Name: "orphan", UploadKbps: 0},
			},
			// "gone" was orphan's parent in prev and is absent from nodes.
			prev: &Topology{Epoch: 1, Rev: 1, Root: "root", Edges: []Edge{
				{Parent: "root", Child: "gone"}, {Parent: "gone", Child: "orphan"},
			}},
			cons:      Constraints{Root: "root", MaxDepth: 2, StreamKbps: 2000, Rev: 2},
			wantEdges: 1,
			check: func(t *testing.T, topo *Topology) {
				if p := topo.ParentOf("orphan"); p != "root" {
					t.Errorf("orphan parent = %q, want root", p)
				}
				if topo.Depth("gone") != -1 {
					t.Error("a departed node survived into the new tree")
				}
			},
		},
		{
			name: "prev naming nodes that no longer exist is tolerated",
			nodes: []Node{
				{Name: "root", UploadKbps: 8000},
				{Name: "a", UploadKbps: 0},
			},
			prev: &Topology{Epoch: 1, Rev: 1, Root: "ghostroot", Edges: []Edge{
				{Parent: "ghostroot", Child: "ghost1"},
				{Parent: "ghost1", Child: "a"},
				{Parent: "ghost1", Child: "ghost2"},
			}},
			cons:      Constraints{Root: "root", MaxDepth: 2, StreamKbps: 2000, Rev: 2},
			wantEdges: 1,
		},
		{
			name: "over-constrained fleet fails loud",
			nodes: []Node{
				{Name: "root", UploadKbps: 2000}, // cap 1
				{Name: "a", UploadKbps: 0},
				{Name: "b", UploadKbps: 0},
			},
			cons:    Constraints{Root: "root", MaxDepth: 1, StreamKbps: 2000}, // star, cap 1, two leaves
			wantErr: true,
		},
		{
			name: "all-TURN fleet fails loud (nobody can parent)",
			nodes: []Node{
				{Name: "a", UploadKbps: 9000, NAT: NATRelayed},
				{Name: "b", UploadKbps: 9000, NAT: NATRelayed},
			},
			cons:    Constraints{Root: "a", MaxDepth: 2, StreamKbps: 2000},
			wantErr: true,
		},
		{
			name:      "lone TURN node is still a valid one-node tree",
			nodes:     []Node{{Name: "a", UploadKbps: 0, NAT: NATRelayed}},
			cons:      Constraints{Root: "a", MaxDepth: 2, StreamKbps: 2000},
			wantEdges: 0,
		},
		{
			name:    "unknown root fails",
			nodes:   []Node{{Name: "a", UploadKbps: 4000}},
			cons:    Constraints{Root: "ghost", MaxDepth: 2, StreamKbps: 1000},
			wantErr: true,
		},
		{
			name:    "TURN root with peers is rejected",
			nodes:   []Node{{Name: "root", UploadKbps: 9000, NAT: NATRelayed}, {Name: "b", UploadKbps: 1000}},
			cons:    Constraints{Root: "root", MaxDepth: 2, StreamKbps: 2000},
			wantErr: true,
		},
		{
			name:    "bad constraints (StreamKbps 0) fails",
			nodes:   []Node{{Name: "a", UploadKbps: 4000}},
			cons:    Constraints{Root: "a", MaxDepth: 2, StreamKbps: 0},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cons := stamp(tt.cons)
			topo, err := BuildTree(tt.nodes, tt.prev, cons)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("BuildTree: expected error, got tree %+v", topo)
				}
				return
			}
			if err != nil {
				t.Fatalf("BuildTree: unexpected error: %v", err)
			}
			if len(topo.Edges) != tt.wantEdges {
				t.Errorf("edges = %d, want %d (%+v)", len(topo.Edges), tt.wantEdges, topo.Edges)
			}
			// A successful build must place EVERY node: silently dropping one is the
			// "mysterious missing stream" the fail-loud rule exists to prevent.
			if got := len(nodeSet(topo, cons.Root)); got != len(tt.nodes) {
				t.Errorf("tree holds %d nodes, want all %d", got, len(tt.nodes))
			}
			if err := Validate(topo, tt.nodes, cons); err != nil {
				t.Errorf("built tree fails Validate: %v\ntree: %+v", err, topo.Edges)
			}
			if tt.check != nil {
				tt.check(t, topo)
			}
		})
	}
}

// nodeSet returns every name in the tree, root included even when it has no edges.
func nodeSet(t *Topology, root string) map[string]bool {
	set := map[string]bool{root: true}
	for _, n := range t.Nodes() {
		set[n] = true
	}
	return set
}

// TestBuildTreeStamps pins the fencing token BuildTree writes and the guard against
// a non-advancing revision — a tree stamped with a rev no peer will accept is worse
// than no tree, because it fails silently at every peer instead of loudly here.
func TestBuildTreeStamps(t *testing.T) {
	nodes := []Node{{Name: "root", UploadKbps: 8000}, {Name: "a", UploadKbps: 0}}
	base := Constraints{Root: "root", MaxDepth: 2, StreamKbps: 2000, Epoch: 7, Rev: 3}

	topo, err := BuildTree(nodes, nil, base)
	if err != nil {
		t.Fatalf("BuildTree: %v", err)
	}
	if topo.Epoch != 7 || topo.Rev != 3 || topo.Root != "root" {
		t.Errorf("stamp = (epoch %d, rev %d, root %q), want (7, 3, root)", topo.Epoch, topo.Rev, topo.Root)
	}

	bad := []struct {
		name string
		prev *Topology
		cons Constraints
	}{
		{"epoch 0 is unpublishable", nil, Constraints{Root: "root", MaxDepth: 2, StreamKbps: 2000, Epoch: 0, Rev: 1}},
		{"rev 0 is unpublishable", nil, Constraints{Root: "root", MaxDepth: 2, StreamKbps: 2000, Epoch: 1, Rev: 0}},
		{
			"rev must advance within an epoch",
			&Topology{Epoch: 7, Rev: 3, Root: "root"},
			Constraints{Root: "root", MaxDepth: 2, StreamKbps: 2000, Epoch: 7, Rev: 3},
		},
		{
			"rev must not go backwards within an epoch",
			&Topology{Epoch: 7, Rev: 9, Root: "root"},
			Constraints{Root: "root", MaxDepth: 2, StreamKbps: 2000, Epoch: 7, Rev: 8},
		},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			if got, err := BuildTree(nodes, tc.prev, tc.cons); err == nil {
				t.Errorf("BuildTree accepted a bad stamp, got %+v", got)
			}
		})
	}

	// A NEW epoch resets rev to 1, which is legal even though 1 < prev.Rev.
	prev := &Topology{Epoch: 7, Rev: 9, Root: "root"}
	next, err := BuildTree(nodes, prev, Constraints{Root: "root", MaxDepth: 2, StreamKbps: 2000, Epoch: 8, Rev: 1})
	if err != nil {
		t.Fatalf("BuildTree across an epoch bump: %v", err)
	}
	if !next.Supersedes(prev) {
		t.Error("a tree from a newer epoch must supersede the previous one")
	}
}

// TestBuildTreeStickiness is the §3.4 rank-1 rule, tested at both extremes of the
// knob as the ROADMAP demands: an incumbent parent survives a less-loaded and a
// marginally-closer challenger, and loses only to one that beats StickinessMs.
func TestBuildTreeStickiness(t *testing.T) {
	// R (cap 4) roots; P and Q (cap 2 each) are its children; u hangs off P.
	prev := &Topology{Epoch: 1, Rev: 1, Root: "R", Edges: []Edge{
		{Parent: "R", Child: "P"},
		{Parent: "R", Child: "Q"},
		{Parent: "P", Child: "u"},
	}}
	fleet := func(rttToQ float64) []Node {
		return []Node{
			{Name: "R", UploadKbps: 8000},
			{Name: "P", UploadKbps: 4000},
			{Name: "Q", UploadKbps: 4000},
			{Name: "u", RTT: map[string]float64{"P": 60, "Q": rttToQ}},
		}
	}

	tests := []struct {
		name       string
		rttToQ     float64
		stickiness float64
		wantParent string
	}{
		// 50 + 25 = 75, not < 60: a 10 ms win is not worth a stream interruption.
		{"marginally closer challenger loses", 50, DefaultStickinessMs, "P"},
		// 30 + 25 = 55 < 60: a materially closer parent breaks incumbency.
		{"materially closer challenger wins", 30, DefaultStickinessMs, "Q"},
		// Stickiness 0 is Phase 4's memoryless behaviour: any RTT win moves the node.
		{"stickiness 0 is memoryless", 50, 0, "Q"},
		// A large margin pins the tree in place even against a much closer parent.
		{"large stickiness pins the tree", 1, 1000, "P"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nodes := fleet(tt.rttToQ)
			cons := Constraints{Root: "R", MaxDepth: 3, StreamKbps: 2000, Epoch: 1, Rev: 2, StickinessMs: tt.stickiness}
			topo := mustBuild(t, nodes, prev, cons)
			if got := topo.ParentOf("u"); got != tt.wantParent {
				t.Errorf("u parent = %q, want %q", got, tt.wantParent)
			}
			if err := Validate(topo, nodes, cons); err != nil {
				t.Errorf("Validate: %v", err)
			}
		})
	}

	// Load alone NEVER breaks incumbency: P is the incumbent and busier than Q, and
	// no RTT is known anywhere (the live case — §13.2).
	loadNodes := []Node{
		{Name: "R", UploadKbps: 8000},
		{Name: "P", UploadKbps: 8000},
		{Name: "Q", UploadKbps: 8000},
		{Name: "u"}, {Name: "v"}, {Name: "w"},
	}
	loadPrev := &Topology{Epoch: 1, Rev: 1, Root: "R", Edges: []Edge{
		{Parent: "R", Child: "P"}, {Parent: "R", Child: "Q"},
		{Parent: "P", Child: "u"}, {Parent: "P", Child: "v"}, {Parent: "P", Child: "w"},
	}}
	cons := Constraints{Root: "R", MaxDepth: 3, StreamKbps: 2000, Epoch: 1, Rev: 2, StickinessMs: DefaultStickinessMs}
	topo := mustBuild(t, loadNodes, loadPrev, cons)
	for _, n := range []string{"u", "v", "w"} {
		if got := topo.ParentOf(n); got != "P" {
			t.Errorf("%s parent = %q, want P (an idle sibling must not steal an incumbent's children)", n, got)
		}
	}
}

// TestBuildTreeProcessingOrder is the §3.4(A) fix for the memoryless-rebuild defect:
// a strong newcomer must not displace an incumbent's claim on a parent's capacity.
func TestBuildTreeProcessingOrder(t *testing.T) {
	// root has capacity for exactly 2 children, and both are already taken.
	nodes := []Node{
		{Name: "root", UploadKbps: 4000}, // cap 2
		{Name: "a", UploadKbps: 4000},    // cap 2
		{Name: "b", UploadKbps: 0},
		{Name: "strong", UploadKbps: 100000}, // the newcomer
	}
	prev := &Topology{Epoch: 1, Rev: 1, Root: "root", Edges: []Edge{
		{Parent: "root", Child: "a"},
		{Parent: "root", Child: "b"},
	}}
	cons := Constraints{Root: "root", MaxDepth: 3, StreamKbps: 2000, Epoch: 1, Rev: 2, StickinessMs: DefaultStickinessMs}

	topo := mustBuild(t, nodes, prev, cons)
	if got := topo.ParentOf("a"); got != "root" {
		t.Errorf("a parent = %q, want root (incumbent keeps its slot)", got)
	}
	if got := topo.ParentOf("b"); got != "root" {
		t.Errorf("b parent = %q, want root (incumbent keeps its slot)", got)
	}
	if got := topo.ParentOf("strong"); got != "a" {
		t.Errorf("strong parent = %q, want a (the only relay with room)", got)
	}
	if err := Validate(topo, nodes, cons); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if err := ValidateLocalRepair(prev, topo, nil, []string{"strong"}, nil); err != nil {
		t.Errorf("a pure join must not re-parent anyone: %v", err)
	}

	// Contrast: from scratch (prev == nil) the strongest node DOES claim a root slot.
	// This is not a bug — it is exactly the Phase 4 behaviour prev is there to damp.
	fresh := mustBuild(t, nodes, nil, cons)
	if got := fresh.ParentOf("strong"); got != "root" {
		t.Errorf("from-scratch build: strong parent = %q, want root", got)
	}
}

// TestBuildTreeDeterministic guards the promise the whole simnet harness rests on:
// same input → same tree, every time, despite Go's randomised map iteration. If the
// builder ever depended on map order, this would flake. Compared as marshalled
// bytes so a divergence in Backups or the stamp counts too, not just Edges.
func TestBuildTreeDeterministic(t *testing.T) {
	nodes := []Node{
		{Name: "root", UploadKbps: 6000},
		// Equal RTTs on purpose: ties are where a map-order dependency would show.
		{Name: "a", UploadKbps: 4000, RTT: map[string]float64{"root": 10, "b": 10, "c": 10}},
		{Name: "b", UploadKbps: 4000, RTT: map[string]float64{"root": 10, "a": 10, "c": 10}},
		{Name: "c", UploadKbps: 4000, RTT: map[string]float64{"root": 10, "a": 10, "b": 10}},
		{Name: "d", UploadKbps: 2000},
		{Name: "e", UploadKbps: 2000},
	}
	cons := Constraints{Root: "root", MaxDepth: 2, StreamKbps: 2000, Epoch: 1, Rev: 1, StickinessMs: DefaultStickinessMs}

	for _, withPrev := range []bool{false, true} {
		var prev *Topology
		c := cons
		if withPrev {
			prev = mustBuild(t, nodes, nil, cons)
			c.Rev = 2
		}
		first, err := BuildTree(nodes, prev, c)
		if err != nil {
			t.Fatalf("BuildTree: %v", err)
		}
		want, err := json.Marshal(first)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 50; i++ {
			again, err := BuildTree(nodes, prev, c)
			if err != nil {
				t.Fatalf("BuildTree run %d: %v", i, err)
			}
			got, err := json.Marshal(again)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != string(want) {
				t.Fatalf("non-deterministic build (prev=%v)\n run 0: %s\n run %d: %s", withPrev, want, i, got)
			}
		}
	}
}

// TestPickRoot covers §3.8: never provisional, sticky within the margin, and a
// deterministic fallback.
func TestPickRoot(t *testing.T) {
	tests := []struct {
		name  string
		nodes []Node
		prev  *Topology
		want  string
	}{
		{
			name: "highest upload wins when there is no incumbent",
			nodes: []Node{
				{Name: "a", UploadKbps: 2000},
				{Name: "b", UploadKbps: 8000},
			},
			want: "b",
		},
		{
			name: "ties break by name",
			nodes: []Node{
				{Name: "b", UploadKbps: 8000},
				{Name: "a", UploadKbps: 8000},
			},
			want: "a",
		},
		{
			name: "a provisional node is never rooted, however strong",
			nodes: []Node{
				{Name: "guess", UploadKbps: 100000, Provisional: true},
				{Name: "known", UploadKbps: 2000},
			},
			want: "known",
		},
		{
			name: "a zero-upload node cannot root a fleet",
			nodes: []Node{
				{Name: "a", UploadKbps: 0},
				{Name: "b", UploadKbps: 2000},
			},
			want: "b",
		},
		{
			name:  "TURN-bound nodes cannot root",
			nodes: []Node{{Name: "a", UploadKbps: 9000, NAT: NATRelayed}, {Name: "b", UploadKbps: 1000}},
			want:  "b",
		},
		{
			name:  "no eligible node at all",
			nodes: []Node{{Name: "a", Provisional: true}, {Name: "b", UploadKbps: 0}},
			want:  "",
		},
		{
			name:  "a lone node roots itself even when provisional",
			nodes: []Node{{Name: "solo", Provisional: true}},
			want:  "solo",
		},
		{
			name: "incumbent survives a challenger inside the margin",
			nodes: []Node{
				{Name: "inc", UploadKbps: 6000},
				{Name: "chal", UploadKbps: 6000 + RootChangeMarginKbps - 1},
			},
			prev: &Topology{Epoch: 1, Rev: 1, Root: "inc"},
			want: "inc",
		},
		{
			name: "incumbent loses to a challenger beyond the margin",
			nodes: []Node{
				{Name: "inc", UploadKbps: 6000},
				{Name: "chal", UploadKbps: 6000 + RootChangeMarginKbps},
			},
			prev: &Topology{Epoch: 1, Rev: 1, Root: "inc"},
			want: "chal",
		},
		{
			name: "a departed incumbent hands the root over",
			nodes: []Node{
				{Name: "x", UploadKbps: 2000},
				{Name: "y", UploadKbps: 4000},
			},
			prev: &Topology{Epoch: 1, Rev: 1, Root: "gone"},
			want: "y",
		},
		{
			name: "an incumbent that has gone TURN-bound hands the root over",
			nodes: []Node{
				{Name: "inc", UploadKbps: 9000, NAT: NATRelayed},
				{Name: "y", UploadKbps: 2000},
			},
			prev: &Topology{Epoch: 1, Rev: 1, Root: "inc"},
			want: "y",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := PickRoot(tt.nodes, tt.prev); got != tt.want {
				t.Errorf("PickRoot = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestPickRootNeverRootsAGuess replays the fleet from the live defect: a 3-peer
// managed run where the strong relay had not yet reported, the projection showed it
// at the assumed default, and PickRoot handed BuildTree a root that could serve
// nobody ("root \"leaf-b\" cannot serve any children").
//
// What the Provisional rule closes, and is asserted here: a node whose number is a
// GUESS can never be rooted, in any arrival order.
//
// What it does NOT close, stated rather than papered over: a node that has really
// reported an upload too small to serve one child at the fleet's StreamKbps is
// still eligible here, because PickRoot's frozen signature does not receive
// Constraints and so cannot compute capacity. In that window PickRoot returns a
// truthful-but-useless root and BuildTree rejects it — an honest "unbuildable" over
// real telemetry rather than a guess, and one the coordinator's first-build settle
// rule is what actually suppresses. Closing it inside overlay needs StreamKbps (or
// a minimum-upload argument) on PickRoot; that is a contract gap, not a code bug.
func TestPickRootNeverRootsAGuess(t *testing.T) {
	real := map[string]int{"relay-a": 8000, "leaf-b": 1200, "leaf-c": 1200}
	names := []string{"relay-a", "leaf-b", "leaf-c"}
	cons := Constraints{MaxDepth: 2, StreamKbps: 2000, Epoch: 1, Rev: 1}

	for _, order := range permutations(names) {
		// Only the first reporter has real telemetry; the rest are projected at the
		// coordinator's assumed default and flagged provisional.
		var nodes []Node
		for i, n := range order {
			if i == 0 {
				nodes = append(nodes, Node{Name: n, UploadKbps: real[n]})
			} else {
				nodes = append(nodes, Node{Name: n, Provisional: true})
			}
		}
		root := PickRoot(nodes, nil)
		if root == "" {
			continue // nobody eligible yet: the coordinator waits, which is correct
		}
		for _, n := range nodes {
			if n.Name == root && n.Provisional {
				t.Fatalf("order %v: PickRoot rooted the meet on a guess (%q)", order, root)
			}
		}
		c := cons
		c.Root = root
		if _, err := BuildTree(nodes, nil, c); err != nil {
			// The only legal failure left: the chosen root really did report an
			// upload below one stream. Anything else would be the old defect.
			if real[root] >= cons.StreamKbps {
				t.Fatalf("order %v: build over root %q failed although it can serve a child: %v", order, root, err)
			}
		}
	}
}

// TestReportOrderInvarianceSettledPath is §12.3 as written, and its limits are
// stated here rather than left to look stronger than they are.
//
// WHAT IT COVERS: with §5.9's early exit (every member reported before the first
// build), all permutations feed BuildTree the same node SET, differing only in
// SLICE ORDER. So it proves BuildTree and PickRoot do not leak input order — which
// is a real regression guard against a map range or a first-seen tiebreak sneaking
// in — and nothing more.
//
// WHAT IT DOES NOT COVER: the straggler path, where the settle bound expires before
// everyone has reported. There the permutations produce different HISTORIES, not
// just different orders, and the converged trees legitimately differ. See
// TestStragglerPathIsPathDependent.
func TestReportOrderInvarianceSettledPath(t *testing.T) {
	fleet := []Node{
		{Name: "n1", UploadKbps: 8000, RTT: map[string]float64{"n2": 10, "n3": 20}},
		{Name: "n2", UploadKbps: 6000, RTT: map[string]float64{"n1": 10, "n3": 15}},
		{Name: "n3", UploadKbps: 2000, RTT: map[string]float64{"n1": 20, "n2": 15}},
		{Name: "n4", UploadKbps: 0, RTT: map[string]float64{"n1": 30, "n2": 5}},
		{Name: "n5", UploadKbps: 0, RTT: map[string]float64{"n1": 5, "n2": 40}},
	}
	byName := map[string]Node{}
	names := make([]string, 0, len(fleet))
	for _, n := range fleet {
		byName[n.Name] = n
		names = append(names, n.Name)
	}

	var want string
	var wantOrder []string
	perms := permutations(names)
	if len(perms) != 120 {
		t.Fatalf("expected 120 permutations, got %d", len(perms))
	}
	for _, order := range perms {
		nodes := make([]Node, 0, len(order))
		for _, n := range order {
			nodes = append(nodes, byName[n])
		}
		cons := Constraints{MaxDepth: 2, StreamKbps: 2000, Epoch: 1, Rev: 1, StickinessMs: DefaultStickinessMs}
		cons.Root = PickRoot(nodes, nil)
		topo, err := BuildTree(nodes, nil, cons)
		if err != nil {
			t.Fatalf("order %v: %v", order, err)
		}
		if err := Validate(topo, nodes, cons); err != nil {
			t.Fatalf("order %v: Validate: %v", order, err)
		}
		blob, err := json.Marshal(topo)
		if err != nil {
			t.Fatal(err)
		}
		if want == "" {
			want, wantOrder = string(blob), order
			continue
		}
		if string(blob) != want {
			t.Fatalf("input order changed the tree\n order %v: %s\n order %v: %s", wantOrder, want, order, blob)
		}
	}
}

// TestStragglerPathIsPathDependent is a CHARACTERIZATION test, not a wish.
//
// Stickiness (§3.4 rank 1) makes BuildTree a function of history, not just of the
// fleet: that is precisely the property traded FOR minimal-disruption rebuilds, and
// the two cannot both hold. On the straggler path — where FirstBuildSettle expires
// with only some members heard from — different arrival orders produce different
// first trees, and every later rebuild preserves whichever one it inherited. So two
// runs over the SAME fleet legitimately converge to two different valid trees.
//
// What must still hold, and is asserted here: every tree is Validate-clean, holds
// every node, and the converged ROOT is the same (root stickiness is damped by
// RootChangeMarginKbps, not by history). If the divergence ever disappears, the
// stability trade-off has changed and docs/PLAN.md §3.4 must be revisited — hence
// the deliberate assertion that at least two permutations differ.
func TestStragglerPathIsPathDependent(t *testing.T) {
	// The reviewer's counterexample fleet.
	fleet := []Node{
		{Name: "A", UploadKbps: 4000},
		{Name: "B", UploadKbps: 4000},
		{Name: "C", UploadKbps: 12000},
		{Name: "D", UploadKbps: 4000},
		{Name: "E", UploadKbps: 4000},
	}
	byName := map[string]Node{}
	names := make([]string, 0, len(fleet))
	for _, n := range fleet {
		byName[n.Name] = n
		names = append(names, n.Name)
	}
	cons := Constraints{MaxDepth: 2, StreamKbps: 2000, StickinessMs: DefaultStickinessMs, Epoch: 1}

	// converge runs the straggler model: the first `settled` reports land inside the
	// settle window and produce the first tree; the rest straggle in one at a time,
	// each triggering a stability-preserving rebuild.
	converge := func(order []string, settled int) *Topology {
		t.Helper()
		reported := map[string]bool{}
		var prev *Topology
		for i, arriving := range order {
			reported[arriving] = true
			if i+1 < settled {
				continue // still inside the settle window: no tree yet
			}
			nodes := make([]Node, 0, len(names))
			for _, n := range names { // name-ordered projection, as the coordinator's is
				if reported[n] {
					nodes = append(nodes, byName[n])
				} else {
					nodes = append(nodes, Node{Name: n, Provisional: true})
				}
			}
			c := cons
			c.Root = PickRoot(nodes, prev)
			if c.Root == "" {
				continue
			}
			c.Rev = 1
			if prev != nil {
				c.Rev = prev.Rev + 1
			}
			next, err := BuildTree(nodes, prev, c)
			if err != nil {
				continue // over-constrained for now: keep the previous tree
			}
			if err := Validate(next, nodes, c); err != nil {
				t.Fatalf("order %v: published tree fails Validate: %v", order, err)
			}
			prev = next
		}
		return prev
	}

	seen := map[string][]string{} // edges JSON → the first order that produced it
	rootSeen := ""
	for _, order := range permutations(names) {
		final := converge(order, 2) // settle expires with two members reported
		if final == nil {
			t.Fatalf("order %v: never converged to a tree", order)
		}
		if got := len(nodeSet(final, final.Root)); got != len(names) {
			t.Fatalf("order %v: converged tree holds %d of %d nodes", order, got, len(names))
		}
		if rootSeen == "" {
			rootSeen = final.Root
		} else if final.Root != rootSeen {
			t.Fatalf("converged root depends on arrival order: %q vs %q (order %v)", rootSeen, final.Root, order)
		}
		blob, err := json.Marshal(final.Edges)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := seen[string(blob)]; !ok {
			seen[string(blob)] = order
		}
	}
	if len(seen) < 2 {
		t.Fatalf("expected the straggler path to be path-dependent (see the doc comment), "+
			"but every permutation converged to the same tree: %v", seen)
	}
	t.Logf("straggler path converged to %d distinct valid trees over the same fleet", len(seen))
	// Sorted, because a map range would make the reported evidence differ run to run.
	shapes := make([]string, 0, len(seen))
	for blob := range seen {
		shapes = append(shapes, blob)
	}
	sort.Strings(shapes)
	for _, blob := range shapes {
		t.Logf("  order %v ⇒ %s", seen[blob], blob)
	}
}

// permutations returns every ordering of in. Generated recursively over copied
// slices so the output is itself deterministic — no map iteration, no rand — which
// is what lets a failing case be reported as a reproducible arrival order.
func permutations(in []string) [][]string {
	if len(in) <= 1 {
		return [][]string{append([]string(nil), in...)}
	}
	var out [][]string
	for i := range in {
		rest := make([]string, 0, len(in)-1)
		rest = append(rest, in[:i]...)
		rest = append(rest, in[i+1:]...)
		for _, tail := range permutations(rest) {
			out = append(out, append([]string{in[i]}, tail...))
		}
	}
	return out
}

// TestBuildTreePrevRootOrdering pins the case §3.4(A) leaves undefined, and it is
// undefined in a way two implementers would resolve differently: after a re-root,
// the OLD root is present in nodes but appears nowhere in prev.Edges, so it is
// neither "an incumbent, in prev.Edges order" nor "a newcomer with no entry in
// prev". Whichever way it is resolved must be pinned, or a later refactor will
// silently pick the other one.
//
// This package resolves it by processing prev.Root FIRST, at the position
// prev.Edges implicitly gives it (that order is topological precisely BECAUSE it
// starts at the root). The consequence is the assertion below: the former root is
// already attached by the time its former children are processed, so their
// incumbency still has a live candidate and they stay put. Had it been processed
// with the newcomers — after them — it would not yet be attached, every one of its
// children would fall through to rank 2 over whatever else was available, and a
// re-root would scatter the old root's entire subtree for nothing. That is the
// exact opposite of what the stability-preserving rebuild exists to do.
func TestBuildTreePrevRootOrdering(t *testing.T) {
	// "new" arrives with enough upload to clear RootChangeMarginKbps over "old",
	// so PickRoot hands it the root and the tree must re-root around it.
	nodes := []Node{
		{Name: "new", UploadKbps: 20000},
		{Name: "old", UploadKbps: 6000}, // the previous root: in nodes, not in prev.Edges
		{Name: "x", UploadKbps: 0},      // old's children in prev…
		{Name: "y", UploadKbps: 0},
	}
	prev := &Topology{Epoch: 1, Rev: 1, Root: "old", Edges: []Edge{
		{Parent: "old", Child: "x"},
		{Parent: "old", Child: "y"},
	}}
	cons := Constraints{Root: "new", MaxDepth: 3, StreamKbps: 2000, Epoch: 1, Rev: 2, StickinessMs: DefaultStickinessMs}

	// The fixture is only meaningful if the root really did change, and PickRoot is
	// what decides that in production — so assert it rather than assuming it.
	if got := PickRoot(nodes, prev); got != "new" {
		t.Fatalf("fixture drifted: PickRoot = %q, want new (a re-root is the case under test)", got)
	}

	topo := mustBuild(t, nodes, prev, cons)
	if err := Validate(topo, nodes, cons); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if got := topo.ParentOf("old"); got != "new" {
		t.Errorf("old parent = %q, want new (the former root attaches under the new one)", got)
	}
	// The load-bearing assertion: the former root's children are NOT scattered.
	for _, child := range []string{"x", "y"} {
		if got := topo.ParentOf(child); got != "old" {
			t.Errorf("%s parent = %q, want old — a re-root must not re-parent the former root's children", child, got)
		}
	}
	// Stated as a churn property too, which is the form the coordinator asserts in:
	// a re-root is the one legitimately global repair, so the oracle must report it
	// as such — and report nothing else, since nobody but "old" actually moved.
	err := ValidateLocalRepair(prev, topo, nil, []string{"new"}, nil)
	if err == nil || !strings.Contains(err.Error(), "root changed") {
		t.Errorf("ValidateLocalRepair = %v, want it to flag the re-root (and only that)", err)
	}
}
