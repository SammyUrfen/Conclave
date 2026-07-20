package overlay

import (
	"testing"
)

// TestBuildTree drives the greedy builder over hand-picked fleets and asserts the
// result with the independent Validate oracle (never by re-deriving the expected
// tree, which would just re-implement the heuristic). Each case pins one property
// the builder must honour.
func TestBuildTree(t *testing.T) {
	tests := []struct {
		name      string
		nodes     []Node
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
			topo, err := BuildTree(tt.nodes, tt.cons)
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
			if err := Validate(topo, tt.nodes, tt.cons); err != nil {
				t.Errorf("built tree fails Validate: %v\ntree: %+v", err, topo.Edges)
			}
			if tt.check != nil {
				tt.check(t, topo)
			}
		})
	}
}

// TestBuildTreeDeterministic guards the promise the whole simnet harness rests on:
// same input → same tree, every time, despite Go's randomised map iteration. If the
// builder ever depended on map order, this would flake.
func TestBuildTreeDeterministic(t *testing.T) {
	nodes := []Node{
		{Name: "root", UploadKbps: 6000},
		{Name: "a", UploadKbps: 4000},
		{Name: "b", UploadKbps: 4000},
		{Name: "c", UploadKbps: 4000},
		{Name: "d", UploadKbps: 2000},
		{Name: "e", UploadKbps: 2000},
	}
	cons := Constraints{Root: "root", MaxDepth: 2, StreamKbps: 2000}

	first, err := BuildTree(nodes, cons)
	if err != nil {
		t.Fatalf("BuildTree: %v", err)
	}
	for i := 0; i < 50; i++ {
		again, err := BuildTree(nodes, cons)
		if err != nil {
			t.Fatalf("BuildTree run %d: %v", i, err)
		}
		if !sameEdges(first.Edges, again.Edges) {
			t.Fatalf("non-deterministic build:\n run 0: %+v\n run %d: %+v", first.Edges, i, again.Edges)
		}
	}
}

// TestValidateCatches proves the oracle actually rejects broken trees — otherwise
// a passing Validate in the tests above would mean nothing.
func TestValidateCatches(t *testing.T) {
	nodes := []Node{
		{Name: "root", UploadKbps: 2000}, // cap 1
		{Name: "a", UploadKbps: 2000},
		{Name: "b", UploadKbps: 2000},
	}
	cons := Constraints{Root: "root", MaxDepth: 2, StreamKbps: 2000}

	bad := []struct {
		name  string
		edges []Edge
	}{
		{"over capacity", []Edge{{"root", "a"}, {"root", "b"}}}, // root cap 1, given 2 children
		{"two parents", []Edge{{"root", "a"}, {"root", "b"}, {"a", "b"}}},
		{"disconnected", []Edge{{"root", "a"}}}, // b never attached
		{"unknown node", []Edge{{"root", "a"}, {"root", "ghost"}}},
		{"root has parent", []Edge{{"a", "root"}, {"root", "b"}}},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			if err := Validate(&Topology{Edges: tc.edges}, nodes, cons); err == nil {
				t.Errorf("Validate accepted a broken tree (%s): %+v", tc.name, tc.edges)
			}
		})
	}

	// A depth violation needs a fleet that permits the shape but a bound that forbids
	// it: root→a→b is depth 2, rejected under MaxDepth 1.
	deepNodes := []Node{{Name: "root", UploadKbps: 4000}, {Name: "a", UploadKbps: 4000}, {Name: "b", UploadKbps: 0}}
	deepCons := Constraints{Root: "root", MaxDepth: 1, StreamKbps: 2000}
	if err := Validate(&Topology{Edges: []Edge{{"root", "a"}, {"a", "b"}}}, deepNodes, deepCons); err == nil {
		t.Error("Validate accepted a tree deeper than MaxDepth 1")
	}
}

func sameEdges(a, b []Edge) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
