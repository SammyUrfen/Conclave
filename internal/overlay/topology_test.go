package overlay

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestTopology covers the pure tree queries the Router drives every decision from.
func TestTopology(t *testing.T) {
	// A: root, R: relay (children B,C,D), leaves B,C,D.
	topo := &Topology{Root: "A", Edges: []Edge{
		{Parent: "A", Child: "R"},
		{Parent: "R", Child: "B"},
		{Parent: "R", Child: "C"},
		{Parent: "R", Child: "D"},
	}}

	if got := topo.ParentOf("R"); got != "A" {
		t.Errorf("ParentOf(R) = %q, want A", got)
	}
	if got := topo.ParentOf("A"); got != "" {
		t.Errorf("ParentOf(A) = %q, want \"\" (root)", got)
	}
	if got := topo.ChildrenOf("R"); len(got) != 3 {
		t.Errorf("ChildrenOf(R) = %v, want 3 children", got)
	}
	if got := topo.NeighborsOf("R"); len(got) != 4 { // B,C,D + parent A
		t.Errorf("NeighborsOf(R) = %v, want 4", got)
	}
	if got := topo.NeighborsOf("B"); len(got) != 1 || got[0] != "R" {
		t.Errorf("NeighborsOf(B) = %v, want [R]", got)
	}
	if !topo.IsRelay("R") || topo.IsRelay("B") {
		t.Errorf("IsRelay: R=%v B=%v, want true,false", topo.IsRelay("R"), topo.IsRelay("B"))
	}
	// A has child R, so it is structurally a relay too — a single-child root that
	// only receives (its forwarder would have no downstream legs). Harmless, and the
	// honest reading of "has children".
	if !topo.IsRelay("A") {
		t.Error("IsRelay(A) = false, want true (A has child R)")
	}

	// Offerer: the relay offers on every edge; the non-relay answers.
	if !topo.Offers("R", "B") {
		t.Error("Offers(R,B) = false, want true (relay offers)")
	}
	if topo.Offers("B", "R") {
		t.Error("Offers(B,R) = true, want false (leaf answers)")
	}
	if !topo.Offers("R", "A") {
		t.Error("Offers(R,A) = false, want true (relay offers toward its parent too)")
	}

	// Nodes: every distinct peer, parents before the children that introduce them.
	if got := topo.Nodes(); len(got) != 5 || got[0] != "A" || got[1] != "R" {
		t.Errorf("Nodes() = %v, want [A R B C D]", got)
	}
}

// TestDepthAndSubtree pins the two accessors the backup invariant (§3.5) and local
// repair (§5.5) are written in terms of. Both must answer for a name that is not in
// the tree at all — the coordinator asks about departed peers.
func TestDepthAndSubtree(t *testing.T) {
	topo := &Topology{Root: "A", Edges: []Edge{
		{Parent: "A", Child: "R"},
		{Parent: "R", Child: "B"},
		{Parent: "R", Child: "C"},
		{Parent: "B", Child: "D"},
	}}

	depths := map[string]int{"A": 0, "R": 1, "B": 2, "C": 2, "D": 3, "ghost": -1}
	for name, want := range depths {
		if got := topo.Depth(name); got != want {
			t.Errorf("Depth(%q) = %d, want %d", name, got, want)
		}
	}

	// BFS order: the node itself, then each level left-to-right in edge order.
	subtrees := map[string][]string{
		"A":     {"A", "R", "B", "C", "D"},
		"R":     {"R", "B", "C", "D"},
		"B":     {"B", "D"},
		"C":     {"C"},
		"ghost": nil,
	}
	for name, want := range subtrees {
		got := topo.Subtree(name)
		if len(got) != len(want) {
			t.Errorf("Subtree(%q) = %v, want %v", name, got, want)
			continue
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("Subtree(%q) = %v, want %v", name, got, want)
				break
			}
		}
	}

	// A single-node tree has no edges but still has a root at depth 0.
	lone := &Topology{Root: "solo"}
	if got := lone.Depth("solo"); got != 0 {
		t.Errorf("Depth(root of a one-node tree) = %d, want 0", got)
	}
	if got := lone.Subtree("solo"); len(got) != 1 || got[0] != "solo" {
		t.Errorf("Subtree(root of a one-node tree) = %v, want [solo]", got)
	}
}

// TestBackupOf covers the accessor peers call on parent failure.
func TestBackupOf(t *testing.T) {
	topo := &Topology{Root: "A", Edges: []Edge{
		{Parent: "A", Child: "R"},
		{Parent: "R", Child: "B"},
	}, Backups: []Backup{{Node: "B", Parent: "A"}}}

	if got := topo.BackupOf("B"); got != "A" {
		t.Errorf("BackupOf(B) = %q, want A", got)
	}
	if got := topo.BackupOf("R"); got != "" {
		t.Errorf("BackupOf(R) = %q, want \"\" (root child, no backup)", got)
	}
	if got := topo.BackupOf("ghost"); got != "" {
		t.Errorf("BackupOf(ghost) = %q, want \"\"", got)
	}
}

// TestSupersedes pins the fencing comparison. It is the ONE place the (Epoch, Rev)
// order is written, so every wrong answer here is a wrong answer everywhere.
func TestSupersedes(t *testing.T) {
	tests := []struct {
		name string
		t    *Topology
		prev *Topology
		want bool
	}{
		{"anything supersedes nil", &Topology{Epoch: 1, Rev: 1}, nil, true},
		{"higher rev, same epoch", &Topology{Epoch: 1, Rev: 2}, &Topology{Epoch: 1, Rev: 1}, true},
		{"equal is not strictly newer", &Topology{Epoch: 1, Rev: 1}, &Topology{Epoch: 1, Rev: 1}, false},
		{"lower rev, same epoch", &Topology{Epoch: 1, Rev: 1}, &Topology{Epoch: 1, Rev: 2}, false},
		{"higher epoch wins despite lower rev", &Topology{Epoch: 2, Rev: 1}, &Topology{Epoch: 1, Rev: 99}, true},
		{"lower epoch loses despite higher rev", &Topology{Epoch: 1, Rev: 99}, &Topology{Epoch: 2, Rev: 1}, false},
		{"a static tree is never superseded", &Topology{Epoch: 9, Rev: 9}, &Topology{Epoch: StaticEpoch, Rev: 1}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.t.Supersedes(tt.prev); got != tt.want {
				t.Errorf("Supersedes = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestLoadTopology covers the loader's fail-loud validation, including the
// single-parent tree invariant (a re-parenting copy/paste slip must not slip
// through and become a silently half-formed tree at run time).
func TestLoadTopology(t *testing.T) {
	tests := []struct {
		name    string
		json    string
		wantErr string // substring; "" means expect success
	}{
		{name: "valid star", json: `{"edges":[{"parent":"r","child":"b"},{"parent":"r","child":"d"}]}`},
		{name: "valid two-level", json: `{"edges":[{"parent":"a","child":"r"},{"parent":"r","child":"b"}]}`},
		{name: "no edges", json: `{"edges":[]}`, wantErr: "no edges"},
		{name: "missing child", json: `{"edges":[{"parent":"r","child":""}]}`, wantErr: "required"},
		{name: "self parent", json: `{"edges":[{"parent":"r","child":"r"}]}`, wantErr: "its own parent"},
		{name: "two parents", json: `{"edges":[{"parent":"p1","child":"c"},{"parent":"p2","child":"c"}]}`, wantErr: "two parents"},
		{name: "bad json", json: `{"edges":`, wantErr: "parse"},
		// Two disjoint stars pass the single-parent check but have two parentless
		// nodes, so the root cannot be derived: fail loud rather than guess.
		{name: "two roots", json: `{"edges":[{"parent":"p1","child":"c1"},{"parent":"p2","child":"c2"}]}`, wantErr: "root"},
		// An explicit root that disagrees with the edges is the "redundancy is a
		// chance to disagree" case §3.1 makes a hard error.
		{name: "explicit root disagrees", json: `{"root":"b","edges":[{"parent":"r","child":"b"}]}`, wantErr: "root"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "tree.json")
			if err := os.WriteFile(path, []byte(tt.json), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := LoadTopology(path)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("LoadTopology(%s): unexpected error: %v", tt.name, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("LoadTopology(%s): got %v, want error containing %q", tt.name, err, tt.wantErr)
			}
		})
	}
}

// TestLoadTopologyStampsDefaults is the Phase 3 compatibility contract (§3.9): a
// hand-authored file has no epoch/rev/root, and the loader must fill them so the
// rest of the system can treat a static tree like any other — while StaticEpoch
// keeps a coordinator push from ever overriding the operator's file.
func TestLoadTopologyStampsDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tree.json")
	const phase3File = `{"edges":[{"parent":"a","child":"r"},{"parent":"r","child":"b"}]}`
	if err := os.WriteFile(path, []byte(phase3File), 0o600); err != nil {
		t.Fatal(err)
	}
	topo, err := LoadTopology(path)
	if err != nil {
		t.Fatalf("LoadTopology: %v", err)
	}
	if topo.Epoch != StaticEpoch {
		t.Errorf("Epoch = %d, want StaticEpoch %d", topo.Epoch, StaticEpoch)
	}
	if topo.Rev != 1 {
		t.Errorf("Rev = %d, want 1", topo.Rev)
	}
	if topo.Root != "a" {
		t.Errorf("Root = %q, want a (the unique parentless node)", topo.Root)
	}
	if topo.Backups != nil {
		t.Errorf("Backups = %v, want nil (a static tree has no failover)", topo.Backups)
	}
	// The whole point of StaticEpoch: no coordinator-minted tree can supersede it.
	if (&Topology{Epoch: 1 << 40, Rev: 1 << 40}).Supersedes(topo) {
		t.Error("a coordinator push superseded a static -topology file")
	}
}
