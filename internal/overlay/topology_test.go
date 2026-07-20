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
	topo := &Topology{Edges: []Edge{
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
