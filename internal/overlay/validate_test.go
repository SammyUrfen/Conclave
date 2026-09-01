package overlay

import (
	"strings"
	"testing"
)

// oracleFleet is the fixture the Validate cases mutate: R roots, P and Q are its
// children, u hangs off P and v off Q, and u's backup is Q.
func oracleFleet() ([]Node, Constraints, *Topology) {
	nodes := []Node{
		{Name: "R", UploadKbps: 8000}, // cap 4
		{Name: "P", UploadKbps: 8000}, // cap 4
		{Name: "Q", UploadKbps: 4000}, // cap 2
		{Name: "u", UploadKbps: 0},
		{Name: "v", UploadKbps: 0},
	}
	cons := Constraints{Root: "R", MaxDepth: 3, StreamKbps: 2000, Epoch: 1, Rev: 1}
	topo := &Topology{
		Epoch: 1, Rev: 1, Root: "R",
		Edges: []Edge{
			{Parent: "R", Child: "P"},
			{Parent: "R", Child: "Q"},
			{Parent: "P", Child: "u"},
			{Parent: "Q", Child: "v"},
		},
		Backups: []Backup{{Node: "u", Parent: "Q"}},
	}
	return nodes, cons, topo
}

// TestValidateAcceptsAWellFormedTree makes sure the mutation cases below fail for
// the reason they claim and not because the fixture was broken to begin with.
func TestValidateAcceptsAWellFormedTree(t *testing.T) {
	nodes, cons, topo := oracleFleet()
	if err := Validate(topo, nodes, cons); err != nil {
		t.Fatalf("the fixture itself does not validate: %v", err)
	}
}

// TestValidateCatches proves the oracle actually rejects broken trees — otherwise
// a passing Validate anywhere else in this package would mean nothing. Each case
// mutates exactly one property of the fixture and names the substring the error
// must mention, so a case cannot pass by tripping a different check.
func TestValidateCatches(t *testing.T) {
	tests := []struct {
		name string
		// extra declares nodes the mutation attaches, so the closed-world check is
		// not what fails the case.
		extra   []Node
		mutate  func(nodes []Node, c *Constraints, t *Topology)
		wantErr string
	}{
		{
			name:    "epoch 0 is unpublishable",
			mutate:  func(_ []Node, _ *Constraints, top *Topology) { top.Epoch = 0 },
			wantErr: "epoch",
		},
		{
			name:    "rev 0 is unpublishable",
			mutate:  func(_ []Node, _ *Constraints, top *Topology) { top.Rev = 0 },
			wantErr: "rev",
		},
		{
			name:    "Root disagrees with the constraints",
			mutate:  func(_ []Node, _ *Constraints, top *Topology) { top.Root = "P" },
			wantErr: "root",
		},
		{
			name:    "Root is empty",
			mutate:  func(_ []Node, _ *Constraints, top *Topology) { top.Root = "" },
			wantErr: "root",
		},
		{
			name: "edges are not in topological order",
			mutate: func(_ []Node, _ *Constraints, top *Topology) {
				// P→u placed before R→P introduces P.
				top.Edges = []Edge{{"P", "u"}, {"R", "P"}, {"R", "Q"}, {"Q", "v"}}
			},
			wantErr: "order",
		},
		{
			name: "over capacity",
			mutate: func(_ []Node, c *Constraints, top *Topology) {
				c.StreamKbps = 8000 // R's capacity collapses to 1, but it has 2 children
			},
			wantErr: "capacity",
		},
		{
			name: "two parents",
			mutate: func(_ []Node, _ *Constraints, top *Topology) {
				top.Edges = append(top.Edges, Edge{Parent: "Q", Child: "u"})
			},
			wantErr: "two parents",
		},
		{
			name: "disconnected node",
			mutate: func(_ []Node, _ *Constraints, top *Topology) {
				top.Edges = top.Edges[:3] // v never attached
				top.Backups = nil
			},
			wantErr: "no parent",
		},
		{
			name: "unknown node in an edge",
			mutate: func(_ []Node, _ *Constraints, top *Topology) {
				top.Edges = append(top.Edges, Edge{Parent: "R", Child: "ghost"})
			},
			wantErr: "known node",
		},
		{
			name: "depth exceeded",
			mutate: func(_ []Node, c *Constraints, top *Topology) {
				c.MaxDepth = 1
				top.Backups = nil
			},
			wantErr: "MaxDepth",
		},
		{
			name: "TURN-bound node used as a parent",
			mutate: func(nodes []Node, _ *Constraints, top *Topology) {
				nodes[1].NAT = NATRelayed // P has a child
			},
			wantErr: "TURN",
		},
		{
			name: "two backups for one node",
			mutate: func(_ []Node, _ *Constraints, top *Topology) {
				top.Backups = append(top.Backups, Backup{Node: "u", Parent: "R"})
			},
			wantErr: "backup",
		},
		{
			name: "backup for an unknown node",
			mutate: func(_ []Node, _ *Constraints, top *Topology) {
				top.Backups = append(top.Backups, Backup{Node: "ghost", Parent: "R"})
			},
			wantErr: "backup",
		},
		{
			name: "backup for the root",
			mutate: func(_ []Node, _ *Constraints, top *Topology) {
				top.Backups = append(top.Backups, Backup{Node: "R", Parent: "P"})
			},
			wantErr: "backup",
		},
		{
			name: "backup parent is the node itself",
			mutate: func(_ []Node, _ *Constraints, top *Topology) {
				top.Backups = []Backup{{Node: "u", Parent: "u"}}
			},
			wantErr: "backup",
		},
		{
			name: "backup parent is the current parent",
			mutate: func(_ []Node, _ *Constraints, top *Topology) {
				top.Backups = []Backup{{Node: "u", Parent: "P"}}
			},
			wantErr: "backup",
		},
		{
			name:  "backup inside Subtree(P): a sibling dies with the same failure",
			extra: []Node{{Name: "w", UploadKbps: 0}},
			mutate: func(_ []Node, _ *Constraints, top *Topology) {
				// Give P a second child and make it u's backup.
				top.Edges = append(top.Edges, Edge{Parent: "P", Child: "w"})
				top.Backups = []Backup{{Node: "u", Parent: "w"}}
			},
			wantErr: "backup",
		},
		{
			name: "backup parent cannot parent at all",
			mutate: func(nodes []Node, _ *Constraints, top *Topology) {
				// v has no upload budget, so it could never take over for P.
				top.Backups = []Backup{{Node: "u", Parent: "v"}}
			},
			wantErr: "backup",
		},
		{
			name:  "backup promotion would sink the promoted subtree past MaxDepth",
			extra: []Node{{Name: "w", UploadKbps: 0}},
			mutate: func(nodes []Node, c *Constraints, top *Topology) {
				// u gains a child (depth 3, still legal). Its backup v sits at depth
				// 2, so promoting u under v would put u at 3 and w at 4.
				nodes[3].UploadKbps = 4000 // u now has a child of its own
				nodes[4].UploadKbps = 4000 // v must be able to parent at all
				top.Edges = append(top.Edges, Edge{Parent: "u", Child: "w"})
				top.Backups = []Backup{{Node: "u", Parent: "v"}}
			},
			wantErr: "backup",
		},
		{
			name:  "backup fan-in exceeds the overshoot allowance",
			extra: []Node{{Name: "w", UploadKbps: 0}, {Name: "x", UploadKbps: 0}},
			mutate: func(nodes []Node, c *Constraints, top *Topology) {
				// Q has capacity 2 and one child (v), so it may back up at most one
				// more node beyond its budget; three backups is two too many.
				top.Edges = append(top.Edges, Edge{Parent: "P", Child: "w"}, Edge{Parent: "P", Child: "x"})
				top.Backups = []Backup{
					{Node: "u", Parent: "Q"},
					{Node: "w", Parent: "Q"},
					{Node: "x", Parent: "Q"},
				}
			},
			wantErr: "backup",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nodes, cons, topo := oracleFleet()
			nodes = append(nodes, tt.extra...)
			tt.mutate(nodes, &cons, topo)
			err := Validate(topo, nodes, cons)
			if err == nil {
				t.Fatalf("Validate accepted a broken tree (%s)", tt.name)
			}
			if !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(tt.wantErr)) {
				t.Errorf("Validate error = %q, want it to mention %q", err, tt.wantErr)
			}
		})
	}
}

// TestValidateLocalRepair drives the churn oracle: it answers "was this change
// minimal?", which is a property of a TRANSITION, and it is the only reason "local
// repair" is a testable claim rather than an assertion in a doc.
func TestValidateLocalRepair(t *testing.T) {
	// prev: R→a, R→b, a→c, a→d.
	prev := &Topology{Epoch: 1, Rev: 1, Root: "R", Edges: []Edge{
		{Parent: "R", Child: "a"},
		{Parent: "R", Child: "b"},
		{Parent: "a", Child: "c"},
		{Parent: "a", Child: "d"},
	}}

	tests := []struct {
		name     string
		next     *Topology
		gone     []string
		joined   []string
		promoted []string
		wantErr  string
	}{
		{
			name: "no change at all is trivially minimal",
			next: &Topology{Epoch: 1, Rev: 2, Root: "R", Edges: prev.Edges},
		},
		{
			name: "a pure join moves nobody",
			next: &Topology{Epoch: 1, Rev: 2, Root: "R", Edges: []Edge{
				{Parent: "R", Child: "a"}, {Parent: "R", Child: "b"},
				{Parent: "a", Child: "c"}, {Parent: "a", Child: "d"},
				{Parent: "b", Child: "n"},
			}},
			joined: []string{"n"},
		},
		{
			name: "orphans of a departed relay may move; nobody else",
			next: &Topology{Epoch: 1, Rev: 2, Root: "R", Edges: []Edge{
				{Parent: "R", Child: "b"},
				{Parent: "b", Child: "c"},
				{Parent: "b", Child: "d"},
			}},
			gone: []string{"a"},
		},
		{
			name: "a gratuitous re-parent is caught",
			next: &Topology{Epoch: 1, Rev: 2, Root: "R", Edges: []Edge{
				{Parent: "R", Child: "a"}, {Parent: "R", Child: "b"},
				{Parent: "a", Child: "c"},
				{Parent: "b", Child: "d"}, // d moved for no reason: a still has room
			}},
			wantErr: "d",
		},
		{
			name: "a newcomer must never displace an incumbent",
			next: &Topology{Epoch: 1, Rev: 2, Root: "R", Edges: []Edge{
				{Parent: "R", Child: "a"}, {Parent: "R", Child: "b"},
				{Parent: "a", Child: "c"},
				{Parent: "a", Child: "n"}, // n took d's slot
				{Parent: "b", Child: "d"},
			}},
			joined:  []string{"n"},
			wantErr: "d",
		},
		{
			name: "a departed node must not survive into the new tree",
			next: &Topology{Epoch: 1, Rev: 2, Root: "R", Edges: prev.Edges},
			gone: []string{"a"},
			// "a" is still there.
			wantErr: "a",
		},
		{
			name: "a self-promoted node's move is ratified, not flagged",
			next: &Topology{Epoch: 1, Rev: 2, Root: "R", Edges: []Edge{
				{Parent: "R", Child: "a"}, {Parent: "R", Child: "b"},
				{Parent: "a", Child: "c"},
				{Parent: "b", Child: "d"}, // d promoted its backup after its EDGE to a failed
			}},
			promoted: []string{"d"},
		},
		{
			name: "a re-root is allowed when the old root is gone",
			next: &Topology{Epoch: 1, Rev: 2, Root: "a", Edges: []Edge{
				{Parent: "a", Child: "b"},
				{Parent: "a", Child: "c"},
				{Parent: "a", Child: "d"},
			}},
			gone: []string{"R"},
		},
		{
			name: "a re-root while the old root survives is not local repair",
			next: &Topology{Epoch: 1, Rev: 2, Root: "a", Edges: []Edge{
				{Parent: "a", Child: "R"},
				{Parent: "a", Child: "c"},
				{Parent: "a", Child: "d"},
				{Parent: "R", Child: "b"},
			}},
			wantErr: "root",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateLocalRepair(prev, tt.next, tt.gone, tt.joined, tt.promoted)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("ValidateLocalRepair: unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("ValidateLocalRepair accepted a non-minimal transition (%s)", tt.name)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error = %q, want it to mention %q", err, tt.wantErr)
			}
		})
	}

	// A nil prev is a caller bug, not a vacuous pass: an oracle that can be silently
	// skipped is not an oracle.
	if err := ValidateLocalRepair(nil, prev, nil, nil, nil); err == nil {
		t.Error("ValidateLocalRepair(nil prev) must fail loud rather than pass vacuously")
	}
	if err := ValidateLocalRepair(prev, nil, nil, nil, nil); err == nil {
		t.Error("ValidateLocalRepair(nil next) must fail loud")
	}
}
