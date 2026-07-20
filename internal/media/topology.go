package media

import (
	"encoding/json"
	"fmt"
	"os"
)

// Topology is a hardcoded relay tree, authored ahead of time and shared by every
// peer. It is expressed in stable NAMES (not the server-assigned runtime ids),
// because the tree is written before anyone connects — the Router resolves names
// to ids at run time from the signaling roster.
//
// Phase 3 does not compute or optimise the tree (that is Phase 4's coordinator);
// it just reads this file and each peer plays the role the edges give it. Depth is
// kept shallow (≤ 2) by how the file is authored, not enforced here.
type Topology struct {
	// Edges are directed parent→child links. A peer's parent is its upstream
	// (toward the root); its children are downstream. A relay is any peer with at
	// least one child.
	Edges []Edge `json:"edges"`
}

// Edge is one parent→child link in the tree, named by peer label.
type Edge struct {
	Parent string `json:"parent"`
	Child  string `json:"child"`
}

// LoadTopology reads and validates a tree file. It fails loud on malformed JSON,
// empty edges, or an edge naming a peer as its own parent — bad config should stop
// a peer at startup, not surface as a mysterious missing stream later.
func LoadTopology(path string) (*Topology, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read topology %q: %w", path, err)
	}
	var t Topology
	if err := json.Unmarshal(data, &t); err != nil {
		return nil, fmt.Errorf("parse topology %q: %w", path, err)
	}
	if len(t.Edges) == 0 {
		return nil, fmt.Errorf("topology %q has no edges", path)
	}
	// Validate the tree invariant, not just individual edges. The load-bearing one
	// is single-parent: ParentOf/NeighborsOf assume each node has ≤1 parent, and a
	// node with two parents makes NeighborsOf asymmetric — one side would open a
	// session the other never does, and the offer would be silently dropped. That is
	// exactly the "mysterious missing stream" this loader promises to prevent, so a
	// re-parenting copy/paste slip must fail loud here, at startup.
	parentOf := make(map[string]string, len(t.Edges))
	for i, e := range t.Edges {
		if e.Parent == "" || e.Child == "" {
			return nil, fmt.Errorf("topology %q edge %d: parent and child are required", path, i)
		}
		if e.Parent == e.Child {
			return nil, fmt.Errorf("topology %q edge %d: %q is its own parent", path, i, e.Parent)
		}
		if prev, ok := parentOf[e.Child]; ok {
			return nil, fmt.Errorf("topology %q: child %q has two parents (%q and %q); a tree gives each node one", path, e.Child, prev, e.Parent)
		}
		parentOf[e.Child] = e.Parent
	}
	return &t, nil
}

// ParentOf returns the upstream peer of name, or "" if name is the root (has no
// parent). A well-formed tree gives every non-root exactly one parent; if the file
// lists more, the first is returned.
func (t *Topology) ParentOf(name string) string {
	for _, e := range t.Edges {
		if e.Child == name {
			return e.Parent
		}
	}
	return ""
}

// ChildrenOf returns the downstream peers of name, in file order.
func (t *Topology) ChildrenOf(name string) []string {
	var kids []string
	for _, e := range t.Edges {
		if e.Parent == name {
			kids = append(kids, e.Child)
		}
	}
	return kids
}

// NeighborsOf returns every peer directly linked to name — its parent (if any)
// plus its children. These are exactly the peers name opens a PeerConnection to in
// tree mode; everyone else is reached through the tree, not directly. That is the
// whole point of the relay: a leaf connects only to its parent, not to N−1 peers.
func (t *Topology) NeighborsOf(name string) []string {
	neighbors := t.ChildrenOf(name)
	if p := t.ParentOf(name); p != "" {
		neighbors = append(neighbors, p)
	}
	return neighbors
}

// IsRelay reports whether name forwards media for others — i.e. it has at least
// one child. Leaves and a childless root are not relays.
func (t *Topology) IsRelay(name string) bool {
	for _, e := range t.Edges {
		if e.Parent == name {
			return true
		}
	}
	return false
}

// Offers reports whether self should be the OFFERER on its edge with peer. Rule:
// the relay offers on every one of its edges, because only a fresh offer can add
// the forwarded m-lines it needs, and an answer cannot. When exactly one endpoint
// is a relay, that endpoint offers — glare-free by construction while no two
// relays are adjacent (true for a single-relay Phase-3 tree). If both endpoints
// are relays (a deeper tree, Phase 5+), fall back to a deterministic name
// tie-break so the rule still yields exactly one offerer.
func (t *Topology) Offers(self, peer string) bool {
	selfRelay, peerRelay := t.IsRelay(self), t.IsRelay(peer)
	if selfRelay != peerRelay {
		return selfRelay
	}
	return self > peer
}
