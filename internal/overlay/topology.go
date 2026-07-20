package overlay

import (
	"encoding/json"
	"fmt"
	"os"
)

// Topology is a relay tree: a set of directed parent→child edges over peers named
// by stable label. It is the single currency the media layer speaks — a peer opens
// a session only to its topology neighbours and forwards among them — and it is
// what overlay.BuildTree produces and the coordinator pushes to peers.
//
// It is expressed in NAMES, not the server-assigned runtime ids, because a tree is
// reasoned about independently of who is connected right now: Phase 3 authored one
// in a file before anyone joined, and Phase 4's coordinator computes one over the
// members' declared names. The media Router resolves names to ids at run time.
//
// A Topology carries no pion/WebRTC types on purpose — keeping this package pure
// (no sockets, no clock, no media) is what lets BuildTree and the whole control
// plane be unit- and simulation-tested in milliseconds. media imports overlay, not
// the other way around.
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

// LoadTopology reads and validates a hand-authored tree file (the Phase 3 static
// path; Phase 4 computes topologies instead of loading them). It fails loud on
// malformed JSON, empty edges, an edge naming a peer as its own parent, or a child
// with two parents — bad config should stop a peer at startup, not surface as a
// mysterious missing stream later.
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
	if err := t.validateTree(); err != nil {
		return nil, fmt.Errorf("topology %q: %w", path, err)
	}
	return &t, nil
}

// validateTree enforces the structural invariants a file may violate but BuildTree
// guarantees by construction: every edge is well-formed and no child has two
// parents. The load-bearing one is single-parent — ParentOf/NeighborsOf assume it,
// and a node with two parents makes NeighborsOf asymmetric (one side opens a
// session the other never does, and the offer is silently dropped). That is exactly
// the "mysterious missing stream" LoadTopology promises to prevent, so a
// re-parenting copy/paste slip must fail loud, at startup.
func (t *Topology) validateTree() error {
	parentOf := make(map[string]string, len(t.Edges))
	for i, e := range t.Edges {
		if e.Parent == "" || e.Child == "" {
			return fmt.Errorf("edge %d: parent and child are required", i)
		}
		if e.Parent == e.Child {
			return fmt.Errorf("edge %d: %q is its own parent", i, e.Parent)
		}
		if prev, ok := parentOf[e.Child]; ok {
			return fmt.Errorf("child %q has two parents (%q and %q); a tree gives each node one", e.Child, prev, e.Parent)
		}
		parentOf[e.Child] = e.Parent
	}
	return nil
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

// ChildrenOf returns the downstream peers of name, in edge order.
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

// Nodes returns every distinct peer named anywhere in the tree, in first-seen edge
// order (parents before the children that first introduce them). Handy for the
// Router and coordinator to enumerate a topology's participants deterministically.
func (t *Topology) Nodes() []string {
	seen := make(map[string]bool, len(t.Edges)*2)
	var out []string
	add := func(n string) {
		if n != "" && !seen[n] {
			seen[n] = true
			out = append(out, n)
		}
	}
	for _, e := range t.Edges {
		add(e.Parent)
		add(e.Child)
	}
	return out
}

// Offers reports whether self should be the OFFERER on its edge with peer. Rule:
// the relay offers on every one of its edges, because only a fresh offer can add
// the forwarded m-lines it needs, and an answer cannot. When exactly one endpoint
// is a relay, that endpoint offers — glare-free by construction while no two
// relays are adjacent (true for a single-relay tree). If both endpoints are relays
// (a deeper tree), fall back to a deterministic name tie-break so the rule still
// yields exactly one offerer. This is a structural property of the tree — who
// initiates on an edge — so it lives with the tree, not in the media layer.
func (t *Topology) Offers(self, peer string) bool {
	selfRelay, peerRelay := t.IsRelay(self), t.IsRelay(peer)
	if selfRelay != peerRelay {
		return selfRelay
	}
	return self > peer
}
