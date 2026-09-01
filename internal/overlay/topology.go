package overlay

import (
	"encoding/json"
	"fmt"
	"os"
)

// Topology is a relay tree (a "subnet"): a set of directed parent→child edges over
// peers named by stable label, stamped with the control-plane term that authorised
// it. It is the single currency the media layer speaks — a peer opens a session
// only to its topology neighbours and forwards among them — and it is what
// overlay.BuildTree produces and the coordinator pushes to peers.
//
// It is expressed in NAMES, not the server-assigned runtime ids, because a tree is
// reasoned about independently of who is connected right now: Phase 3 authored one
// in a file before anyone joined, and the coordinator computes one over the
// members' declared names. The media Router resolves names to ids at run time.
//
// (Epoch, Rev) is the fencing token, ordered lexicographically. Epoch is the
// arbiter's coordinator term and changes only on a coordinator handover; Rev is the
// coordinator's own revision counter within its term. Two fields rather than one
// because they have two different single writers: only the arbiter may raise Epoch,
// only the sitting coordinator may raise Rev. Collapsing them into one counter
// would mean a coordinator could mint a number that fences the arbiter's own
// announcement — the precise failure the epoch exists to prevent. This is Raft's
// (term, index) split, for the same reason.
//
// A Topology carries no pion/WebRTC types on purpose — keeping this package pure
// (no sockets, no clock, no media) is what lets BuildTree and the whole control
// plane be unit- and simulation-tested in milliseconds. media imports overlay, not
// the other way around.
type Topology struct {
	// Epoch is the arbiter-minted coordinator term this tree was computed under.
	// Always ≥ 1 in a computed tree; 0 only in a not-yet-stamped zero value.
	Epoch uint64 `json:"epoch"`
	// Rev is the coordinator's revision within Epoch, starting at 1 and
	// incrementing on every published tree. Resets to 1 when Epoch advances.
	Rev uint64 `json:"rev"`
	// Root is the tree's source node — the unique node with no parent. Stored
	// explicitly (rather than derived by scanning Edges) so a peer, the media
	// Router, and the dashboard can each answer "am I / who is the root" in O(1)
	// without re-deriving it three different ways. It is redundant, and redundancy
	// is a chance to disagree, so Validate treats disagreement with Edges as a hard
	// error — the redundancy can never survive into a published tree.
	Root string `json:"root"`
	// Edges are directed parent→child links, IN TOPOLOGICAL (ATTACH) ORDER: for
	// every i, Edges[i].Parent is either Root or appears as some Edges[j].Child
	// with j < i. This ordering is an INVARIANT, not an accident — the
	// stability-preserving rebuild replays it directly to process incumbents
	// parents-first, and Validate checks it.
	Edges []Edge `json:"edges"`
	// Backups are the precomputed warm secondary parents, at most one per node. A
	// node absent from this slice has no backup and must wait for a coordinator
	// push on parent failure — which is the correct and deliberate answer for the
	// root's own children, whose only "backup" would be inside the subtree the
	// failure destroys.
	//
	// A slice rather than a map: both marshal reproducibly, but the slice preserves
	// the builder's assignment order (so a diff between two published trees reads
	// in the dashboard event stream) and lets Validate report a failing index the
	// way it already does for Edges.
	Backups []Backup `json:"backups,omitempty"`
}

// Edge is one parent→child link in the tree, named by peer label.
type Edge struct {
	Parent string `json:"parent"`
	Child  string `json:"child"`
}

// Backup assigns Node a warm secondary parent to fail over to when its primary
// parent is lost, without waiting for a coordinator recompute. Promoting it is a
// peer-local decision the coordinator later ratifies.
type Backup struct {
	Node   string `json:"node"`
	Parent string `json:"parent"`
}

// StaticEpoch is the epoch stamped on a hand-authored topology file. It is
// deliberately the MAXIMUM uint64 rather than 1: a static-tree peer is not
// participating in the election plane at all, and stamping it max means no
// coordinator push can ever supersede the operator's explicit file (Supersedes
// returns false for every real epoch). A peer run with -topology is pinned, by
// definition. Using 1 instead would let a coordinator on the same server silently
// overwrite the operator's tree, which is a surprising and hard-to-debug
// interaction between two modes that are supposed to be independent.
const StaticEpoch uint64 = ^uint64(0)

// LoadTopology reads and validates a hand-authored tree file (the Phase 3 static
// path; from Phase 4 the coordinator computes topologies instead of loading them).
// It fails loud on malformed JSON, empty edges, an edge naming a peer as its own
// parent, a child with two parents, or a file whose root cannot be determined — bad
// config should stop a peer at startup, not surface as a mysterious missing stream
// later.
//
// Phase 3 files carry no epoch, rev, or root, so those are filled in here: the file
// is stamped StaticEpoch/rev 1 and its root derived from the edges. Backups stay
// nil — a static tree has no failover, which is correct, since its whole premise is
// a hand-authored, unchanging tree.
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
	root, err := t.deriveRoot()
	if err != nil {
		return nil, fmt.Errorf("topology %q: %w", path, err)
	}
	if t.Root != "" && t.Root != root {
		return nil, fmt.Errorf("topology %q: declared root %q is not the tree's parentless node %q", path, t.Root, root)
	}
	t.Root = root
	if t.Epoch == 0 {
		t.Epoch = StaticEpoch
	}
	if t.Rev == 0 {
		t.Rev = 1
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

// deriveRoot returns the unique node that appears as a parent but never as a child.
// Anything else — no such node (a cycle) or several (a forest) — is not a tree, and
// guessing which one the operator meant is exactly the kind of silent repair this
// loader refuses to do.
func (t *Topology) deriveRoot() (string, error) {
	isChild := make(map[string]bool, len(t.Edges))
	for _, e := range t.Edges {
		isChild[e.Child] = true
	}
	var roots []string
	for _, n := range t.Nodes() { // Nodes() is slice-ordered, so roots is too
		if !isChild[n] {
			roots = append(roots, n)
		}
	}
	switch len(roots) {
	case 1:
		return roots[0], nil
	case 0:
		return "", fmt.Errorf("no root: every node has a parent (the edges form a cycle)")
	default:
		return "", fmt.Errorf("ambiguous root: %v are all parentless; a tree has exactly one", roots)
	}
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
//
// Note a one-node tree has no edges and therefore no Nodes: ask Root as well when
// enumerating a possibly-degenerate tree.
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

// BackupOf returns the precomputed secondary parent for name, or "" if it has none.
// "None" is a normal answer, not an error: the root's children have no legal backup
// by construction (see BuildTree), and a peer that finds none simply waits for a
// coordinator push instead of failing over locally.
func (t *Topology) BackupOf(name string) string {
	for _, b := range t.Backups {
		if b.Node == name {
			return b.Parent
		}
	}
	return ""
}

// Depth returns the hop distance from Root to name: 0 for the root itself, and the
// SENTINEL -1 when name is not attached to this tree — either because it is not
// mentioned at all, or because its parent chain does not reach Root (a malformed
// tree Validate would reject).
//
// The -1 sentinel rather than an (int, bool) pair because every caller uses the
// value in an arithmetic comparison against MaxDepth, and -1 is safe in all of
// them: an unattached node compares as "shallower than anything", which is exactly
// the answer that makes a caller's depth check refuse to grow the tree from a node
// it cannot place.
func (t *Topology) Depth(name string) int {
	if name == "" {
		return -1
	}
	if name == t.Root {
		return 0
	}
	d := 0
	cur := name
	// Bounded by the edge count: a malformed tree with a cycle must return the
	// sentinel, not spin forever.
	for i := 0; i <= len(t.Edges); i++ {
		p := t.ParentOf(cur)
		if p == "" {
			return -1 // a parentless node that is not Root: not attached
		}
		d++
		if p == t.Root {
			return d
		}
		cur = p
	}
	return -1
}

// Subtree returns name plus every node reachable downward from it, in BFS order
// (edge order within each level). It returns nil when name is not attached to the
// tree. Used by the backup-parent invariant and by local repair, both of which ask
// "what does this one failure take down with it?".
func (t *Topology) Subtree(name string) []string {
	if t.Depth(name) < 0 {
		return nil
	}
	out := []string{name}
	for i := 0; i < len(out); i++ {
		out = append(out, t.ChildrenOf(out[i])...)
	}
	return out
}

// Supersedes reports whether t is strictly newer than prev under the lexicographic
// (Epoch, Rev) order. A nil prev is superseded by anything.
//
// It is an ORDERING predicate and nothing more. It does NOT answer "may I accept
// this topology" — that is a separate authorization question, because only the
// arbiter may raise an epoch, so a peer must additionally check the sender against
// the coordinator announced for the epoch it currently believes in, and REJECT a
// topology carrying a higher epoch than that. Answering authorization here would
// require the arbiter's announcement, which overlay cannot see and must stay pure
// of; conflating the two would mean any actor could stamp a huge epoch and be
// universally adopted.
func (t *Topology) Supersedes(prev *Topology) bool {
	if prev == nil {
		return true
	}
	if t.Epoch != prev.Epoch {
		return t.Epoch > prev.Epoch
	}
	return t.Rev > prev.Rev
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
