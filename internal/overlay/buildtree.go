package overlay

import (
	"fmt"
	"math"
	"sort"
)

// NATType classifies a node's reachability, which bounds the role it can play. It
// lives in overlay because it is fundamentally a graph *constraint* (it decides who
// may be a parent), consumed by BuildTree; the metrics package reports it as
// telemetry about a node.
type NATType string

const (
	// NATDirect: STUN-reachable or public. Such a node has a usable direct path to
	// others and may act as a relay.
	NATDirect NATType = "direct"
	// NATRelayed: symmetric-NAT / CGNAT, reachable only via a TURN relay. Its path is
	// already indirect and its bandwidth constrained, so it is forced to be a LEAF —
	// never a parent. (This is the ROADMAP's "TURN-bound peers are leaves" rule.)
	NATRelayed NATType = "turn"
)

// Node is one participant as the tree builder sees it — the telemetry that shapes
// the tree, stripped of everything the algorithm doesn't need. Kept as a plain
// value so a test or the simnet harness can hand-write a fleet without any I/O.
type Node struct {
	// Name is the node's stable label (the tree's vocabulary).
	Name string
	// UploadKbps is the upload budget the node will spend forwarding others' media.
	// Divided by the per-stream cost, it becomes the node's child capacity — the
	// degree bound. This is the scarce resource the whole project is organised
	// around, so it is the first thing BuildTree reasons about.
	UploadKbps int
	// RTT maps another node's name → measured round-trip ms to it. It may be partial
	// or empty: pairwise RTT is a post-connection measurement, and BuildTree must
	// still produce a valid tree before any of it exists. A missing entry is treated
	// as "unknown" (worst-case), so the latency term simply doesn't bias attachment
	// until real numbers arrive.
	RTT map[string]float64
	// NAT bounds the node's role: NATRelayed forces it to a leaf.
	NAT NATType
}

// Constraints parameterise a build: which node roots the tree, how deep it may get,
// and the per-child upload cost that turns each node's budget into a degree bound.
type Constraints struct {
	// Root is the node at depth 0 — the tree's source/relay origin. Required.
	Root string
	// MaxDepth is the maximum number of hops from the root to any leaf (1 = a star:
	// root → leaves; 2 = root → relay → leaf). Latency accumulates per hop, so this
	// is kept small; the media layer targets ≤ 2. Must be ≥ 1.
	MaxDepth int
	// StreamKbps is the upload cost of forwarding one stream to one child. A node's
	// child capacity is floor(UploadKbps / StreamKbps). Must be > 0.
	StreamKbps int
}

// unknownRTT is the score used when a candidate parent's RTT is unmeasured. It must
// dominate any real RTT so measured parents always win over unknown ones, while
// still letting the deterministic tiebreak (fewer children, then name) choose among
// equally-unknown candidates — which is exactly the load-balancing behaviour we
// want on a LAN where no RTT has been measured yet.
const unknownRTT = math.MaxFloat64

// BuildTree computes a degree-bounded, depth-limited, min-latency relay tree by a
// greedy heuristic, and is the pure heart of the control plane: no sockets, no
// clock, no randomness — the same inputs always yield the same tree, which is what
// makes it unit- and simulation-testable.
//
// Why greedy and not optimal: the target — a degree-constrained, depth-limited,
// minimum-latency spanning tree — is NP-hard (it generalises degree-bounded
// minimum spanning tree). For a handful of home peers reconfigured on every join or
// leave, an optimal solve is both unnecessary and too slow to run in the control
// loop. The greedy rule ("attach each node to the lowest-latency relay that still
// has spare upload and keeps within the depth bound") runs in well under a
// millisecond and produces shallow, balanced trees that are easily good enough. The
// trade-off is stated so a future maintainer doesn't mistake the heuristic for a
// bug.
//
// The algorithm:
//  1. Seed the tree with the root at depth 0.
//  2. Consider the remaining nodes strongest-upload-first, so high-capacity nodes
//     attach near the root and naturally become the relays that carry the tree.
//  3. Attach each node to the best currently-attached parent that can take it: one
//     that is not itself forced to a leaf, is above the depth bound, and has spare
//     child capacity — choosing, among those, minimum RTT, then fewest children
//     (balance), then name (determinism).
//  4. If no parent can take a node, fail loud: the fleet is over-constrained (too
//     little aggregate upload, or a depth bound too shallow) and silently dropping
//     the node would be the "mysterious missing stream" we refuse to ship.
func BuildTree(nodes []Node, c Constraints) (*Topology, error) {
	if c.StreamKbps <= 0 {
		return nil, fmt.Errorf("build tree: StreamKbps must be > 0, got %d", c.StreamKbps)
	}
	if c.MaxDepth < 1 {
		return nil, fmt.Errorf("build tree: MaxDepth must be ≥ 1, got %d", c.MaxDepth)
	}
	if c.Root == "" {
		return nil, fmt.Errorf("build tree: Root is required")
	}
	if len(nodes) == 0 {
		return nil, fmt.Errorf("build tree: no nodes")
	}

	byName := make(map[string]Node, len(nodes))
	for _, n := range nodes {
		if n.Name == "" {
			return nil, fmt.Errorf("build tree: a node has an empty name")
		}
		if _, dup := byName[n.Name]; dup {
			return nil, fmt.Errorf("build tree: duplicate node name %q", n.Name)
		}
		byName[n.Name] = n
	}
	root, ok := byName[c.Root]
	if !ok {
		return nil, fmt.Errorf("build tree: root %q is not among the nodes", c.Root)
	}
	// The root must be able to parent — unless it is the only node. A TURN-bound root
	// with peers to serve is unsatisfiable (nobody could attach to it), so reject it
	// early with a clear reason rather than failing per-node below.
	if len(nodes) > 1 && capacityOf(root, c) == 0 {
		return nil, fmt.Errorf("build tree: root %q cannot serve any children (upload %d kbit/s < stream cost %d, or TURN-bound)",
			root.Name, root.UploadKbps, c.StreamKbps)
	}

	// capacity/depth/children track the growing tree. attachedOrder gives the
	// candidate parents a deterministic iteration order (root first, then in the
	// order nodes attached), so parent selection never depends on Go's randomised
	// map iteration.
	depth := map[string]int{root.Name: 0}
	children := map[string]int{}
	attachedOrder := []string{root.Name}

	// Consider nodes strongest-first (upload desc, then name for determinism).
	others := make([]Node, 0, len(nodes)-1)
	for _, n := range nodes {
		if n.Name != root.Name {
			others = append(others, n)
		}
	}
	sort.Slice(others, func(i, j int) bool {
		if others[i].UploadKbps != others[j].UploadKbps {
			return others[i].UploadKbps > others[j].UploadKbps
		}
		return others[i].Name < others[j].Name
	})

	var edges []Edge
	for _, u := range others {
		parent, err := bestParent(u, byName, c, depth, children, attachedOrder)
		if err != nil {
			return nil, err
		}
		edges = append(edges, Edge{Parent: parent, Child: u.Name})
		depth[u.Name] = depth[parent] + 1
		children[parent]++
		attachedOrder = append(attachedOrder, u.Name)
	}
	return &Topology{Edges: edges}, nil
}

// bestParent picks the parent for u among the already-attached nodes, or errors if
// none can take it. Selection order: eligible (non-leaf, within depth, spare
// capacity), then minimum RTT, then fewest children (balance), then name.
func bestParent(u Node, byName map[string]Node, c Constraints, depth, children map[string]int, attached []string) (string, error) {
	best := ""
	bestRTT := math.Inf(1)
	bestChildren := math.MaxInt

	for _, name := range attached {
		p := byName[name]
		if capacityOf(p, c) == 0 {
			continue // TURN-bound or too little upload to parent anyone
		}
		if depth[name] >= c.MaxDepth {
			continue // a child here would exceed the depth bound
		}
		if children[name] >= capacityOf(p, c) {
			continue // degree bound reached
		}
		rtt := unknownRTT
		if v, ok := u.RTT[name]; ok {
			rtt = v
		}
		// Total, deterministic comparison: RTT, then load, then name.
		better := rtt < bestRTT ||
			(rtt == bestRTT && children[name] < bestChildren) ||
			(rtt == bestRTT && children[name] == bestChildren && (best == "" || name < best))
		if best == "" || better {
			best, bestRTT, bestChildren = name, rtt, children[name]
		}
	}
	if best == "" {
		return "", fmt.Errorf("build tree: cannot attach %q — no relay has spare upload within depth %d (fleet is over-constrained)", u.Name, c.MaxDepth)
	}
	return best, nil
}

// PickRoot chooses a sensible root for BuildTree: the highest-upload node that can
// actually parent (not TURN-bound), ties broken by name for determinism. BuildTree
// takes an explicit root because the caller may have a policy of its own; this is
// the default policy the coordinator and the simnet harness share so they do not
// each reinvent it. Returns "" when no node can be a relay for the others (all
// TURN-bound with more than one node), which the caller treats as "unbuildable"; a
// lone TURN node is returned as-is, since a one-node tree is trivially valid.
func PickRoot(nodes []Node) string {
	best := ""
	bestUp := -1
	for _, n := range nodes {
		if n.NAT == NATRelayed {
			continue // a TURN-bound node cannot root a tree of others
		}
		if n.UploadKbps > bestUp || (n.UploadKbps == bestUp && (best == "" || n.Name < best)) {
			best, bestUp = n.Name, n.UploadKbps
		}
	}
	if best == "" && len(nodes) == 1 {
		return nodes[0].Name
	}
	return best
}

// capacityOf is how many children a node may serve: its upload budget divided by
// the per-stream cost, or 0 if it is TURN-bound (forced leaf). Computed on demand
// because it depends only on immutable node fields plus the constraints.
func capacityOf(n Node, c Constraints) int {
	if n.NAT == NATRelayed {
		return 0
	}
	if c.StreamKbps <= 0 {
		return 0
	}
	return n.UploadKbps / c.StreamKbps
}
