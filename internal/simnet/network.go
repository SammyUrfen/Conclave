package simnet

import (
	"fmt"
	"math/rand"

	"github.com/SammyUrfen/conclave/internal/overlay"
)

// Network is a deterministic, media-free model of a room's participants, built to
// test the control plane the way the ROADMAP intends: feed a fleet (with injectable
// upload caps, NAT classes, and pairwise latencies) into the REAL overlay.BuildTree
// and assert overlay.Validate — no pion, no cameras, no wall-clock flakiness. Churn
// is just Add/Remove between builds. Because BuildTree is pure and Network keeps a
// deterministic insertion order, a whole scenario replays identically from a seed,
// which is the only sane way to develop the failover and election phases (5–6).
type Network struct {
	// order is insertion order; order[0] is the "pinned" seed that RemoveRandom
	// never evicts, so churn scenarios can keep a guaranteed relay present.
	order []string
	nodes map[string]overlay.Node
	// rtt is a symmetric pairwise latency matrix (ms); absent pairs are "unknown",
	// which BuildTree tolerates. It is what makes latencies "injectable".
	rtt map[string]map[string]float64
}

// New returns an empty Network.
func New() *Network {
	return &Network{
		nodes: make(map[string]overlay.Node),
		rtt:   make(map[string]map[string]float64),
	}
}

// Add inserts a node (or replaces one of the same name), recording insertion order
// the first time a name is seen.
func (n *Network) Add(node overlay.Node) {
	if _, ok := n.nodes[node.Name]; !ok {
		n.order = append(n.order, node.Name)
	}
	n.nodes[node.Name] = node
}

// SetRTT records a symmetric round-trip latency (ms) between a and b.
func (n *Network) SetRTT(a, b string, ms float64) {
	set := func(x, y string) {
		if n.rtt[x] == nil {
			n.rtt[x] = make(map[string]float64)
		}
		n.rtt[x][y] = ms
	}
	set(a, b)
	set(b, a)
}

// Remove deletes a node and every RTT entry mentioning it. A no-op if absent.
func (n *Network) Remove(name string) {
	if _, ok := n.nodes[name]; !ok {
		return
	}
	delete(n.nodes, name)
	delete(n.rtt, name)
	for _, row := range n.rtt {
		delete(row, name)
	}
	for i, nm := range n.order {
		if nm == name {
			n.order = append(n.order[:i], n.order[i+1:]...)
			break
		}
	}
}

// RemoveRandom evicts a random non-pinned node (never order[0]) and returns its
// name, or "" if only the pinned seed remains. Pinning the seed keeps a strong
// relay present so a churn scenario exercises leaf/relay churn without collapsing
// into an unbuildable fleet — that failure mode is the property test's job.
func (n *Network) RemoveRandom(rng *rand.Rand) string {
	if len(n.order) <= 1 {
		return ""
	}
	name := n.order[1+rng.Intn(len(n.order)-1)]
	n.Remove(name)
	return name
}

// Len reports the current node count.
func (n *Network) Len() int { return len(n.nodes) }

// OverlayNodes projects the fleet into BuildTree's input in deterministic insertion
// order, attaching each node's RTT row. Iterating order (not the map) is what keeps
// the projection — and thus the whole simulation — reproducible.
func (n *Network) OverlayNodes() []overlay.Node {
	out := make([]overlay.Node, 0, len(n.order))
	for _, name := range n.order {
		node := n.nodes[name]
		if row := n.rtt[name]; len(row) > 0 {
			cp := make(map[string]float64, len(row))
			for k, v := range row {
				cp[k] = v
			}
			node.RTT = cp
		}
		out = append(out, node)
	}
	return out
}

// Build selects a root with overlay.PickRoot and runs BuildTree, returning the tree
// and the constraints (with Root filled) so the caller can pass both straight to
// overlay.Validate. It errors when no node can root the tree.
func (n *Network) Build(cons overlay.Constraints) (*overlay.Topology, overlay.Constraints, error) {
	nodes := n.OverlayNodes()
	root := overlay.PickRoot(nodes)
	if root == "" {
		return nil, cons, fmt.Errorf("simnet: no relay-capable node among %d", len(nodes))
	}
	cons.Root = root
	topo, err := overlay.BuildTree(nodes, cons)
	return topo, cons, err
}

// Random deterministically builds a fleet of n nodes with varied upload budgets and
// an occasional TURN-bound (forced-leaf) node. Node 0 is given a strong budget so
// it can root a star — keeping most fleets buildable while the weaker nodes still
// force multi-level trees when they must relay. Fleets that are genuinely
// over-constrained are the interesting edge the property test tolerates as an
// honest BuildTree error.
func Random(rng *rand.Rand, n int) *Network {
	net := New()
	uploads := []int{0, 1000, 2000, 4000}
	for i := 0; i < n; i++ {
		up := uploads[rng.Intn(len(uploads))]
		nat := overlay.NATDirect
		if i != 0 && rng.Intn(6) == 0 { // ~1 in 6 TURN-bound, but never the seed
			nat = overlay.NATRelayed
		}
		if i == 0 {
			up = 8000 // strong seed so a star is usually available as a fallback
		}
		net.Add(overlay.Node{Name: fmt.Sprintf("n%d", i), UploadKbps: up, NAT: nat})
	}
	return net
}
