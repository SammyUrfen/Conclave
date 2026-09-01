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

	// The failure-injection state (see injection.go). It is kept on the Network
	// rather than on the Scenario because these are facts about the WORLD, not
	// about one script: a coordinator adapter, a scenario step, and an assertion
	// must all be able to ask the same question and get the same answer.
	departures []Departure
	partitions map[string]map[string]bool
	isolated   map[string]bool
	degraded   map[string]map[string]linkQuality
}

// New returns an empty Network.
func New() *Network {
	return &Network{
		nodes:      make(map[string]overlay.Node),
		rtt:        make(map[string]map[string]float64),
		partitions: make(map[string]map[string]bool),
		isolated:   make(map[string]bool),
		degraded:   make(map[string]map[string]linkQuality),
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
//
// This is the ONE place the injected faults meet the builder, which is why it is
// worth naming exactly what does and does not cross:
//
//   - Degrade's ROUND-TRIP overrides the measured baseline, so a degraded link
//     genuinely re-parents a child on latency alone.
//   - Degrade's LOSS crosses twice: as Node.LossPct, which derates the node's usable
//     upload (retransmits really do consume the budget a relay would spend on
//     children), and — once it reaches ImpairedLossPct — as Node.Impaired, which
//     strips the node's incumbency protection and disqualifies it from taking new
//     children, as root, and as a backup parent. The second is what makes a
//     sustained-degradation scenario MOVE an impaired relay's children instead of
//     producing a byte-identical tree.
//   - Partition does not cross at all — the overlay model has no way to say "these
//     two cannot be neighbours", and faking it through the RTT matrix would read as
//     "unknown", which the builder treats as a load-balancing tie and which could
//     make the unreachable parent MORE attractive. See Partition.
//
// The harness sets Impaired directly rather than modelling the dwell timer, because
// the dwell is the coordinator's job (it is what turns a stream of bad samples into
// the one-bit decision) and simnet's job is to state the decision's OUTCOME so the
// graph layer can be tested against it.
func (n *Network) OverlayNodes() []overlay.Node {
	out := make([]overlay.Node, 0, len(n.order))
	for _, name := range n.order {
		node := n.nodes[name]
		row := n.rttRow(name)
		if len(row) > 0 {
			node.RTT = row
		}
		node.LossPct = n.NodeLossPct(name)
		node.Impaired = node.Impaired || n.Impaired(name)
		out = append(out, node)
	}
	return out
}

// rttRow returns name's measured latencies with any injected degradation applied on
// top, as a fresh map so a caller cannot mutate the model by editing a projection.
func (n *Network) rttRow(name string) map[string]float64 {
	base, inj := n.rtt[name], n.degraded[name]
	if len(base) == 0 && len(inj) == 0 {
		return nil
	}
	row := make(map[string]float64, len(base)+len(inj))
	for k, v := range base {
		row[k] = v
	}
	for k, q := range inj {
		row[k] = q.rttMs
	}
	return row
}

// Build runs one rebuild against the REAL overlay: it fills in the root and the
// fencing counters (see nextConstraints), runs BuildTree over prev, and returns the
// tree together with the constraints actually used, so the caller can pass both
// straight to overlay.Validate without re-deriving what was asked for.
//
// prev is the tree the build should PREFER — the coordinator's working copy, not
// necessarily the last published one. Passing it is what makes the rebuild
// stability-preserving; passing nil is the memoryless build, which is right for a
// first build and for a test isolating attachment from history.
//
// It errors when no node can root the tree, and propagates BuildTree's error for an
// over-constrained fleet — an honest outcome a churn scenario handles by holding the
// previous tree, exactly as the coordinator does.
func (n *Network) Build(prev *overlay.Topology, cons overlay.Constraints) (*overlay.Topology, overlay.Constraints, error) {
	nodes := n.OverlayNodes()
	cons, ok := nextConstraints(cons, nodes, prev)
	if !ok {
		return nil, cons, fmt.Errorf("simnet: no relay-capable node among %d", len(nodes))
	}
	topo, err := buildTree(nodes, prev, cons)
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
