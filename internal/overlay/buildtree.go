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
	// Impaired marks a node the control plane has classified as SUSTAINED-DEGRADED —
	// its degradation dwell timer fired after a full dwell of bad telemetry, so this
	// is an evidence-backed decision, not one bad sample.
	//
	// An impaired node KEEPS its existing children when there is nowhere better for
	// them (evicting them is the interruption we are trying to avoid), but it is
	// disqualified from taking NEW ones, disqualified as root, disqualified as a
	// backup parent, and — critically — it LOSES INCUMBENCY PROTECTION over the
	// children it has (rank 1 below). That last clause is the whole point: it is what
	// makes a sustained-degradation event produce a materially different tree instead
	// of a byte-identical one. Without it the dwell timer, its three constants, and
	// its flag are inert machinery that reads as a working safety property.
	Impaired bool
	// LossPct is the node's measured uplink packet loss, 0–100. It DERATES the node's
	// usable upload, because retransmits and repair traffic genuinely consume the
	// budget a relay would otherwise spend on children. See effectiveUploadKbps.
	LossPct float64
	// Provisional marks a node whose telemetry is an ASSUMED DEFAULT, not a
	// measurement — a peer that has joined but whose first metrics report has not
	// arrived. It is set by the coordinator's projection, which supplies the assumed
	// number; this flag records that the number is a guess.
	//
	// Why overlay needs to know: a provisional node may be ATTACHED (as a leaf,
	// which is what a 0-upload default already makes it) but may NEVER be chosen as
	// ROOT by PickRoot. Rooting on a guess is how the tree ends up re-rooting a
	// second later when the real number arrives — the most expensive reconfiguration
	// there is, triggered by nothing but WebSocket arrival order. Expressing it as a
	// plain value on Node keeps overlay pure: the package still has no idea what a
	// "report" is, only that this datum is soft.
	Provisional bool
}

// Constraints parameterise a build: which node roots the tree, how deep it may get,
// the per-child upload cost that turns each node's budget into a degree bound, the
// control-plane term the result is stamped with, and how hard the builder clings to
// the previous tree.
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
	// Epoch is stamped onto the result. The coordinator supplies the term it is
	// serving under; BuildTree never invents one. Must be ≥ 1.
	Epoch uint64
	// Rev is stamped onto the result. Must be ≥ 1 and, when prev is non-nil with the
	// same Epoch, must be > prev.Rev — BuildTree rejects a non-advancing revision
	// rather than silently emitting a tree no peer will accept.
	Rev uint64
	// StickinessMs is the RTT margin (ms) by which a challenger must beat a node's
	// INCUMBENT parent before the node is re-parented. It is the anti-thrash knob at
	// the graph layer: 0 makes BuildTree memoryless and purely greedy; a large value
	// pins the tree in place. See DefaultStickinessMs.
	StickinessMs float64
}

// DefaultStickinessMs is the re-parent margin used unless a caller overrides it.
//
// 25 ms is chosen against the ARCHITECTURE's own latency budget: ~150 ms one-way is
// "good" and ~400 ms is the ceiling, so a 25 ms improvement is ~6% of the usable
// range — small enough that trading a stream interruption for it is a bad deal,
// large enough that a genuinely closer relay (a LAN peer at 5 ms vs a WAN peer at
// 60 ms) still wins immediately. It is also comfortably above the sampling noise on
// a residential link, where consecutive RTT samples routinely differ by 5–15 ms.
const DefaultStickinessMs = 25.0

// RootChangeMarginKbps is how much more upload a challenger must advertise before
// it is worth re-rooting the entire subnet.
//
// 2000 kbit/s is exactly one stream at the default stream cost, i.e. the challenger
// must be able to serve at least one MORE child than the incumbent before the swap
// is even considered. That is the smallest difference that buys anything
// structural; anything less re-parents every peer in the meet to gain nothing.
// Deliberately an ABSOLUTE margin rather than a ratio: capacity here is integer
// children (floor(upload / streamKbps)), so a percentage margin would mean
// different things at different fleet sizes, while "one more child's worth" means
// the same thing everywhere.
const RootChangeMarginKbps = 2000

// BackupOvershootAllowance is how far a relay's COMMITTED load (its primary
// children plus every node naming it as a backup) may exceed its computed capacity.
//
// 1, because that is the exact overshoot the design is willing to pay for: capacity
// is deliberately NOT reserved for backups — at the scale this system runs (4–8
// peers on residential upload) capacity is already the binding constraint, and
// halving usable fan-out to insure against a failure that triggers one recompute is
// a bad trade. But "not reserved" must not mean "unbounded": with no allowance at
// all, every child of a dying relay could name the SAME backup and promote at once,
// oversubscribing it by |Subtree(P)|−1 rather than by one child. Spending capacity
// across backup assignment with an allowance of exactly 1 keeps the documented
// bound true: at the instant of a simultaneous failover a relay may serve at most
// one child more than its computed capacity, until the coordinator's recompute
// lands. Note this overshoot exists only in a peer's REALIZED state — it is never
// encoded in a Topology, so Validate still holds for everything this package emits.
const BackupOvershootAllowance = 1

// unknownRTT is the score used when a candidate parent's RTT is unmeasured. It must
// dominate any real RTT so measured parents always win over unknown ones, while
// still letting the deterministic tiebreak (fewer children, then name) choose among
// equally-unknown candidates — which is exactly the load-balancing behaviour we
// want on a LAN where no RTT has been measured yet.
const unknownRTT = math.MaxFloat64

// BuildTree computes a degree-bounded, depth-limited, min-latency relay tree by a
// greedy heuristic that PREFERS THE PREVIOUS TREE, and is the pure heart of the
// control plane: no sockets, no clock, no randomness — the same (nodes, prev, c)
// always yields the same tree, which is what makes it unit- and
// simulation-testable.
//
// Why greedy and not optimal: the target — a degree-constrained, depth-limited,
// minimum-latency spanning tree — is NP-hard (it generalises degree-bounded minimum
// spanning tree). For a handful of home peers reconfigured on every join or leave,
// an optimal solve is both unnecessary and too slow to run in the control loop. The
// greedy rule runs in well under a millisecond and produces shallow, balanced trees
// that are easily good enough. The trade-off is stated so a future maintainer
// doesn't mistake the heuristic for a bug.
//
// prev may be nil (the first build for a meet, or a coordinator that has not yet
// rebuilt its state). A non-nil prev makes the build stability-preserving, which is
// what "local repair — re-parent only the subtree" requires: a node's parent changes
// only when it HAS to, so a rebuild after one departure moves exactly the orphans
// and nobody else. That property is asserted by ValidateLocalRepair.
//
// The cost of that property, stated plainly: with prev supplied the result is a
// function of HISTORY, not just of the fleet. Two meets with identical members can
// legitimately hold different valid trees because they got there by different
// routes. Path-independence is exactly what was traded away to buy
// minimal-disruption rebuilds; the two cannot both hold.
//
// The algorithm:
//  1. Seed the tree with the root at depth 0.
//  2. Process incumbents first, in prev's edge order, then newcomers strongest
//     first — so a strong joiner cannot claim capacity ahead of a node that is
//     already using it (see processingOrder).
//  3. Attach each node to the best parent that can take it: incumbency first,
//     then minimum RTT, then fewest children, then name (see bestParent).
//  4. Assign warm backup parents over the finished tree (see assignBackups).
//  5. If no parent can take a node, fail loud: silently dropping the node would be
//     the "mysterious missing stream" we refuse to ship.
func BuildTree(nodes []Node, prev *Topology, c Constraints) (*Topology, error) {
	if c.StreamKbps <= 0 {
		return nil, fmt.Errorf("build tree: StreamKbps must be > 0, got %d", c.StreamKbps)
	}
	if c.MaxDepth < 1 {
		return nil, fmt.Errorf("build tree: MaxDepth must be ≥ 1, got %d", c.MaxDepth)
	}
	if c.Root == "" {
		return nil, fmt.Errorf("build tree: Root is required")
	}
	if c.Epoch < 1 {
		return nil, fmt.Errorf("build tree: Epoch must be ≥ 1 (the arbiter mints it), got %d", c.Epoch)
	}
	if c.Rev < 1 {
		return nil, fmt.Errorf("build tree: Rev must be ≥ 1, got %d", c.Rev)
	}
	if prev != nil && prev.Epoch == c.Epoch && c.Rev <= prev.Rev {
		return nil, fmt.Errorf("build tree: Rev %d does not advance on the previous tree's rev %d in epoch %d; no peer would accept it",
			c.Rev, prev.Rev, c.Epoch)
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
		return nil, fmt.Errorf("build tree: root %q cannot serve any children (%d kbit/s effective of %d advertised < stream cost %d, or TURN-bound)",
			root.Name, effectiveUploadKbps(root), root.UploadKbps, c.StreamKbps)
	}

	// capacity/depth/children track the growing tree. attachedOrder gives the
	// candidate parents a deterministic iteration order (root first, then in the
	// order nodes attached), so parent selection never depends on Go's randomised
	// map iteration.
	depth := map[string]int{root.Name: 0}
	children := map[string]int{}
	attachedOrder := []string{root.Name}

	edges := make([]Edge, 0, len(nodes)-1)
	for _, u := range processingOrder(nodes, byName, prev, root.Name) {
		parent, err := bestParent(u, byName, c, depth, children, attachedOrder, prev)
		if err != nil {
			return nil, err
		}
		edges = append(edges, Edge{Parent: parent, Child: u.Name})
		depth[u.Name] = depth[parent] + 1
		children[parent]++
		attachedOrder = append(attachedOrder, u.Name)
	}

	topo := &Topology{Epoch: c.Epoch, Rev: c.Rev, Root: root.Name, Edges: edges}
	topo.Backups = assignBackups(topo, byName, c, attachedOrder)
	return topo, nil
}

// processingOrder fixes the order nodes are attached in, which is the fix for the
// memoryless-rebuild defect. A purely upload-descending order (the Phase 4 rule)
// let a strong joiner claim a parent's capacity ahead of the incumbents already
// using it, re-parenting the world on every join. The order is:
//
//  1. prev's ROOT, if it is still present and is not the new root. It is neither an
//     incumbent (it has no parent in prev, so it appears nowhere in prev.Edges) nor
//     a newcomer, and prev.Edges implicitly starts at it — so it is placed at that
//     implicit position. Placing it here, rather than with the newcomers, is what
//     lets its former children keep it as their parent after a re-root instead of
//     being scattered because their parent had not been attached yet.
//  2. Incumbents, in prev.Edges order. That order is topological by invariant, so
//     parents are placed before their children — which is why the invariant is
//     load-bearing and not decoration.
//  3. Newcomers — everything left — sorted upload descending, then name ascending.
//
// With prev == nil steps 1 and 2 are empty and this degenerates exactly to the
// Phase 4 order.
func processingOrder(nodes []Node, byName map[string]Node, prev *Topology, root string) []Node {
	placed := map[string]bool{root: true}
	out := make([]Node, 0, len(nodes))
	take := func(name string) {
		if placed[name] {
			return
		}
		n, ok := byName[name]
		if !ok {
			return // named in prev but no longer in the fleet
		}
		placed[name] = true
		out = append(out, n)
	}

	if prev != nil {
		take(prev.Root)
		for _, e := range prev.Edges {
			take(e.Child)
		}
	}

	newcomers := make([]Node, 0, len(nodes))
	for _, n := range nodes { // slice order in, so the sort below is total and stable
		if !placed[n.Name] {
			newcomers = append(newcomers, n)
		}
	}
	sort.Slice(newcomers, func(i, j int) bool {
		if newcomers[i].UploadKbps != newcomers[j].UploadKbps {
			return newcomers[i].UploadKbps > newcomers[j].UploadKbps
		}
		return newcomers[i].Name < newcomers[j].Name
	})
	return append(out, newcomers...)
}

// bestParent picks the parent for u among the already-attached nodes, or errors if
// none can take it. The preference ordering is lexicographic and total:
//
//	rank 0  hard filters — attached, able to parent, within depth, spare capacity;
//	rank 1  INCUMBENCY — u's parent in prev wins unless a candidate beats it by
//	        more than c.StickinessMs of RTT;
//	rank 2  minimum RTT (unknown scores worst);
//	rank 3  fewest children (load balance — what makes attachment sane on a LAN
//	        where no RTT has been measured);
//	rank 4  name ascending (determinism; no ties survive).
//
// Rank 1 sits ABOVE RTT and ABOVE load and BELOW the hard constraints, and that
// placement is the whole design: re-parenting costs a visible stream interruption,
// so a 5 ms RTT win or a less-loaded sibling must not buy one, while an incumbent
// that is gone, TURN-bound, full, or too deep is simply not a candidate — stickiness
// must never produce an invalid tree.
//
// Rejected alternative: a scored objective (cost = α·rtt + β·churn + γ·load, pick
// the argmin). More expressive, and one knob would trade all three off smoothly. It
// lost on explainability: a lexicographic ordering answers "why is B parented to R?"
// by walking four rules, while a weighted sum requires reconstructing three floats
// and their weights — false precision over inputs this noisy.
func bestParent(u Node, byName map[string]Node, c Constraints, depth, children map[string]int, attached []string, prev *Topology) (string, error) {
	// Rank 0: the eligible set, in attach order.
	eligible := make([]string, 0, len(attached))
	for _, name := range attached {
		if name == u.Name {
			continue
		}
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
		eligible = append(eligible, name)
	}
	if len(eligible) == 0 {
		return "", fmt.Errorf("build tree: cannot attach %q — no relay has spare upload within depth %d (fleet is over-constrained%s)",
			u.Name, c.MaxDepth, stabilityHint(prev))
	}

	// Rank 0b: the impairment filter, and it is SOFT. Impaired candidates are dropped
	// only if at least one healthy candidate survives; if every candidate is impaired
	// they are all re-admitted, because a degraded parent beats no parent. Two passes
	// over the same set rather than a score, so "degradation is a preference,
	// disconnection is not" stays a structural fact instead of a weight.
	//
	// THIS RANK IS THE ONLY PLACE IMPAIRMENT IS EXPRESSED, and it produces BOTH
	// impairment consequences — read them as coming from here, because nothing else
	// implements them:
	//
	//  1. An impaired node takes no NEW children: it is not in the candidate set.
	//  2. An impaired node LOSES THE CHILDREN IT HAS — because eliminating it from the
	//     candidate set means its own incumbents cannot re-select it at rank 1 either.
	//     This is the mechanism by which a fired dwell timer becomes an actual
	//     re-parent, i.e. it is the entire answer to "the dwell must not be inert".
	//
	// Consequence 2 is enforced from outside as the IMPAIRED-INCUMBENT INVARIANT, a
	// black-box property of this function's output rather than a clause in rank 1:
	// impaired incumbent + an eligible non-impaired candidate ⇒ the child moves.
	// Weakening this elimination into a mere preference breaks that property test
	// immediately, which is the protection a restated clause in rank 1 could not give —
	// it was unreachable, because whenever it could have fired, this filter had already
	// removed the incumbent.
	//
	// healthyAvailable is computed once and consumed here alone. It also gates the
	// re-admission, which is what preserves incumbency in an all-impaired fleet: the
	// void exists to move children onto a HEALTHY relay, so with nowhere healthy to go
	// its premise is absent and only churn would remain — repeatedly, since impairment
	// is sustained by definition.
	healthy := make([]string, 0, len(eligible))
	for _, name := range eligible {
		if !byName[name].Impaired {
			healthy = append(healthy, name)
		}
	}
	if healthyAvailable := len(healthy) > 0; healthyAvailable {
		eligible = healthy
	}

	// Rank 1: incumbency. Only a materially closer parent breaks it. Rank 1 carries no
	// impairment clause of its own and does not need one: rank 0b has already removed
	// an impaired incumbent from `eligible` in exactly the case where one should not be
	// re-selected, so a clause here could never execute. See the invariant named above.
	if prev != nil {
		if incumbent := prev.ParentOf(u.Name); incumbent != "" && contains(eligible, incumbent) {
			incumbentRTT := rttTo(u, incumbent)
			beaten := false
			for _, q := range eligible {
				if q != incumbent && rttTo(u, q)+c.StickinessMs < incumbentRTT {
					beaten = true
					break
				}
			}
			if !beaten {
				return incumbent, nil
			}
		}
	}

	// Ranks 2–4.
	best, bestRTT, bestChildren := "", math.Inf(1), math.MaxInt
	for _, name := range eligible {
		rtt := rttTo(u, name)
		better := rtt < bestRTT ||
			(rtt == bestRTT && children[name] < bestChildren) ||
			(rtt == bestRTT && children[name] == bestChildren && name < best)
		if best == "" || better {
			best, bestRTT, bestChildren = name, rtt, children[name]
		}
	}
	if best == "" {
		// Unreachable while rank 0b re-admits impaired candidates, and asserted rather
		// than assumed: returning "" here would emit an edge with an empty parent, and
		// a malformed tree is far worse than a loud failure.
		return "", fmt.Errorf("build tree: cannot attach %q — every candidate was filtered out (internal invariant violated%s)",
			u.Name, stabilityHint(prev))
	}
	return best, nil
}

// stabilityHint distinguishes "this fleet cannot be served at all" from "this fleet
// cannot be served WITHOUT MOVING PEOPLE". A stability-preserving rebuild pins every
// surviving node to its incumbent parent before the newcomers are placed, so a
// rebuild can fail on a fleet that a from-scratch build would satisfy. Saying which
// one happened is the difference between an operator adding upload and a caller
// simply retrying with prev = nil.
func stabilityHint(prev *Topology) string {
	if prev == nil {
		return ""
	}
	return "; this was a stability-preserving rebuild, so a rebuild from scratch may still succeed"
}

// rttTo scores the link from u to name, treating an unmeasured pair as worst-case.
func rttTo(u Node, name string) float64 {
	if v, ok := u.RTT[name]; ok {
		return v
	}
	return unknownRTT
}

func contains(names []string, name string) bool {
	for _, n := range names {
		if n == name {
			return true
		}
	}
	return false
}

// PickRoot chooses the tree's root.
//
// ELIGIBILITY. A node is eligible iff ALL of: not Provisional, not Impaired, not
// NATRelayed, and it can serve AT LEAST ONE child — effectiveUploadKbps(n)/streamKbps ≥ 1.
//
// streamKbps is why this parameter exists, and it closes the half of the live root-flap
// defect that the Provisional flag alone cannot. Provisional closes "rooted a guess" in
// every arrival order. But a node reporting a REAL 1200 kbit/s against a 2000 kbit/s
// stream cost genuinely cannot parent anyone, and without streamKbps this function could
// not see that: it returned the unfit node and BuildTree then failed with "root cannot
// serve any children". That is an honest failure over real telemetry rather than an
// arrival-order artifact — but suppressed is not closed, and one scalar closes it.
//
// Three rules, in order:
//
//  1. Only ELIGIBLE nodes are considered.
//  2. KEEP THE INCUMBENT. If prev is non-nil and prev.Root is present and STILL ELIGIBLE
//     (the same test — an incumbent that has become unfit is not defended), it is
//     retained unless some eligible challenger advertises at least RootChangeMarginKbps
//     more upload.
//  3. Otherwise: highest-upload eligible node, ties broken by name.
//
// Returns "" when no eligible node exists — a genuinely unrootable fleet, which the
// caller reports as unbuildable and which no retry can rescue, because dropping the
// stability preference does not create upload capacity.
//
// Root churn is the single most expensive thing this control plane can do: with a
// stability-preserving builder, a changed root invalidates every parent choice in the
// tree at once, the exact opposite of the "move one subtree, not the world" property the
// design exists to provide. Hence a stickiness rule of its own, stronger than the
// per-node one.
//
// MaxDepth is deliberately not a parameter: the root sits at depth 0 and the depth bound
// constrains its descendants, not its own eligibility. Passing the whole Constraints was
// rejected for a sharper reason — Constraints.Root is the very thing PickRoot computes,
// so it is a self-referential parameter that invites a caller to pass a stale root and
// quietly get it back. A scalar cannot be misused that way.
//
// A lone node is returned as-is (a one-node tree is trivially valid) even if ineligible;
// callers that build real meets never reach that path, and it exists for simnet and unit
// tests.
func PickRoot(nodes []Node, prev *Topology, streamKbps int) string {
	if len(nodes) == 1 {
		return nodes[0].Name
	}

	eligible := func(n Node) bool {
		if n.Provisional || n.Impaired || n.NAT == NATRelayed || streamKbps <= 0 {
			return false
		}
		return effectiveUploadKbps(n)/streamKbps >= 1
	}

	// Rule 3's answer, computed first because rule 2 needs the best challenger.
	best, bestUp := "", -1
	for _, n := range nodes { // slice order: never a map range
		if !eligible(n) {
			continue
		}
		if n.UploadKbps > bestUp || (n.UploadKbps == bestUp && n.Name < best) {
			best, bestUp = n.Name, n.UploadKbps
		}
	}
	if best == "" {
		return ""
	}

	// Rule 2: the incumbent keeps the role unless beaten by the whole margin.
	if prev != nil && prev.Root != "" {
		for _, n := range nodes {
			if n.Name != prev.Root {
				continue
			}
			if eligible(n) && bestUp < n.UploadKbps+RootChangeMarginKbps {
				return n.Name
			}
			break
		}
	}
	return best
}

// MaxLossDeratePct caps how much measured loss may shrink a node's advertised upload.
//
// 50%: a link losing half its packets cannot usefully relay anything, so there is
// nothing below this worth discriminating, and the Impaired flag — a discrete decision
// the control plane makes only after a DWELL — is the right instrument for the rest.
// Named, rather than inlined, so a maintainer does not "fix" the cap: an UNCAPPED derate
// would let a single bad sample halve a relay's capacity and re-parent its children
// instantly, which is exactly the thrash the dwell exists to prevent.
const MaxLossDeratePct = 50.0

// effectiveUploadKbps is the budget a node can actually spend on children: its advertised
// upload derated by measured loss, capped at MaxLossDeratePct. Negative or absent loss
// derates nothing.
func effectiveUploadKbps(n Node) int {
	loss := n.LossPct
	if loss <= 0 {
		return n.UploadKbps
	}
	if loss > MaxLossDeratePct {
		loss = MaxLossDeratePct
	}
	return int(float64(n.UploadKbps) * (1 - loss/100))
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
	return effectiveUploadKbps(n) / c.StreamKbps
}
