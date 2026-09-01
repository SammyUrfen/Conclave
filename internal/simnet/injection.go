package simnet

// Failure injection: the primitives a scenario uses to make things go wrong on
// purpose. They are separate verbs rather than one parameterised Fault() because the
// distinctions between them are exactly what Phases 5 and 6 are about — a graceful
// leave, a vanished machine, a live-but-unreachable process, and a merely degraded
// link provoke four different code paths, and a scenario that says Kill reads as the
// story it is testing.
//
// INJECTED FAULTS PERSIST UNTIL EXPLICITLY CLEARED, including across membership
// churn: a scenario that kills an isolated node and re-adds it must still find it
// isolated, because the whole point of that scenario is a peer coming back with
// stale beliefs. Measured RTT is the opposite — it is a per-membership fact, so
// Remove drops it and a rejoining node re-declares it.

// ImpairedLossPct is the packet loss at which an injected degradation counts as an
// IMPAIRMENT rather than noise: a sustained fault that should void a relay's
// incumbency and derate its capacity, not merely nudge its latency rank.
//
// 5% is chosen because it is the point at which a video call is visibly broken
// (retransmission and PLI storms rather than the occasional concealed frame) while
// still sitting an order of magnitude above the 0.1–0.5% a healthy residential link
// shows — so it separates "this relay is failing" from "this relay had a bad
// second" without needing a second threshold.
const ImpairedLossPct = 5.0

// Departure records one node leaving the fleet and, crucially, WHETHER THE CONTROL
// PLANE WAS TOLD. That single bit is the whole difference between Kill and Leave: to
// the tree they are the same removal, but a graceful leave is known immediately
// while a kill is discovered only by a liveness timeout. Phase 5 exists for the
// second case, so the harness has to be able to tell a test which one it staged.
type Departure struct {
	// Name is the node that left.
	Name string
	// Graceful is true for Leave (the peer closed its connection and said so) and
	// false for Kill (the machine vanished mid-call).
	Graceful bool
}

// linkQuality is an injected degradation on one direction of a link. Stored per
// ordered pair and written on both, so Degrade is symmetric by construction.
type linkQuality struct {
	rttMs   float64
	lossPct float64
}

// Kill removes name WITHOUT any graceful notice — the machine dropped off the
// network mid-call. No leave notice is generated, so only the liveness timers will
// ever notice. This is the failure mode Phase 5 exists for, and it is strictly
// different from Leave. A no-op if name is not in the fleet.
func (n *Network) Kill(name string) { n.depart(name, false) }

// Leave removes name gracefully (the peer closed its connection). Equivalent to
// Remove plus the departure notice, kept under a role-explicit name so a scenario
// reads as the story it is testing.
func (n *Network) Leave(name string) { n.depart(name, true) }

func (n *Network) depart(name string, graceful bool) {
	if _, ok := n.nodes[name]; !ok {
		return
	}
	n.Remove(name)
	n.departures = append(n.departures, Departure{Name: name, Graceful: graceful})
}

// Departures returns every departure since the Network was created, in order. It is
// how a scenario asserts that the churn it staged is the churn that happened, and
// how a coordinator adapter decides which removals it was entitled to hear about.
func (n *Network) Departures() []Departure {
	return append([]Departure(nil), n.departures...)
}

// Partition makes the control link between a and b lossy in BOTH directions: frames
// between them are dropped until Heal. Symmetric because an asymmetric partition is
// a much rarer real failure and doubles the state space for no extra insight at this
// stage.
//
// It deliberately does NOT change what BuildTree sees. A partition is a control-plane
// fact — reports and pushes between two peers stop arriving — and the overlay model
// has no "forbidden pair" concept to express it with. Faking one through the RTT
// matrix would be worse than useless: an absent RTT reads as "unknown", which the
// builder treats as a load-balancing tie and may make the unreachable parent MORE
// attractive. Scenarios consult Partitioned when they deliver frames.
func (n *Network) Partition(a, b string) {
	if a == b {
		return
	}
	n.setPartition(a, b, true)
	n.setPartition(b, a, true)
}

// Heal removes a partition between a and b. A no-op if there was none.
func (n *Network) Heal(a, b string) {
	n.setPartition(a, b, false)
	n.setPartition(b, a, false)
}

func (n *Network) setPartition(a, b string, on bool) {
	if !on {
		delete(n.partitions[a], b)
		return
	}
	if n.partitions[a] == nil {
		n.partitions[a] = make(map[string]bool)
	}
	n.partitions[a][b] = true
}

// Partitioned reports whether the control link between a and b is currently cut,
// either by an explicit Partition or because one of them is Isolated. A node is
// never partitioned from itself.
func (n *Network) Partitioned(a, b string) bool {
	if a == b {
		return false
	}
	if n.isolated[a] || n.isolated[b] {
		return true
	}
	return n.partitions[a][b]
}

// Isolate partitions name from EVERY other node — the "my Wi-Fi died but my process
// is alive" case, which is distinct from Kill because the node keeps its own state
// and may come back with stale beliefs. This is the scenario that exercises epoch
// fencing.
//
// It is a STANDING property of the node rather than a snapshot of pairs, so a member
// who joins while name is isolated cannot reach it either. Modelling it as a set of
// pairs would silently heal the isolation the moment the fleet changed, which is the
// opposite of what a partition test wants.
func (n *Network) Isolate(name string) { n.isolated[name] = true }

// Rejoin clears every partition involving name, including its isolation.
func (n *Network) Rejoin(name string) {
	delete(n.isolated, name)
	delete(n.partitions, name)
	for _, row := range n.partitions {
		delete(row, name)
	}
}

// Degrade sets the injected link quality between a and b: round-trip milliseconds
// and loss percent, in both directions.
//
// Unlike Partition this DOES reach the builder: the injected round-trip overrides
// the measured baseline in the projection BuildTree consumes, so degrading a link
// actually moves a child onto a healthier parent. That is what makes a
// sustained-degradation scenario a behaviour test rather than a bookkeeping one.
func (n *Network) Degrade(a, b string, rttMs, lossPct float64) {
	if a == b {
		return
	}
	n.setDegraded(a, b, linkQuality{rttMs: rttMs, lossPct: lossPct})
	n.setDegraded(b, a, linkQuality{rttMs: rttMs, lossPct: lossPct})
}

// Restore clears an injected degradation between a and b, returning the link to its
// measured baseline. A no-op if there was none.
func (n *Network) Restore(a, b string) {
	delete(n.degraded[a], b)
	delete(n.degraded[b], a)
}

func (n *Network) setDegraded(a, b string, q linkQuality) {
	if n.degraded[a] == nil {
		n.degraded[a] = make(map[string]linkQuality)
	}
	n.degraded[a][b] = q
}

// LinkQuality returns the injected round-trip and loss between a and b, and whether
// any degradation is in force. ok is false for a clean link — "no injection" is a
// distinct answer from "injected zero loss", and collapsing them would make Restore
// unobservable.
func (n *Network) LinkQuality(a, b string) (rttMs, lossPct float64, ok bool) {
	q, ok := n.degraded[a][b]
	return q.rttMs, q.lossPct, ok
}

// NodeLossPct reports the worst injected loss on any link touching name, or 0 when
// none is injected.
//
// The injectors are link-level but the overlay's degradation signal is NODE-level (a
// relay whose incumbency is voided and whose capacity is derated), so somebody has
// to aggregate. Doing it here, at one choke point consumed by the projection, means
// the aggregation rule is stated and tested once. The rule is MAX, not mean: one
// thoroughly broken leg is a broken relay, and averaging it against healthy legs is
// exactly how a real fault gets diluted into invisibility.
func (n *Network) NodeLossPct(name string) float64 {
	worst := 0.0
	for _, q := range n.degraded[name] { // max over a set: iteration order is irrelevant
		if q.lossPct > worst {
			worst = q.lossPct
		}
	}
	return worst
}

// Impaired reports whether name's injected loss has reached ImpairedLossPct — i.e.
// whether this is a sustained fault rather than noise.
func (n *Network) Impaired(name string) bool { return n.NodeLossPct(name) >= ImpairedLossPct }
