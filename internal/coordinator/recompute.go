package coordinator

import (
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/SammyUrfen/conclave/internal/overlay"
)

// recompute is the ONLY caller of the build path, which is what makes the
// "threshold events only" policy structural rather than a convention: the eight
// threshold handlers call this, and nothing else does.
//
// It gates in three stages — eligibility, cooldown, build — in that order, because
// each answers a different question and the wrong order produces a different system:
// checking the cooldown first would let an ineligible meet burn its window, and
// checking neither would rebuild on every frame.
func (c *Coordinator) recompute(rs *roomState, cause string) {
	if !rs.serving {
		// Not this node's meet. Not a "settling" verdict either — that would claim a
		// readiness this node has no standing to report — so it emits nothing at all.
		return
	}
	now := c.cfg.Clock.Now()
	nodes, waiting := c.project(rs)

	// The rebuild window comes FIRST, ahead of even the minimum-size gate. A bootstrap
	// term over an empty meet has vacuously heard from everyone and must be able to say
	// so and close its window; ordering the size gate first would leave the window
	// armed until a member happened to join.
	if rs.rebuilding {
		unheard := c.unheard(rs)
		if len(unheard) > 0 && now.Before(rs.rebuildUntil) {
			c.settling(rs, unheard, "rebuilding from peers: waiting for realized topology")
			return
		}
		rs.rebuilding = false
		rs.rebuildUntil = time.Time{}
		rs.baseline = c.reconstructBaseline(rs)
		rs.heard = nil
	}

	// Rule 2. A meet too small to have a tree builds nothing and does not consume
	// the settle: a 1-node tree has no edges, carries no media, and would seed the
	// stickiness baseline with a root chosen from a fleet of one.
	if len(nodes) < MinBuildableMembers {
		c.settling(rs, waiting, fmt.Sprintf("meet has %d named member(s); %d are needed for a tree",
			len(nodes), MinBuildableMembers))
		return
	}
	// Rule 3. Eligible when every known member has reported (the fast path) OR the
	// membership-growth window has closed (the bound the fast path lacks).
	if len(waiting) > 0 && now.Before(rs.settleUntil) {
		c.settling(rs, waiting, "waiting for first telemetry from the fleet")
		return
	}
	rs.settlingNow = false
	// The window is spent, whichever branch ended it. Leaving it armed would fire a
	// redundant recompute later, and — worse — make a second growth episode's
	// re-arm indistinguishable from this one's leftovers.
	rs.settleUntil = time.Time{}

	// The cooldown. A suppressed recompute is never dropped: it sets the dirty flag
	// and runs when the window closes (see the dlCooldown deadline).
	if !rs.urgent && rs.built && now.Sub(rs.lastBuild) < c.cfg.RecomputeCooldown {
		rs.dirty = true
		c.log.Debug("recompute coalesced by the cooldown",
			slog.String("room_id", rs.id), slog.String("cause", cause))
		return
	}
	c.build(rs, nodes, cause)
}

// build performs the mandatory TWO-ATTEMPT build and publishes the result.
//
// Stickiness can make a satisfiable fleet unbuildable: a sticky rebuild pins
// incumbents before newcomers, so it can fail where a from-scratch build succeeds.
// That is inherent to the stability preference, not a bug — but a meet must never be
// declared unbuildable while a perfectly good tree exists. Hence: sticky, then
// exactly one retry with prev = nil. Only after BOTH fail does "unbuildable" mean
// genuinely over-constrained.
//
// The retry is a retry, not a mode: same tick, same inputs, one extra attempt, and
// the next recompute starts sticky again from the newly published tree. It does NOT
// bypass the cooldown — a relaxed build re-parents nearly everyone and is the single
// most expensive thing the control plane does.
func (c *Coordinator) build(rs *roomState, nodes []overlay.Node, cause string) {
	live := make(map[string]bool, len(nodes))
	for _, n := range nodes {
		live[n.Name] = true
	}
	// §5.6a's middle tree, derived fresh every round so it can never drift from
	// published.
	rs.working = deriveWorking(rs.published, live, rs.promotions)
	if rs.working == nil && rs.baseline != nil {
		// The first build of a new term: there is no published tree to patch, so the
		// stickiness baseline is the one reconstructed from the peers' realized state.
		// Without it BuildTree runs memoryless and re-parents a fleet that has not
		// changed — a global interruption caused by a handover rather than by anything
		// that happened to the media.
		rs.working = rs.baseline
	}

	root := overlay.PickRoot(nodes, rs.working, c.cfg.StreamKbps)
	if root == "" {
		// Short-circuit: dropping the stability preference does not create upload
		// capacity, so an unrootable fleet is over-constrained immediately and the
		// second attempt would be pointless.
		c.unbuildable(rs, ReasonNoEligibleRoot)
		return
	}

	cons := overlay.Constraints{
		Root:         root,
		MaxDepth:     c.cfg.MaxDepth,
		StreamKbps:   c.cfg.StreamKbps,
		Epoch:        rs.epoch,
		Rev:          rs.rev + 1,
		StickinessMs: c.cfg.StickinessMs,
	}

	outcome, reason := OutcomeBuilt, ""
	next, err := overlay.BuildTree(nodes, rs.working, cons)
	if err != nil {
		relaxed, err2 := overlay.BuildTree(nodes, nil, cons)
		if err2 != nil {
			c.unbuildable(rs, err2.Error())
			return
		}
		// The FIRST attempt's error is carried as the reason, because it is the only
		// explanation of why every peer is about to be re-parented; discarding it
		// would make the event unreadable.
		next, outcome, reason = relaxed, OutcomeRelaxed, err.Error()
		c.log.Warn("stability-preserving build failed; published a relaxed tree",
			slog.String("room_id", rs.id), slog.String("cause", cause),
			slog.Any("sticky_error", err))
	}

	// The oracle gates publication in both branches. A tree that fails its own
	// validator is never published — keeping the previous one is strictly better
	// than shipping a tree we can prove is wrong.
	if verr := overlay.Validate(next, nodes, cons); verr != nil {
		c.log.Error("computed tree failed its own oracle; keeping the previous topology",
			slog.String("room_id", rs.id), slog.Any("error", verr))
		c.unbuildable(rs, "computed tree failed Validate: "+verr.Error())
		return
	}
	c.publishTree(rs, next, outcome, reason, cause)
}

// publishTree commits a tree: it becomes `published`, seeds the next round's
// `working`, is pushed to every named member, and is announced.
func (c *Coordinator) publishTree(rs *roomState, next *overlay.Topology, outcome BuildOutcome, reason, cause string) {
	rs.published = next
	rs.working = next
	rs.rev = next.Rev
	rs.built = true
	rs.lastBuild = c.cfg.Clock.Now()
	rs.dirty = false
	rs.urgent = false
	// Ratifications are consumed by the tree that folds them in; carrying them
	// forward would re-apply a promotion the fleet has since moved past.
	rs.promotions = make(map[string]string)
	// Likewise the reconstructed baseline: it seeds exactly the first tree of a term,
	// after which `published` is the real thing to be sticky about.
	rs.baseline = nil

	// A relaxed publish is a SUCCESS, but a rare and expensive one the operator
	// should see: a meet that goes relaxed repeatedly is telling you the fleet is
	// chronically near its capacity bound.
	fields := []any{
		slog.String("room_id", rs.id), slog.String("cause", cause),
		slog.Uint64("epoch", next.Epoch), slog.Uint64("rev", next.Rev),
		slog.String("root", next.Root), slog.Int("edges", len(next.Edges)),
		slog.String("outcome", string(outcome)),
	}
	if outcome == OutcomeRelaxed {
		c.log.Warn("published topology", fields...)
	} else {
		c.log.Info("published topology", fields...)
	}

	for _, id := range c.sortedPeerIDs(rs) {
		ns := rs.nodes[id]
		if ns.name == "" {
			continue // unmanaged peer (joined without a name): nothing to push
		}
		c.enqueueSend(sendOp{roomID: rs.id, peerID: ns.id, topo: next, gate: rs.gate})
	}

	c.publish(rs, Event{Kind: EventTopology, Outcome: outcome, Reason: reason, Topo: next})
}

// unbuildable reports a fleet no tree can serve, retains the previous topology, and
// leaves the urgency flag alone so the next event retries promptly.
func (c *Coordinator) unbuildable(rs *roomState, reason string) {
	c.log.Warn("no tree exists for this fleet; keeping the previous topology",
		slog.String("room_id", rs.id), slog.String("reason", reason))
	c.publish(rs, Event{Kind: EventUnbuildable, Outcome: OutcomeUnbuildable, Reason: reason})
}

// settling reports that the meet is not ready to build yet. It fires ONCE per
// transition into ineligibility, not per suppressed event: the latter would be a
// per-frame event storm at exactly the moment the dashboard is connecting.
//
// Debug, never Warn. A warning that fires on every healthy startup teaches operators
// to ignore warnings.
func (c *Coordinator) settling(rs *roomState, waiting []string, reason string) {
	if rs.settlingNow {
		return
	}
	rs.settlingNow = true
	c.log.Debug("meet is settling", slog.String("room_id", rs.id),
		slog.String("reason", reason), slog.Any("waiting", waiting))
	c.publish(rs, Event{Kind: EventSettling, Outcome: OutcomeSettling,
		Waiting: waiting, Reason: reason})
}

// sortedPeerIDs gives the room's members a deterministic iteration order. Never a
// map range: the projection order feeds PickRoot's tie-break and BuildTree's, so map
// iteration would make the tree depend on Go's hash seed.
func (c *Coordinator) sortedPeerIDs(rs *roomState) []string {
	ids := make([]string, 0, len(rs.nodes))
	for id := range rs.nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// project turns the room's members into BuildTree's input, and reports which of them
// have not been heard from yet.
//
// It drops peers that cannot be placed in a name-keyed tree (no name), peers that
// collide on a name, and peers the health FSM has declared GONE — the last being the
// entire mechanism of local repair: excluding a node from the projection is what
// makes the general builder re-attach exactly its orphans and nobody else.
func (c *Coordinator) project(rs *roomState) (nodes []overlay.Node, waiting []string) {
	seenName := make(map[string]bool, len(rs.nodes))
	for _, id := range c.sortedPeerIDs(rs) {
		ns := rs.nodes[id]
		if ns.name == "" || ns.health == HealthGone {
			continue
		}
		if seenName[ns.name] {
			c.log.Warn("duplicate member name; skipping",
				slog.String("room_id", rs.id), slog.String("name", ns.name),
				slog.String("peer_id", id))
			continue
		}
		seenName[ns.name] = true

		n := overlay.Node{
			Name:        ns.name,
			UploadKbps:  c.cfg.DefaultUploadKbps,
			NAT:         overlay.NATDirect,
			Impaired:    ns.impaired,
			Provisional: !ns.reported,
		}
		if ns.reported {
			n.UploadKbps = ns.report.UploadKbps
			n.LossPct = ns.report.LossPct
			if ns.report.NAT != "" {
				n.NAT = ns.report.NAT
			}
		} else {
			waiting = append(waiting, ns.name)
		}
		nodes = append(nodes, n)
	}
	sort.Strings(waiting) // Event.Waiting is ordered so the dashboard is replayable
	return nodes, waiting
}

// deriveWorking builds §5.6a's middle tree: `published`, patched with everything the
// coordinator already knows changed — every departed or gone node's edges removed,
// and every ratified self-promotion applied — and nothing else.
//
// It is BuildTree's `prev`, never ValidateLocalRepair's. The asymmetry is the whole
// point: the BUILDER is told what the peers have already DONE, so stickiness
// protects it and the coordinator ratifies a peer's local decision instead of
// fighting it; the ORACLE is told what the peers were last INSTRUCTED to do, so it
// can check that what they did was allowed. Feeding the oracle this tree would have
// it assert against the very belief it exists to check, and would destroy the backup
// assignment a promotion must be checked against.
//
// Edges keep published's relative order but are re-emitted parents-first. The
// re-ordering is not cosmetic: BuildTree replays prev.Edges to process incumbents,
// and a promotion that moves a node under a parent listed LATER would otherwise be
// evaluated before that parent had been attached — silently undoing the very
// ratification this function exists to perform.
//
// Backups are deliberately NOT carried over: nothing reads them here, and copying
// them would invite exactly the published/working conflation this split prevents.
//
// LIMITATION, stated rather than hidden: a node orphaned by a departure has no edge
// here, so BuildTree processes it among the newcomers — after its OWN children, if
// it had any. At MaxDepth 2 (what the media layer targets) an orphan is always a
// leaf and the case cannot arise; at MaxDepth ≥ 3 a grandchild may be re-parented
// gratuitously. Fixing it needs a change in overlay.processingOrder, which this work
// item does not own.
func deriveWorking(published *overlay.Topology, live map[string]bool, promotions map[string]string) *overlay.Topology {
	if published == nil {
		return nil
	}
	parent := make(map[string]string, len(published.Edges))
	order := make([]string, 0, len(published.Edges))
	for _, e := range published.Edges {
		if !live[e.Parent] || !live[e.Child] {
			continue
		}
		parent[e.Child] = e.Parent
		order = append(order, e.Child)
	}
	for _, e := range published.Edges {
		to, promoted := promotions[e.Child]
		if !promoted || to == e.Child || !live[e.Child] || !live[to] {
			continue
		}
		if _, listed := parent[e.Child]; !listed {
			// Its old edge was dropped because its parent died — which is the common
			// case for a promotion. Re-admit it at published's position.
			order = append(order, e.Child)
		}
		parent[e.Child] = to
	}

	// A node that is a parent here but has no parent of its own is a fragment root
	// (the tree root, or a survivor whose own parent departed). Treating it as
	// placeable is what lets its descendants keep their edges.
	placed := make(map[string]bool, len(parent)+1)
	placed[published.Root] = true
	for _, p := range parent {
		if _, has := parent[p]; !has {
			placed[p] = true
		}
	}

	edges := make([]overlay.Edge, 0, len(order))
	emitted := make(map[string]bool, len(order))
	for len(edges) < len(order) {
		before := len(edges)
		for _, child := range order {
			if emitted[child] || !placed[parent[child]] {
				continue
			}
			edges = append(edges, overlay.Edge{Parent: parent[child], Child: child})
			emitted[child], placed[child] = true, true
		}
		if len(edges) == before {
			// Only reachable through a promotion that names a descendant, i.e. a
			// peer reporting something the backup invariant forbids. Emit the rest
			// in published order rather than silently truncating the hint; prev is
			// advisory to BuildTree, so a bad hint costs a re-parent, not a tree.
			for _, child := range order {
				if !emitted[child] {
					edges = append(edges, overlay.Edge{Parent: parent[child], Child: child})
					emitted[child] = true
				}
			}
			break
		}
	}

	return &overlay.Topology{
		Epoch: published.Epoch,
		Rev:   published.Rev,
		Root:  published.Root,
		Edges: edges,
	}
}

// unheard lists the named, live members this coordinator has not received a heartbeat
// from since the rebuild window opened, ascending. Empty means the fast exit applies —
// including vacuously, for a meet with no members yet.
func (c *Coordinator) unheard(rs *roomState) []string {
	var out []string
	for _, id := range c.sortedPeerIDs(rs) {
		ns := rs.nodes[id]
		if ns.name == "" || ns.health == HealthGone {
			continue
		}
		if !rs.heard[ns.name] {
			out = append(out, ns.name)
		}
	}
	sort.Strings(out)
	return out
}

// reconstructBaseline rebuilds a stickiness baseline for a new term out of what the
// peers say they have REALIZED, or returns nil if what they described is not a legal
// tree.
//
// It is built over the REDUCED node set actually heard from, never the full roster.
// That is the M5 correction and it is the whole reason this is worth doing: validating
// against the full roster made a single lost or late heartbeat render the
// reconstruction disconnected, fail Validate, and drop to prev = nil — a GLOBAL
// re-parent caused by one dropped frame. Reduced, an un-heard-from member is simply
// absent from prev, which the builder already handles perfectly: absent means
// "newcomer", and newcomers are attached after incumbents. One lost heartbeat now moves
// exactly one node instead of all of them.
//
// The baseline comes from heartbeats and from nothing else — never from a topology this
// node holds or was pushed. A new coordinator reconstructs ground truth, not its
// predecessor's beliefs, and that is what makes a handover after a crash take the same
// code path as a graceful one.
func (c *Coordinator) reconstructBaseline(rs *roomState) *overlay.Topology {
	all, _ := c.project(rs)
	heardNodes := make([]overlay.Node, 0, len(rs.heard))
	parents := make(map[string]string, len(rs.heard))
	for _, n := range all {
		if !rs.heard[n.Name] {
			continue
		}
		heardNodes = append(heardNodes, n)
		parents[n.Name] = c.realParentOf(rs, n.Name)
	}
	if len(heardNodes) < MinBuildableMembers {
		return nil // nothing worth being sticky about
	}

	obs := reconstructObserved(parents, rs.epoch)
	if obs == nil {
		c.log.Info("realized state has no unique root; rebuilding without a stickiness baseline",
			slog.String("room_id", rs.id), slog.Int("heard", len(heardNodes)))
		return nil
	}

	// Validate insists on a stamped tree (Epoch and Rev ≥ 1), while the baseline must
	// carry Rev 0 so BuildTree's "the revision must advance" guard passes against the
	// term's first tree at Rev 1. Both requirements are right and they simply disagree,
	// so the oracle sees a stamped copy and the builder gets the real one.
	check := *obs
	check.Rev = 1
	cons := overlay.Constraints{
		Root: obs.Root, MaxDepth: c.cfg.MaxDepth, StreamKbps: c.cfg.StreamKbps,
		Epoch: rs.epoch, Rev: 1, StickinessMs: c.cfg.StickinessMs,
	}
	if err := overlay.Validate(&check, heardNodes, cons); err != nil {
		// The residual case the contract admits: a genuinely torn tree, e.g. a
		// mid-flight re-parent captured half-applied. Falling back to a memoryless
		// build is expensive but correct; seeding the builder with an illegal baseline
		// would not be.
		c.log.Info("realized state is not a legal tree; rebuilding without a stickiness baseline",
			slog.String("room_id", rs.id), slog.Any("error", err))
		return nil
	}
	c.log.Info("reconstructed a stickiness baseline from peer heartbeats",
		slog.String("room_id", rs.id), slog.Uint64("epoch", rs.epoch),
		slog.Int("heard", len(heardNodes)), slog.Int("edges", len(obs.Edges)))
	return obs
}

// realParentOf is the upstream neighbour a member last reported having ACTUALLY
// connected — a different question from who the coordinator told it to connect to, and
// the ground-truth half that makes rebuild-from-peers possible.
func (c *Coordinator) realParentOf(rs *roomState, name string) string {
	for _, id := range c.sortedPeerIDs(rs) {
		if ns := rs.nodes[id]; ns.name == name {
			return ns.realParent
		}
	}
	return ""
}

// reconstructObserved turns a map of node -> realized parent into a topology.
//
// The root is the unique node with no parent inside the set; a node whose parent was
// not heard from counts as parentless, so two of them means the observed state has no
// single root and cannot be reconstructed at all (nil). Nodes not reachable from the
// root — a cycle off to the side — are simply not emitted, which makes the result fail
// Validate rather than pretend to be a tree.
//
// Edges are emitted breadth-first from the root with each node's children sorted by
// name. Two properties fall out, both load-bearing: the order is TOPOLOGICAL, which is
// the invariant BuildTree replays prev.Edges under; and it is a function of the data
// rather than of Go's randomised map iteration, so the same realized fleet always
// reconstructs identically and the first tree of a new term is replayable.
//
// Parent is the authority and Children is deliberately ignored. Parent is
// single-valued, so it cannot disagree with itself; reconciling it against N peers'
// child lists would need a conflict rule for evidence that adds nothing — a child link
// is the same edge seen from the other end.
func reconstructObserved(parents map[string]string, epoch uint64) *overlay.Topology {
	names := make([]string, 0, len(parents))
	for name := range parents {
		names = append(names, name)
	}
	sort.Strings(names)

	var roots []string
	children := make(map[string][]string, len(parents))
	for _, name := range names {
		p := parents[name]
		if _, present := parents[p]; p == "" || !present {
			roots = append(roots, name)
			continue
		}
		children[p] = append(children[p], name)
	}
	if len(roots) != 1 {
		return nil
	}
	for p := range children {
		sort.Strings(children[p])
	}

	root := roots[0]
	edges := make([]overlay.Edge, 0, len(parents))
	queue := []string{root}
	seen := map[string]bool{root: true}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, ch := range children[cur] {
			if seen[ch] {
				continue // a cycle: refuse to loop, and let Validate reject the result
			}
			seen[ch] = true
			edges = append(edges, overlay.Edge{Parent: cur, Child: ch})
			queue = append(queue, ch)
		}
	}
	// Rev 0 on purpose: this is a BASELINE, not a published tree, and BuildTree refuses
	// a prev whose revision the new tree does not advance past.
	return &overlay.Topology{Epoch: epoch, Rev: 0, Root: root, Edges: edges}
}
