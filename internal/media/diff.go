package media

import (
	"sort"

	"github.com/SammyUrfen/conclave/internal/overlay"
)

// liveState is what the Router actually has in place right now, expressed in
// topology NAMES. It is passed to diffTopology as a value so the whole diff is a
// pure function of (self, wanted tree, current reality) — no Router, no pion, no
// locks — and therefore table-testable, which is the only way the ugly cases below
// get pinned at all.
type liveState struct {
	// roles maps each neighbour we hold a live session to onto that session's
	// BAKED-IN offerer role. Presence is "a session exists"; the value is the role
	// it was constructed with, which is what makes role inversion detectable.
	roles map[string]bool
	// legs are the (source → child) forwarding relationships the forwarder holds.
	legs []leg
	// relayEdge records, per neighbour, whether that session was BUILT as a relay
	// edge — i.e. whether setupRelayEdge ran for it. It is per-edge and not a single
	// flag because sessions are created at different moments: a peer promoted from
	// leaf to relay has one edge built the old way and the rest built the new way.
	relayEdge map[string]bool
	// backupChild names the sessions that exist ONLY by the §7.5a backup warrant —
	// a child that promoted us as its backup parent — mapped to the parent it was
	// failing over FROM. Presence means "this edge is not in the tree on purpose",
	// and the recorded parent is what tells a later push whether the coordinator has
	// acted on that failure yet.
	backupChild map[string]string
	// peerRelay records what the PEER was when the session was built. It is the
	// mirror of relayEdge and it exists for symmetry: a re-creation must be agreed by
	// both ends or the one that rebuilds sits waiting for an offer the other end has
	// no reason to send. See topoDiff.recreate reason 3.
	peerRelay map[string]bool
	// parent is the neighbour we are currently attached to as our upstream, or ""
	// if we have none (we are the root, or not yet attached).
	parent string
}

// topoDiff is the work one topology push implies. Everything is sorted so an apply
// is deterministic and a test can compare it whole.
type topoDiff struct {
	// add are neighbours with no live session. The re-parent target is deliberately
	// NOT here: it is opened by the re-parent state machine, which must keep the old
	// parent alive until the new one carries media.
	add []string
	// remove are live sessions the new tree drops. The OLD parent of a re-parent is
	// deliberately not here, for the same reason.
	remove []string
	// recreate are SURVIVING neighbours whose session must be torn down and rebuilt
	// because it was constructed for a world that no longer holds. Two independent
	// reasons, and BOTH matter — each one alone leaves the other's failure silent:
	//
	//  1. The OFFERER ROLE inverted: next.Offers(self, peer) != session.Offerer().
	//     Topology.Offers is evaluated once, in NewSession. A diff keeps a surviving
	//     edge — that is the point of a diff — but the tree around it can change so
	//     that the role flips: a leaf attached to a relay is the answerer, and a
	//     rebuild giving it children of its own makes both ends relays, so the
	//     tie-break may now name it. Its session is still baked as an answerer,
	//     onNegotiationNeeded returns early for a non-offerer, and an answerer cannot
	//     add m-lines. The failure is not "both offer" but "NEITHER CAN".
	//
	//  2. Our RELAY-NESS changed: next.IsRelay(self) != relayEdge[peer]. A session is
	//     built one way for a leaf and another for a relay — only the relay path runs
	//     setupRelayEdge, which is the sole place an upstream is registered and the
	//     only reason an arriving track is fanned out rather than counted. A peer
	//     promoted from leaf to relay therefore keeps a parent edge with no upstream
	//     and no forward loop, and its new child receives NOTHING, forever.
	//
	//  3. The PEER's relay-ness changed: next.IsRelay(peer) != peerRelay[peer]. This
	//     is reason 2 seen from the other end, and it is what makes a re-creation
	//     MUTUAL. A session is negotiated for the track set the far end publishes — a
	//     leaf publishes its own camera, a relay publishes forwarded tracks and no
	//     camera — so when that changes, both ends were built for a world that no
	//     longer holds. Without this the promoted peer tears down alone, and if it is
	//     the answerer on that edge it then waits forever for an offer the far end,
	//     whose own role and neighbour set never changed, has no reason to send.
	//
	// Reason 2 is invisible half the time without this bucket, because a promotion
	// usually flips the role as well — but only when the promoted peer sorts above
	// its parent. Below it, the role is unchanged and reason 1 never fires. Watching
	// only the role makes the core Phase 5 feature a coin flip on alphabetical order.
	//
	// Tearing down is the only cure pion allows for either: an answerer has no way
	// to add m-lines, and there is no way to retro-fit a forward loop onto a
	// TrackRemote another reader already owns.
	recreate []string
	// addLegs and removeLegs are forwarding legs to mutate on sessions that SURVIVE
	// this apply untouched. Legs on added or inverted neighbours are built by that
	// session's own setup, and legs on removed neighbours die with it, so listing
	// them here would double up.
	addLegs    []leg
	removeLegs []leg
	// reparent reports that our own upstream moved; oldParent must stay alive until
	// newParent carries media (§7.3).
	reparent  bool
	oldParent string
	newParent string
}

// empty reports whether this diff asks for nothing at all — the common case, since
// most pushes re-state a tree that already holds.
func (d topoDiff) empty() bool {
	return len(d.add) == 0 && len(d.remove) == 0 && len(d.recreate) == 0 &&
		len(d.addLegs) == 0 && len(d.removeLegs) == 0 && !d.reparent
}

// diffTopology computes what must change for self to realise next, given what is
// live today. It is total: a tree that does not mention self at all yields "close
// everything", never a half-applied state.
func diffTopology(self string, next *overlay.Topology, live liveState) topoDiff {
	var d topoDiff

	wanted := map[string]bool{}
	for _, n := range next.NeighborsOf(self) {
		wanted[n] = true
	}
	newParent := next.ParentOf(self)

	// A re-parent is only a re-parent when we HAVE a parent and it changed. Gaining
	// a first parent is an ordinary add; losing one entirely (a tree that drops us)
	// is an ordinary remove.
	if live.parent != "" && newParent != "" && live.parent != newParent {
		d.reparent = true
		d.oldParent = live.parent
		d.newParent = newParent
	}

	for name := range wanted {
		if _, live := live.roles[name]; live {
			continue
		}
		if d.reparent && name == d.newParent {
			continue // the state machine opens this one, not the plain add path
		}
		d.add = append(d.add, name)
	}
	selfRelay := next.IsRelay(self)
	for name, bakedRole := range live.roles {
		switch {
		case !wanted[name]:
			if d.reparent && name == d.oldParent {
				continue // held open until the new parent carries media
			}
			if promotionPending(next, self, name, live.backupChild) {
				continue // §7.5a: a promotion we accepted, not yet ruled on
			}
			d.remove = append(d.remove, name)
		case next.Offers(self, name) != bakedRole,
			selfRelay != live.relayEdge[name],
			next.IsRelay(name) != live.peerRelay[name]:
			d.recreate = append(d.recreate, name)
		}
	}
	sort.Strings(d.add)
	sort.Strings(d.remove)
	sort.Strings(d.recreate)

	// A session survives this apply untouched only if it is live, wanted, and in
	// none of the three churn buckets. Those are the only ones whose legs we may
	// mutate in place.
	survives := func(name string) bool {
		if _, ok := live.roles[name]; !ok || !wanted[name] {
			return false
		}
		if d.reparent && (name == d.oldParent || name == d.newParent) {
			return false
		}
		return !contains(d.recreate, name)
	}

	want := map[leg]bool{}
	for _, l := range wantedLegs(next, self) {
		want[l] = true
	}
	have := map[leg]bool{}
	for _, l := range live.legs {
		have[l] = true
	}
	for l := range want {
		if !have[l] && survives(l.child) {
			d.addLegs = append(d.addLegs, l)
		}
	}
	for _, l := range live.legs {
		if !want[l] && survives(l.child) {
			d.removeLegs = append(d.removeLegs, l)
		}
	}
	sortLegs(d.addLegs)
	sortLegs(d.removeLegs)
	return d
}

func sortLegs(ls []leg) {
	sort.Slice(ls, func(i, j int) bool {
		if ls[i].src != ls[j].src {
			return ls[i].src < ls[j].src
		}
		return ls[i].child < ls[j].child
	})
}

func contains(ss []string, s string) bool {
	for _, v := range ss {
		if v == s {
			return true
		}
	}
	return false
}

// wantedLegs is the set of (source → neighbour) forwarding relationships self must
// hold under t: for each neighbour, every peer whose media has to REACH that
// neighbour through us.
//
// The earlier model was "every neighbour's media to every other neighbour", which is
// right only for a one-relay tree. One hop deeper it is wrong twice over: a relay
// receives its parent's forwarded tracks, which carry OTHER peers' media and not the
// parent's, so several distinct sources arrive over one edge — and the media of a
// peer two hops away has no neighbour to be named after at all. Identifying a source
// by the neighbour that handed it over therefore collapses every arrival on an edge
// into one, and at the default -max-depth 2 a middle relay silently drops all but the
// first of them.
//
// The right identity is the ORIGINATING peer, and the right question per neighbour is
// "what is NOT already on that neighbour's side of this edge".
func wantedLegs(t *overlay.Topology, self string) []leg {
	if t == nil || !t.IsRelay(self) {
		return nil // a leaf forwards nothing; its one neighbour is its whole world
	}
	var out []leg
	for _, n := range t.NeighborsOf(self) {
		out = append(out, legsToward(t, self, n)...)
	}
	sortLegs(out)
	return out
}

// legsToward is wantedLegs for ONE peer: every source that has to reach peer through
// us. It takes any peer, not just a topology neighbour, because §7.5a's promoted
// backup child is an edge the tree deliberately does not contain yet — and it still
// has to be fed, with exactly the sources its own side of the cut cannot reach.
func legsToward(t *overlay.Topology, self, peer string) []leg {
	if t == nil {
		return nil
	}
	far := beyond(t, self, peer)
	var out []leg
	for _, src := range allNodes(t) {
		if src == self || far[src] || !publishes(t, src) {
			continue
		}
		out = append(out, leg{src: src, child: peer})
	}
	sortLegs(out)
	return out
}

// sourcesFrom is wantedLegs' mirror: the peers whose media ARRIVES over the edge to
// neighbour n, so the forwarder can point each one's upstream at that session and
// translate a downstream keyframe request back to the right place. Sorted, so two
// runs register the same sources in the same order.
func sourcesFrom(t *overlay.Topology, self, n string) []string {
	if t == nil {
		return nil
	}
	far := beyond(t, self, n)
	var out []string
	for _, src := range allNodes(t) {
		if src == self || !far[src] || !publishes(t, src) {
			continue
		}
		out = append(out, src)
	}
	sort.Strings(out)
	return out
}

// beyond is the set of peers on n's side of the (self, n) cut: everything reachable
// from n without passing through self. Cutting a tree at one edge leaves exactly two
// sides, which is what makes "forward what is not already over there" a complete and
// non-overlapping rule — and it is the same computation whether n is a child or the
// parent, so there is no direction case to get wrong.
//
// It walks the UNDIRECTED edge set rather than going through Subtree/Depth, because
// those are anchored on Topology.Root and a hand-authored Phase-3 file carries no
// Root until LoadTopology derives one. Reachability needs no root, and a helper that
// silently returned the empty set for a rootless tree would hand every neighbour
// every source — including its own media, looped straight back at it.
func beyond(t *overlay.Topology, self, n string) map[string]bool {
	adj := map[string][]string{}
	for _, e := range t.Edges {
		adj[e.Parent] = append(adj[e.Parent], e.Child)
		adj[e.Child] = append(adj[e.Child], e.Parent)
	}
	far := map[string]bool{n: true}
	queue := []string{n}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, next := range adj[cur] {
			if next == self || far[next] {
				continue
			}
			far[next] = true
			queue = append(queue, next)
		}
	}
	return far
}

// publishes reports whether a peer contributes media of its own. A relay does not:
// startPeer gives a relay a recvonly transceiver and forwarded tracks, never an
// outbound track of its own, so allocating a leg for one would negotiate an m-line
// that can never carry a packet. If that ever changes, this is the single place to
// change with it.
func publishes(t *overlay.Topology, name string) bool { return !t.IsRelay(name) }

// allNodes is Nodes plus Root, because a one-node tree has no edges and therefore no
// Nodes.
func allNodes(t *overlay.Topology) []string {
	nodes := t.Nodes()
	if t.Root != "" && !contains(nodes, t.Root) {
		nodes = append(nodes, t.Root)
	}
	return nodes
}

// promotionPending reports whether a live session the tree does not name is a
// backup child we accepted (§7.5a) whose promotion the coordinator has not yet ruled
// on — in which case dropping it as a stranger would take away the parent that peer
// has only just failed over to.
//
// Both clauses are exits, and both are needed:
//
//   - The coordinator must still name us as that child's backup. That assignment IS
//     the warrant the edge was accepted on; once it is withdrawn there is nothing
//     left to stand on.
//   - The child's parent-of-record must be unchanged since we accepted it. That is
//     what distinguishes "the coordinator has not acted yet" from "the coordinator
//     acted and placed this child elsewhere" — after which our edge is obsolete and
//     holding it means uploading to a peer that is not ours.
//
// The third exit needs no code: when the coordinator RATIFIES, the edge becomes a
// real tree edge, the child is wanted, and this is never consulted — the ordinary
// diff takes over and re-creates the session for its now-real role.
//
// Note this cannot be derived from the pushed topology alone. "Has the coordinator
// acted" is a question about change, so it needs a before as well as an after, and
// the before is what the Router recorded when it accepted the edge.
func promotionPending(next *overlay.Topology, self, name string, accepted map[string]string) bool {
	from, ok := accepted[name]
	if !ok {
		return false
	}
	return next.BackupOf(name) == self && next.ParentOf(name) == from
}
