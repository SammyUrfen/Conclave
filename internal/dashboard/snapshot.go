package dashboard

import (
	"context"
	"math"
	"sort"
	"time"

	"github.com/SammyUrfen/conclave/internal/arbiter"
	"github.com/SammyUrfen/conclave/internal/coordinator"
	"github.com/SammyUrfen/conclave/internal/overlay"
)

// The frozen §9.4a enum values this package must emit even when its input is a zero
// value. An unset field is not a licence to invent a fourth enum member.
const (
	healthUnknownDefault = string(coordinator.HealthHealthy)
	natUnknownDefault    = string(overlay.NATDirect)
)

// provenanceRealized and provenanceIntended label every topology-shaped number at the
// SCHEMA level (§9.4b), so the UI cannot stumble into rendering a heartbeat-derived
// figure and a coordinator-computed one in the same element.
const (
	provenanceRealized = "realized"
	provenanceIntended = "intended"
)

// The frozen convergence enum (§9.4b). Three values because the comparison genuinely has
// three outcomes; see meetBody.Convergence for why this is not a boolean.
const (
	// convergenceConverged: a tree is published and every member's realized parent
	// matches the one it assigns.
	convergenceConverged = "converged"
	// convergenceDiverged: at least one member's realized parent disagrees. This is
	// convergence lag, a failed apply, or a fenced-out peer.
	convergenceDiverged = "diverged"
	// convergenceNoTree: there is nothing to compare against — no tree published, or
	// one with no edges. It is NOT a fault and NOT health; WHY there is no tree is
	// answered by the build-state events (`settling` versus `unbuildable`, §5.9), which
	// is deliberately not duplicated here.
	convergenceNoTree = "no_tree"
)

// snapshotBody returns the §9.3 detail body, which is also the WS `snapshot` frame's
// data, subject to the snapshotMinInterval read bound.
//
// The bound is the whole point: assembling this body round-trips coordinator.Snapshot
// through the SINGLE Run goroutine that also drives heartbeats, dwell timers and
// rebuilds, and this function is reachable unauthenticated with no socket at all. Left
// unbounded, request rate IS control-plane load — which would undo the care the
// coordinator took to move its outbound sends off that goroutine, and would let one
// caller stall every meet in the process.
//
// A result younger than snapshotMinInterval is reused; otherwise exactly ONE caller
// performs the read while the rest wait on it. Both halves are needed: the interval
// bounds a sequential flood, the single-flight latch bounds a concurrent one.
//
// The returned body is shared, not copied. Nothing mutates a body after it is built —
// it is marshalled and discarded — so a copy would allocate on every request to defend
// against a write that does not exist. Do not introduce one.
//
// It holds no lock across either seam call, which is not an optimisation but a deadlock
// rule: Snapshot round-trips the coordinator's Run goroutine, and Publish runs ON that
// goroutine and takes this same lock.
func (s *Server) snapshotBody(ctx context.Context, meetID string) (meetBody, error) {
	for {
		s.mu.Lock()
		ms := s.streamLocked(meetID)
		if !ms.snapAt.IsZero() && s.clk.Now().Sub(ms.snapAt) < snapshotMinInterval {
			body, err := ms.snap, ms.snapErr
			s.mu.Unlock()
			return body, err
		}
		if ch := ms.snapLoading; ch != nil {
			s.mu.Unlock()
			select {
			case <-ch:
				continue // the loader has published its result; re-read the cache
			case <-ctx.Done():
				return meetBody{}, ctx.Err()
			}
		}
		done := make(chan struct{})
		ms.snapLoading = done
		s.mu.Unlock()

		body, err := s.readSnapshot(ctx, meetID)

		s.mu.Lock()
		ms.snap, ms.snapErr, ms.snapAt = body, err, s.clk.Now()
		ms.snapLoading = nil
		s.mu.Unlock()
		close(done)
		return body, err
	}
}

// readSnapshot performs the actual round-trips. An error is cached alongside a success
// so that a flood of requests for a NON-EXISTENT meet is bounded too — otherwise the
// cheapest way to hammer the arbiter would be to ask for ids that are not there.
func (s *Server) readSnapshot(ctx context.Context, meetID string) (meetBody, error) {
	meet, err := s.cfg.Meets.GetMeet(ctx, meetID)
	if err != nil {
		return meetBody{}, err
	}
	var snap coordinator.RoomSnapshot
	if s.cfg.Subnet != nil {
		snap, err = s.cfg.Subnet.Snapshot(ctx, meetID)
		if err != nil {
			return meetBody{}, err
		}
	}
	return s.buildMeetBody(meet, snap), nil
}

// buildMeetBody is the pure merge of the arbiter's identity facts and the
// coordinator's tree. Keeping it free of I/O is what lets the ordering and
// sanitisation rules be tested without either seam.
func (s *Server) buildMeetBody(meet arbiter.Meet, snap coordinator.RoomSnapshot) meetBody {
	topo := snap.Topo

	// Epoch appears on both sides of the provenance split and legitimately (§9.4b):
	// the arbiter MINTS it, so it is authoritative there, while the coordinator's
	// value is the term it is actually serving. Prefer the coordinator's when it has
	// one — this body is the INTENDED view and should be labelled with the term the
	// tree was computed under — and fall back to the arbiter's when no coordinator is
	// running here, so the field is never a misleading zero.
	epoch := meet.Epoch
	if snap.Epoch != 0 {
		epoch = snap.Epoch
	}
	at := snap.At
	if at.IsZero() {
		at = s.clk.Now()
	}
	root := ""
	if topo != nil {
		root = topo.Root
	}

	body := meetBody{
		APIVersion:     APIVersion,
		ID:             meet.ID,
		Epoch:          epoch,
		Rev:            snap.Rev,
		Coordinator:    meet.Coordinator,
		CoordinatorID:  meet.CoordinatorID,
		ArbiterIsCoord: meet.ArbiterIsCoord,
		Root:           root,
		AtUnixMs:       unixMs(at),
		Nodes:          make([]nodeBody, 0, len(snap.Members)),
		Edges:          edgesOf(topo),
		Backups:        backupsOf(topo),
		StaleRejected:  snap.StaleRejected,
		Diverged:       make([]string, 0),
		Provenance:     provenanceIntended,
	}

	for _, m := range snap.Members {
		body.Nodes = append(body.Nodes, nodeOf(m, topo, meet.Coordinator))
		// A node has DIVERGED when the parent its heartbeat reports differs from the
		// one the published tree assigned. With no published tree at all there is
		// nothing to diverge FROM, so the comparison is skipped entirely and
		// convergenceOf reports no_tree — not "everyone is wrong", and not the
		// "converged" a bare emptiness check used to fall into.
		if topo != nil && topo.ParentOf(m.Name) != m.Parent {
			body.Diverged = append(body.Diverged, m.Name)
		}
	}
	// §8.1: nodes[] ascending by name. The coordinator already sorts its members, but
	// the order is promised by THIS boundary, so it is enforced here rather than
	// inherited from one implementation of one seam.
	sort.Slice(body.Nodes, func(i, j int) bool {
		if body.Nodes[i].Name != body.Nodes[j].Name {
			return body.Nodes[i].Name < body.Nodes[j].Name
		}
		return body.Nodes[i].ID < body.Nodes[j].ID
	})
	sort.Strings(body.Diverged)
	body.Convergence = convergenceOf(topo, body.Diverged)
	return body
}

// convergenceOf classifies the realized-versus-intended comparison (§9.4b).
//
// The order of the tests matters and is the whole design. A real disagreement is
// reported even when the tree has no edges, because a peer still naming a parent under a
// tree that assigns none is exactly the fenced-out or lagging peer this field exists to
// reveal — suppressing the comparison whenever the tree looks empty would hide the one
// case worth seeing. Only once nobody disagrees does "no edges" mean "nothing to
// compare", which covers both a meet with no tree yet and a single-node tree whose sole
// member is trivially its own root.
func convergenceOf(topo *overlay.Topology, diverged []string) string {
	if len(diverged) > 0 {
		return convergenceDiverged
	}
	if topo == nil || len(topo.Edges) == 0 {
		return convergenceNoTree
	}
	return convergenceConverged
}

// nodeOf renders one member. Parent and Children are REALIZED (the peer's own last
// heartbeat); Depth, Roles and Backup are INTENDED (the published tree). Both live on
// the same object on purpose — that is what makes the two comparable at all — and
// §9.4b forbids the UI from rendering them in the same element.
func nodeOf(m coordinator.MemberSnapshot, topo *overlay.Topology, coordName string) nodeBody {
	n := nodeBody{
		ID:                m.ID,
		Name:              m.Name,
		Roles:             rolesOf(m.Name, topo, coordName),
		Health:            healthOf(m.Health),
		Parent:            m.Parent,
		Backup:            m.Backup,
		Children:          append(make([]string, 0, len(m.Children)), m.Children...),
		Depth:             -1,
		UploadKbps:        m.Report.UploadKbps,
		NAT:               natOf(m.Report.NAT),
		RTTServerMs:       finite(m.Report.RTTServerMs),
		LossPct:           round1(m.Report.LossPct),
		CPUPct:            round1(m.Report.CPUPct),
		FitnessLowerBound: round3(fitnessOf(m)),
		LastBeatSeq:       m.LastBeatSeq,
		LastBeatUnixMs:    unixMs(m.LastBeatAt),
	}
	sort.Strings(n.Children)
	if topo != nil {
		n.Depth = topo.Depth(m.Name)
	}
	return n
}

// rolesOf builds the §1.2 role ARRAY. Two independent badges, never one merged role: a
// coordinator that also relays is both, and collapsing them would hide which of the two
// jobs a node is actually failing at. "leaf" is the answer for anything that forwards
// for nobody, including a node not in the tree at all.
func rolesOf(name string, topo *overlay.Topology, coordName string) []string {
	roles := make([]string, 0, 2)
	if coordName != "" && name == coordName {
		roles = append(roles, "coordinator")
	}
	if topo != nil && topo.IsRelay(name) {
		roles = append(roles, "relay")
	} else {
		roles = append(roles, "leaf")
	}
	return roles
}

// fitnessOf scores a member with the arbiter's OWN formula rather than a second one.
//
// arbiter.Fitness.UptimeSec is unavailable at this seam — coordinator.MemberSnapshot
// carries no join time — so the uptime term contributes 0 and the result is a LOWER
// BOUND on the arbiter's own score, short by at most the 0.15 uptime weight. Inventing
// an uptime would be worse: it would put a number the arbiter never computed under the
// name of one it did. §15.14 ruled that the wire key says so out loud
// (`fitness_lower_bound`) rather than footnoting it.
//
// The proper fix, recorded and deliberately NOT taken here, is for the ARBITER to
// expose the score it actually computed per node: it is the only component holding
// every input (Live is its own verdict), and this function is a second implementation
// of a decision the arbiter owns. It is not taken because §13.1 already records that
// the fitness inputs are unmeasured, so more precision would be more precisely
// fictional.
//
// Live is derived from the health FSM (anything but "gone" is live) and Coordinatable
// from the peer's own declaration, so the three hard disqualifiers — TURN-bound, not
// live, not willing — still zero the score exactly as they do in an election.
func fitnessOf(m coordinator.MemberSnapshot) float64 {
	return arbiter.Score(arbiter.Fitness{
		CPUFreePct:    100 - m.Report.CPUPct,
		RTTServerMs:   m.Report.RTTServerMs,
		LossPct:       m.Report.LossPct,
		UptimeSec:     0,
		NAT:           m.Report.NAT,
		Live:          m.Health != coordinator.HealthGone,
		Coordinatable: m.Report.Coordinatable,
	})
}

// edgesOf copies the tree's edges in TOPOLOGICAL order (§8.1). Never sorted: the
// attach order is the invariant a rebuild depends on.
func edgesOf(topo *overlay.Topology) []edgeBody {
	if topo == nil {
		return make([]edgeBody, 0)
	}
	out := make([]edgeBody, 0, len(topo.Edges))
	for _, e := range topo.Edges {
		out = append(out, edgeBody{Parent: e.Parent, Child: e.Child})
	}
	return out
}

// backupsOf copies the backup parents, which overlay already keeps ascending by node.
func backupsOf(topo *overlay.Topology) []backupBody {
	if topo == nil {
		return make([]backupBody, 0)
	}
	out := make([]backupBody, 0, len(topo.Backups))
	for _, b := range topo.Backups {
		out = append(out, backupBody{Node: b.Node, Parent: b.Parent})
	}
	return out
}

// treeShape derives the two summary numbers §9.4's topology event carries but
// overlay.Topology does not store: the deepest hop count, and the relays ascending.
func treeShape(topo *overlay.Topology) (depth int, relays []string) {
	relays = make([]string, 0)
	if topo == nil {
		return 0, relays
	}
	nodes := topo.Nodes()
	if len(nodes) == 0 && topo.Root != "" {
		// A one-node tree has no edges and therefore no Nodes; ask Root as well.
		nodes = []string{topo.Root}
	}
	for _, n := range nodes {
		if d := topo.Depth(n); d > depth {
			depth = d
		}
		if topo.IsRelay(n) {
			relays = append(relays, n)
		}
	}
	sort.Strings(relays)
	return depth, relays
}

// healthOf maps a Health onto the frozen three-value enum, defaulting an unset value
// to "healthy" — a member the coordinator is tracking but has not classified has not
// missed anything.
func healthOf(h coordinator.Health) string {
	if h == "" {
		return healthUnknownDefault
	}
	return string(h)
}

// natOf maps a NATType onto the frozen enum. metrics.Report omits NAT when it is the
// zero value, so "" means direct.
func natOf(n overlay.NATType) string {
	if n == "" {
		return natUnknownDefault
	}
	return string(n)
}

// finite replaces a NaN or an infinity with 0.
//
// This is not defensive padding: encoding/json REFUSES to marshal a non-finite float,
// so one broken sensor reading in one Report would take down the whole response with a
// 500 that names nothing. Clamping to the worst plausible value keeps the other 30
// fields readable, which is the point of a dashboard.
func finite(v float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0
	}
	return v
}

// round1 is §9.4a's one-decimal rule for percentages. Rounding at the boundary rather
// than in the client means every consumer sees the same number.
func round1(v float64) float64 { return math.Round(finite(v)*10) / 10 }

// round3 is §9.4a's three-decimal rule for fitness.
func round3(v float64) float64 { return math.Round(finite(v)*1000) / 1000 }

// unixMs converts an instant to §9.4a's integer-milliseconds convention, mapping the
// ZERO time to 0 rather than to time.Time's real epoch offset. Without that guard an
// unset LastBeatAt serialises as -6795364578871, which the frontend renders as a date
// in 1754 — a plausible-looking number is a far worse answer than an obvious one.
func unixMs(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}
