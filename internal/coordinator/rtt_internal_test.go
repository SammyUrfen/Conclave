package coordinator

import (
	"testing"

	"github.com/SammyUrfen/conclave/internal/metrics"
	"github.com/SammyUrfen/conclave/internal/overlay"
)

// reportRTT files a routine (non-first) report carrying measured pairwise RTT.
func (d *driver) reportRTT(peerID, name string, upload int, rtt map[string]float64) {
	peers := make([]metrics.PeerRTT, 0, len(rtt))
	for n, ms := range rtt {
		peers = append(peers, metrics.PeerRTT{Name: n, RTTMs: ms})
	}
	rep := metrics.Report{Name: name, UploadKbps: upload, PeerRTT: peers}
	rep.Normalize()
	d.c.handle(event{kind: evReport, roomID: "r", peerID: peerID, report: rep})
}

// TestProjectCarriesMeasuredRTTIntoTheBuilder pins the one line that connects the new
// sensor to the tree: overlay.Node.RTT was ALWAYS nil before this, which is why
// docs/DESIGN.md §8.1(c) records BuildTree's min-latency rank as having no live data.
//
// The mutation this catches: dropping the copy in project. Every existing coordinator
// test passes without it, because a nil RTT map is a legal input that BuildTree
// tolerates by design — the failure is silent and looks like "the builder preferred
// the incumbent", which is also what it looks like when everything is working.
func TestProjectCarriesMeasuredRTTIntoTheBuilder(t *testing.T) {
	d := newDriver(t)
	d.join("p1", "a", 8000)
	d.join("p2", "b", 4000)
	d.reportRTT("p2", "b", 4000, map[string]float64{"a": 17.5})

	nodes, _ := d.c.project(d.c.rooms["r"])
	var b *overlay.Node
	for i := range nodes {
		if nodes[i].Name == "b" {
			b = &nodes[i]
		}
	}
	if b == nil {
		t.Fatal("b is not in the projection")
	}
	if b.RTT == nil {
		t.Fatal("Node.RTT is nil: the measured RTT never reached the builder")
	}
	if got := b.RTT["a"]; got != 17.5 {
		t.Errorf("Node.RTT[\"a\"] = %v, want 17.5", got)
	}
}

// TestProjectGivesAProvisionalNodeNoRTT pins that a peer which has joined but not yet
// reported carries no RTT at all, rather than an empty non-nil map or a fabricated
// zero. A zero-valued entry would be the WORST possible default here: bestParent
// treats a missing entry as unknown/worst-case and skips it, but a present 0 would
// beat every real measurement and pin nodes to a peer nobody has measured.
func TestProjectGivesAProvisionalNodeNoRTT(t *testing.T) {
	d := newDriver(t)
	d.join("p1", "a", 8000)
	d.c.handle(event{kind: evJoin, roomID: "r", peerID: "p9", name: "z"}) // joined, never reported

	nodes, _ := d.c.project(d.c.rooms["r"])
	for _, n := range nodes {
		if n.Name != "z" {
			continue
		}
		if !n.Provisional {
			t.Fatal("precondition: z should be provisional")
		}
		if len(n.RTT) != 0 {
			t.Errorf("a provisional node carries RTT %v; it has measured nothing", n.RTT)
		}
		return
	}
	t.Fatal("z is not in the projection")
}

// TestProjectDoesNotAliasTheReportsSlice pins that the builder cannot mutate the
// coordinator's stored telemetry, and that two rebuilds from the same report produce
// independent maps. Sharing one map across every projection would make a future
// builder optimisation that writes into Node.RTT corrupt the stored report — the kind
// of aliasing bug that shows up as a tree that changes without an event.
func TestProjectDoesNotAliasTheReportsSlice(t *testing.T) {
	d := newDriver(t)
	d.join("p1", "a", 8000)
	d.join("p2", "b", 4000)
	d.reportRTT("p2", "b", 4000, map[string]float64{"a": 10})

	first, _ := d.c.project(d.c.rooms["r"])
	for i := range first {
		if first[i].Name == "b" {
			first[i].RTT["a"] = 999
		}
	}
	second, _ := d.c.project(d.c.rooms["r"])
	for _, n := range second {
		if n.Name == "b" && n.RTT["a"] != 10 {
			t.Errorf("second projection saw %v; the first projection's map was shared", n.RTT["a"])
		}
	}
}

// TestACloserRelayWinsTheReParent is the acceptance test for the whole sensor chain,
// stated at the level the feature is claimed at.
//
// Before measured pairwise RTT existed this was STRUCTURALLY IMPOSSIBLE, and it is
// worth being precise about why: overlay.rttTo returns unknownRTT (math.MaxFloat64)
// for any peer absent from Node.RTT, and Node.RTT was always nil, so rank 1's test
// `rttTo(u, challenger) + StickinessMs < incumbentRTT` compared MaxFloat64 against
// MaxFloat64 and was false for every challenger, forever. The incumbent always won,
// and -stickiness-ms had no observable effect at any value.
//
// Note the shape of the trigger. A routine report does NOT rebuild — coordinator.go's
// anti-thrash rule says so explicitly — so the improved measurement is stored and
// acted on at the next THRESHOLD event. That is the contract, not a workaround, and
// the test drives it that way rather than reaching past it.
func TestACloserRelayWinsTheReParent(t *testing.T) {
	d := newDriver(t)
	// Root with room for both relays; two relays with room for a child each.
	d.join("p0", "r", 20000)
	d.join("p1", "p1", 8000)
	d.join("p2", "p2", 8000)
	d.join("p3", "c", 0)

	published := d.c.rooms["r"].published
	if published == nil {
		t.Fatal("precondition: the meet must have built a tree")
	}
	if got := published.ParentOf("c"); got != "p1" {
		t.Fatalf("precondition: c starts under p1, got %q (tree %+v)", got, published.Edges)
	}

	// c now measures its incumbent at 100 ms and the other relay at 5 ms: a 95 ms
	// advantage, far past the 25 ms default stickiness margin.
	d.reportRTT("p3", "c", 0, map[string]float64{"p1": 100, "p2": 5})

	// The measurement alone must not move anything — that is the anti-thrash rule.
	if got := d.c.rooms["r"].published.ParentOf("c"); got != "p1" {
		t.Errorf("a routine report rebuilt the tree on its own; parent is %q", got)
	}

	// The next threshold event rebuilds, and now the challenger is measurable.
	d.join("p4", "d", 0)

	if got := d.c.rooms["r"].published.ParentOf("c"); got != "p2" {
		t.Errorf("c's parent = %q, want p2: the closer relay did not win the re-parent", got)
	}
}

// TestStickinessHoldsAgainstASmallImprovement is the other half, and the half that
// proves -stickiness-ms is a real knob rather than a number that happens to be in a
// comparison. Same fleet, same trigger, an advantage BELOW the margin: the incumbent
// must keep the child, because a 10 ms latency win is not worth a visible stream
// interruption.
//
// Together with the test above this brackets the margin from both sides. Either test
// alone is satisfiable by a constant answer.
func TestStickinessHoldsAgainstASmallImprovement(t *testing.T) {
	d := newDriver(t)
	d.join("p0", "r", 20000)
	d.join("p1", "p1", 8000)
	d.join("p2", "p2", 8000)
	d.join("p3", "c", 0)

	if got := d.c.rooms["r"].published.ParentOf("c"); got != "p1" {
		t.Fatalf("precondition: c starts under p1, got %q", got)
	}

	// 10 ms better — real, measured, and below DefaultStickinessMs (25).
	d.reportRTT("p3", "c", 0, map[string]float64{"p1": 100, "p2": 90})
	d.join("p4", "d", 0)

	if got := d.c.rooms["r"].published.ParentOf("c"); got != "p1" {
		t.Errorf("c's parent = %q, want p1: a sub-margin improvement moved the child anyway", got)
	}
}
