package simnet

import (
	"fmt"
	"testing"

	"github.com/SammyUrfen/conclave/internal/overlay"
)

func injectionFleet() *Network {
	n := New()
	for _, name := range []string{"a", "b", "c"} {
		n.Add(overlay.Node{Name: name, UploadKbps: 4000})
	}
	return n
}

// TestKillVsLeave: both remove the node, and the ONLY difference — the one Phase 5
// exists for — is whether the control plane was told. A Kill generates no notice, so
// nothing but a liveness timeout will ever notice it.
func TestKillVsLeave(t *testing.T) {
	tests := []struct {
		name string
		run  func(n *Network)
		want []Departure
	}{
		{"leave announces", func(n *Network) { n.Leave("b") }, []Departure{{Name: "b", Graceful: true}}},
		{"kill is silent", func(n *Network) { n.Kill("b") }, []Departure{{Name: "b", Graceful: false}}},
		{"order is preserved", func(n *Network) { n.Kill("c"); n.Leave("b") },
			[]Departure{{Name: "c", Graceful: false}, {Name: "b", Graceful: true}}},
		{"removing an absent node records nothing", func(n *Network) { n.Kill("zz") }, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			n := injectionFleet()
			before := n.Len()
			tc.run(n)
			if got := fmt.Sprint(n.Departures()); got != fmt.Sprint(tc.want) {
				t.Errorf("Departures() = %v, want %v", got, tc.want)
			}
			if got, want := n.Len(), before-len(tc.want); got != want {
				t.Errorf("fleet size = %d, want %d", got, want)
			}
			for _, d := range tc.want {
				for _, node := range n.OverlayNodes() {
					if node.Name == d.Name {
						t.Errorf("%q is gone but still projected into the fleet", d.Name)
					}
				}
			}
		})
	}
}

// TestPartitionIsSymmetric: an asymmetric partition is a much rarer real failure and
// doubles the state space for no extra insight, so Partition is symmetric by
// construction and the test pins it in both directions.
func TestPartitionIsSymmetric(t *testing.T) {
	tests := []struct {
		name string
		run  func(n *Network)
		want map[string]bool // "x|y" -> partitioned
	}{
		{"no partition", func(*Network) {}, map[string]bool{"a|b": false, "b|a": false, "a|c": false}},
		{"partition a and b", func(n *Network) { n.Partition("a", "b") },
			map[string]bool{"a|b": true, "b|a": true, "a|c": false, "b|c": false}},
		{"heal removes it", func(n *Network) { n.Partition("a", "b"); n.Heal("a", "b") },
			map[string]bool{"a|b": false, "b|a": false}},
		{"heal of an unpartitioned pair is a no-op", func(n *Network) { n.Heal("a", "b") },
			map[string]bool{"a|b": false}},
		{"a node is never partitioned from itself", func(n *Network) { n.Isolate("a") },
			map[string]bool{"a|a": false}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			n := injectionFleet()
			tc.run(n)
			for pair, want := range tc.want {
				x, y := pair[:1], pair[2:]
				if got := n.Partitioned(x, y); got != want {
					t.Errorf("Partitioned(%q,%q) = %v, want %v", x, y, got, want)
				}
			}
		})
	}
}

// TestIsolateRejoin: Isolate is "my Wi-Fi died but my process is alive" — distinct
// from Kill because the node keeps its state and may come back with stale beliefs.
// It is a STANDING property of the node, not a snapshot of pairs, so a member that
// joins while someone is isolated is partitioned from them too.
func TestIsolateRejoin(t *testing.T) {
	n := injectionFleet()
	n.Isolate("b")
	for _, other := range []string{"a", "c"} {
		if !n.Partitioned("b", other) {
			t.Errorf("isolated b is not partitioned from %q", other)
		}
	}
	if n.Partitioned("a", "c") {
		t.Error("isolating b partitioned a from c")
	}
	if n.Len() != 3 {
		t.Errorf("Isolate removed a node: fleet size %d, want 3", n.Len())
	}

	n.Add(overlay.Node{Name: "d", UploadKbps: 4000})
	if !n.Partitioned("b", "d") {
		t.Error("a member joining while b is isolated can still reach b")
	}

	n.Partition("a", "c")
	n.Rejoin("b")
	for _, other := range []string{"a", "c", "d"} {
		if n.Partitioned("b", other) {
			t.Errorf("after Rejoin, b is still partitioned from %q", other)
		}
	}
	if !n.Partitioned("a", "c") {
		t.Error("Rejoin(b) cleared an unrelated partition")
	}
}

// TestDegradeRestore pins the injected link-quality store, which is what feeds the
// synthetic telemetry a scenario drives the degradation dwell timer with.
func TestDegradeRestore(t *testing.T) {
	tests := []struct {
		name             string
		run              func(n *Network)
		wantRTT, wantLos float64
		wantOK           bool
	}{
		{"no injection", func(*Network) {}, 0, 0, false},
		{"degrade both directions", func(n *Network) { n.Degrade("a", "b", 250, 12) }, 250, 12, true},
		{"a later degrade replaces the earlier one", func(n *Network) { n.Degrade("a", "b", 250, 12); n.Degrade("a", "b", 30, 1) }, 30, 1, true},
		{"restore clears it", func(n *Network) { n.Degrade("a", "b", 250, 12); n.Restore("a", "b") }, 0, 0, false},
		{"restore of a clean link is a no-op", func(n *Network) { n.Restore("a", "b") }, 0, 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			n := injectionFleet()
			tc.run(n)
			for _, pair := range [][2]string{{"a", "b"}, {"b", "a"}} {
				rtt, loss, ok := n.LinkQuality(pair[0], pair[1])
				if ok != tc.wantOK || rtt != tc.wantRTT || loss != tc.wantLos {
					t.Errorf("LinkQuality(%q,%q) = (%v,%v,%v), want (%v,%v,%v)",
						pair[0], pair[1], rtt, loss, ok, tc.wantRTT, tc.wantLos, tc.wantOK)
				}
			}
		})
	}
}

// TestDegradeSteersAttachment proves Degrade is not bookkeeping: an injected
// round-trip overrides the measured baseline in the projection BuildTree consumes,
// so degrading a link actually moves a child onto a healthier parent — the
// behaviour the sustained-degradation path in Phase 5 turns on.
func TestDegradeSteersAttachment(t *testing.T) {
	build := func(n *Network) *overlay.Topology {
		t.Helper()
		cons := overlay.Constraints{Root: "R", MaxDepth: 2, StreamKbps: 2000}
		topo, c, err := n.Build(nil, cons)
		if err != nil {
			t.Fatalf("build: %v", err)
		}
		if verr := validateTopology(topo, n.OverlayNodes(), c); verr != nil {
			t.Fatalf("validate: %v", verr)
		}
		return topo
	}

	n := New()
	n.Add(overlay.Node{Name: "R", UploadKbps: 4000}) // cap 2: fills with r1+r2
	n.Add(overlay.Node{Name: "r1", UploadKbps: 4000})
	n.Add(overlay.Node{Name: "r2", UploadKbps: 4000})
	n.Add(overlay.Node{Name: "leaf", UploadKbps: 0})
	// Pin r1 and r2 as SIBLINGS under R. Without this, r2 attaches under r1 (the
	// fewest-children tiebreak) and sits at MaxDepth, which would disqualify it as
	// an alternate parent and make the assertion below vacuous.
	n.SetRTT("r2", "R", 1)
	n.SetRTT("leaf", "r1", 5)
	n.SetRTT("leaf", "r2", 80)

	if p := build(n).ParentOf("leaf"); p != "r1" {
		t.Fatalf("baseline: leaf attached to %q, want r1 (RTT 5 < 80)", p)
	}
	n.Degrade("leaf", "r1", 200, 8)
	if p := build(n).ParentOf("leaf"); p != "r2" {
		t.Errorf("after degrading leaf↔r1 to 200ms: leaf attached to %q, want r2", p)
	}
	n.Restore("leaf", "r1")
	if p := build(n).ParentOf("leaf"); p != "r1" {
		t.Errorf("after Restore: leaf attached to %q, want r1 again", p)
	}
}

// TestNodeImpairment: sustained degradation is a NODE-level fact in the overlay
// contract (a relay whose incumbency is voided and whose capacity is derated), but
// the injector is link-level. NodeLossPct aggregates the worst injected loss on any
// link touching a node, and Impaired thresholds it at ImpairedLossPct — one choke
// point, so wiring it into overlay.Node is a single assignment when that field lands.
func TestNodeImpairment(t *testing.T) {
	tests := []struct {
		name      string
		run       func(n *Network)
		wantLoss  map[string]float64
		wantImpar map[string]bool
	}{
		{"clean fleet", func(*Network) {},
			map[string]float64{"a": 0, "b": 0, "c": 0},
			map[string]bool{"a": false, "b": false, "c": false}},
		{"one degraded link impairs both endpoints", func(n *Network) { n.Degrade("a", "b", 200, 9) },
			map[string]float64{"a": 9, "b": 9, "c": 0},
			map[string]bool{"a": true, "b": true, "c": false}},
		{"the worst link wins", func(n *Network) { n.Degrade("a", "b", 200, 9); n.Degrade("a", "c", 200, 20) },
			map[string]float64{"a": 20, "b": 9, "c": 20},
			map[string]bool{"a": true, "b": true, "c": true}},
		{"loss below the threshold is not impairment", func(n *Network) { n.Degrade("a", "b", 200, 1) },
			map[string]float64{"a": 1, "b": 1, "c": 0},
			map[string]bool{"a": false, "b": false, "c": false}},
		{"restore clears it", func(n *Network) { n.Degrade("a", "b", 200, 9); n.Restore("a", "b") },
			map[string]float64{"a": 0, "b": 0, "c": 0},
			map[string]bool{"a": false, "b": false, "c": false}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			n := injectionFleet()
			tc.run(n)
			for name, want := range tc.wantLoss {
				if got := n.NodeLossPct(name); got != want {
					t.Errorf("NodeLossPct(%q) = %v, want %v", name, got, want)
				}
			}
			for name, want := range tc.wantImpar {
				if got := n.Impaired(name); got != want {
					t.Errorf("Impaired(%q) = %v, want %v", name, got, want)
				}
			}
		})
	}
}

// TestDegradedRelayLosesItsChildren is the C8 behaviour end to end: a SUSTAINED
// degradation injected on the Network must reach BuildTree as an impaired node and
// actually move that relay's children onto a healthy one.
//
// overlay proves the mechanism with a hand-set Node.Impaired. What this proves is the
// WIRING — that Degrade/NodeLossPct/Impaired are not bookkeeping the builder never
// sees. Those are different failures and only this test catches the second.
//
// The fixture isolates impairment from latency, which is the whole difficulty here.
// The degradation is injected on P↔u at the SAME round-trip the link already
// measured, so no RTT changes anywhere; and the assertion is about v, whose link to P
// was never touched at all. If v moves off P, the only remaining explanation is that
// P reached the builder impaired. The sub-threshold control run pins the other half:
// loss below ImpairedLossPct must move nobody, so the test cannot pass merely because
// something was injected.
func TestDegradedRelayLosesItsChildren(t *testing.T) {
	// R roots (cap 2, so it fills with P and Q). u and v hang off P because P is
	// much closer to them than Q is.
	fleet := func() *Network {
		n := New()
		n.Add(overlay.Node{Name: "R", UploadKbps: 4000})
		n.Add(overlay.Node{Name: "P", UploadKbps: 8000})
		n.Add(overlay.Node{Name: "Q", UploadKbps: 8000})
		n.Add(overlay.Node{Name: "u", UploadKbps: 0})
		n.Add(overlay.Node{Name: "v", UploadKbps: 0})
		n.SetRTT("Q", "R", 1) // pin Q as R's child rather than P's
		n.SetRTT("u", "P", 5)
		n.SetRTT("v", "P", 5)
		n.SetRTT("u", "Q", 90)
		n.SetRTT("v", "Q", 90)
		return n
	}
	cons := overlay.Constraints{Root: "R", MaxDepth: 3, StreamKbps: 2000, StickinessMs: overlay.DefaultStickinessMs}

	build := func(t *testing.T, n *Network, prev *overlay.Topology) (*overlay.Topology, []overlay.Node, overlay.Constraints) {
		t.Helper()
		topo, c, err := n.Build(prev, cons)
		if err != nil {
			t.Fatalf("build: %v", err)
		}
		nodes := n.OverlayNodes()
		if verr := validateTopology(topo, nodes, c); verr != nil {
			t.Fatalf("validate: %v", verr)
		}
		return topo, nodes, c
	}

	t.Run("baseline puts u and v under P", func(t *testing.T) {
		base, _, _ := build(t, fleet(), nil)
		for _, leaf := range []string{"u", "v"} {
			if got := base.ParentOf(leaf); got != "P" {
				t.Fatalf("%s parent = %q, want P — the fixture no longer sets up the C8 scenario", leaf, got)
			}
		}
	})

	t.Run("sub-threshold loss moves nobody", func(t *testing.T) {
		n := fleet()
		base, _, _ := build(t, n, nil)
		n.Degrade("P", "u", 5, ImpairedLossPct/2) // same RTT, loss below the bar
		next, _, _ := build(t, n, base)
		for _, leaf := range []string{"u", "v"} {
			if got := next.ParentOf(leaf); got != "P" {
				t.Errorf("%s moved to %q on sub-threshold loss; only a SUSTAINED degradation may re-parent", leaf, got)
			}
		}
	})

	t.Run("the projection carries impairment to the builder", func(t *testing.T) {
		n := fleet()
		n.Degrade("P", "u", 5, 20)
		byName := map[string]overlay.Node{}
		for _, node := range n.OverlayNodes() {
			byName[node.Name] = node
		}
		if p := byName["P"]; !p.Impaired || p.LossPct != 20 {
			t.Errorf("projected P = {Impaired:%v LossPct:%v}, want {true 20} — Degrade never reaches BuildTree",
				p.Impaired, p.LossPct)
		}
		if q := byName["Q"]; q.Impaired || q.LossPct != 0 {
			t.Errorf("projected Q = {Impaired:%v LossPct:%v}, want {false 0}; impairment leaked to an untouched node",
				q.Impaired, q.LossPct)
		}
		if got, want := byName["P"].RTT["u"], 5.0; got != want {
			t.Errorf("projected RTT P→u = %v, want %v; the fixture must change loss WITHOUT changing latency", got, want)
		}
	})

	t.Run("sustained degradation moves the children", func(t *testing.T) {
		n := fleet()
		base, _, _ := build(t, n, nil)
		n.Degrade("P", "u", 5, 20) // same RTT, loss well past ImpairedLossPct
		next, nodes, c := build(t, n, base)

		// v is the discriminator: nothing about v's own links changed, so the only
		// thing that can have moved it is P being impaired.
		if got := next.ParentOf("v"); got == "P" {
			t.Errorf("v is still parented to the degraded P; impairment did not reach the builder")
		}
		if next.IsRelay("P") {
			t.Errorf("degraded P still relays for %v while Q had room", next.ChildrenOf("P"))
		}
		// Impairment is a parenting disqualification, not an eviction.
		if next.Depth("P") < 0 {
			t.Error("degraded P was dropped from the tree; it must stay attached as a leaf")
		}

		var impaired []string
		for _, node := range nodes {
			if node.Impaired {
				impaired = append(impaired, node.Name)
			}
		}
		if len(impaired) == 0 {
			t.Fatal("no node projected as impaired")
		}
		if rerr := validateRepair(base, next, nodes, c, overlay.Churn{Impaired: impaired}); rerr != nil {
			t.Errorf("moving a degraded relay's children is justified churn: %v", rerr)
		}
	})
}
