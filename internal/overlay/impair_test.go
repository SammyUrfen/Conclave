package overlay

import "testing"

// TestEffectiveUploadDerate covers the loss derate and, more importantly, its CAP. Past
// MaxLossDeratePct the link is broken rather than merely lossy, and the discrete Impaired
// flag — a decision made after a dwell — is the right instrument for that. An uncapped
// derate would let one bad sample halve a relay's capacity and re-parent its children
// instantly, which is the thrash the dwell exists to prevent.
func TestEffectiveUploadDerate(t *testing.T) {
	tests := []struct {
		name string
		node Node
		want int
	}{
		{"no loss is no derate", Node{UploadKbps: 10000}, 10000},
		{"10% loss", Node{UploadKbps: 10000, LossPct: 10}, 9000},
		{"50% loss is the cap itself", Node{UploadKbps: 10000, LossPct: 50}, 5000},
		{"90% loss is capped at 50%", Node{UploadKbps: 10000, LossPct: 90}, 5000},
		{"100% loss is capped at 50%", Node{UploadKbps: 10000, LossPct: 100}, 5000},
		{"negative loss is ignored", Node{UploadKbps: 10000, LossPct: -5}, 10000},
		{"zero upload stays zero", Node{UploadKbps: 0, LossPct: 10}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := effectiveUploadKbps(tt.node); got != tt.want {
				t.Errorf("effectiveUploadKbps = %d, want %d", got, tt.want)
			}
		})
	}
}

// TestCapacityDeratesOnLoss proves the derate actually reaches the degree bound — the
// only place capacity means anything.
func TestCapacityDeratesOnLoss(t *testing.T) {
	c := Constraints{StreamKbps: 2000}
	clean := Node{Name: "a", UploadKbps: 8000}
	lossy := Node{Name: "a", UploadKbps: 8000, LossPct: 25} // 6000 effective
	if got := capacityOf(clean, c); got != 4 {
		t.Errorf("clean capacity = %d, want 4", got)
	}
	if got := capacityOf(lossy, c); got != 3 {
		t.Errorf("lossy capacity = %d, want 3 (25%% of 8000 kbit/s is a whole stream)", got)
	}
}

// TestImpairedRelayLosesItsChildren is the test that turns the degradation dwell from
// inert machinery into observable behaviour, and it is the reason Node.Impaired exists.
//
// A sustained-degradation event marks P impaired. Rank 0b then excludes P as a candidate
// and rank 1 voids its incumbency over the children it already has, so a rebuild MOVES
// them to a healthy relay with room. Before C8 this rebuild produced a byte-identical
// tree and the whole dwell was dead code.
func TestImpairedRelayLosesItsChildren(t *testing.T) {
	// R roots (cap 4). P and Q are relays with room. u and v hang off P.
	prev := &Topology{Epoch: 1, Rev: 1, Root: "R", Edges: []Edge{
		{Parent: "R", Child: "P"},
		{Parent: "R", Child: "Q"},
		{Parent: "P", Child: "u"},
		{Parent: "P", Child: "v"},
	}}
	fleet := func(impaired bool) []Node {
		return []Node{
			{Name: "R", UploadKbps: 8000},
			{Name: "P", UploadKbps: 8000, Impaired: impaired},
			{Name: "Q", UploadKbps: 8000},
			{Name: "u"}, {Name: "v"},
		}
	}
	cons := Constraints{Root: "R", MaxDepth: 3, StreamKbps: 2000, Epoch: 1, Rev: 2, StickinessMs: DefaultStickinessMs}

	// Control: nothing impaired, nothing moves. Without this the test below could pass
	// for the wrong reason.
	healthy := mustBuild(t, fleet(false), prev, cons)
	for _, n := range []string{"u", "v"} {
		if got := healthy.ParentOf(n); got != "P" {
			t.Fatalf("control: %s parent = %q, want P (a healthy incumbent keeps its children)", n, got)
		}
	}

	nodes := fleet(true)
	next := mustBuild(t, nodes, prev, cons)
	if err := Validate(next, nodes, cons); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	for _, n := range []string{"u", "v"} {
		if got := next.ParentOf(n); got == "P" {
			t.Errorf("%s is still parented to the impaired relay P; rank 1 must not protect it", n)
		}
	}
	if next.IsRelay("P") {
		t.Errorf("impaired P still relays for %v; rank 0b must exclude it while a healthy candidate has room",
			next.ChildrenOf("P"))
	}
	// P itself is not evicted from the tree — impairment is a parenting disqualification,
	// not a removal.
	if next.Depth("P") < 0 {
		t.Error("impaired P was dropped from the tree; it must stay attached as a leaf")
	}
	// And the churn oracle must accept the move: Impaired is a justification class.
	if err := ValidateLocalRepair(prev, next, nodes, cons, Churn{Impaired: []string{"P"}}); err != nil {
		t.Errorf("moving an impaired relay's children is justified churn: %v", err)
	}
}

// TestImpairmentIsASoftFilter is the other half of rank 0b, and the half a naive
// implementation gets wrong: a degraded parent beats NO parent. When every eligible
// candidate is impaired, impaired candidates are re-admitted rather than the build
// failing — degradation is a preference, disconnection is not.
func TestImpairmentIsASoftFilter(t *testing.T) {
	// R is the only possible parent and it is impaired. u must still attach.
	nodes := []Node{
		{Name: "R", UploadKbps: 8000, Impaired: true},
		{Name: "u"},
		{Name: "v"},
	}
	cons := Constraints{Root: "R", MaxDepth: 2, StreamKbps: 2000, Epoch: 1, Rev: 1, StickinessMs: DefaultStickinessMs}
	topo, err := BuildTree(nodes, nil, cons)
	if err != nil {
		t.Fatalf("an all-impaired fleet must still build (a degraded parent beats no parent): %v", err)
	}
	if err := Validate(topo, nodes, cons); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	for _, n := range []string{"u", "v"} {
		if topo.ParentOf(n) != "R" {
			t.Errorf("%s parent = %q, want R", n, topo.ParentOf(n))
		}
	}

	// Same shape, but now a healthy relay has room for exactly ONE of the two children:
	// the healthy candidate is preferred, and the second child falls back to the
	// impaired one rather than failing to attach.
	mixed := []Node{
		{Name: "R", UploadKbps: 8000, Impaired: true},
		{Name: "H", UploadKbps: 2000}, // cap 1
		{Name: "u"}, {Name: "v"},
	}
	mixedCons := Constraints{Root: "R", MaxDepth: 3, StreamKbps: 2000, Epoch: 1, Rev: 1, StickinessMs: DefaultStickinessMs}
	mixedTopo, err := BuildTree(mixed, nil, mixedCons)
	if err != nil {
		t.Fatalf("mixed fleet: %v", err)
	}
	if err := Validate(mixedTopo, mixed, mixedCons); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if n := len(mixedTopo.ChildrenOf("H")); n != 1 {
		t.Errorf("healthy relay H has %d children, want 1 (its full capacity, preferred over the impaired root)", n)
	}
}

// TestImpairmentCouplingRule is the §3.4 coupling rule, and §12.2 requires BOTH of its
// branches: one fleet, differing ONLY in whether a single candidate is impaired, must
// produce two different outcomes.
//
//	healthyAvailable  ⇒ rank 0b excludes impaired candidates AND rank 1's void fires:
//	                    the impaired incumbent loses its children to a healthy relay.
//	no healthy relay  ⇒ rank 0b re-admits impaired candidates AND the void is
//	                    SUPPRESSED: incumbency is preserved and nobody moves.
//
// The second branch is the amendment. The void exists for exactly one purpose — let the
// dwell move children off an impaired relay ONTO A HEALTHY ONE. With nowhere healthy to
// go, that premise is absent and all that is left is churn: a stream interruption bought
// for an RTT delta, in the state where the fleet can least afford it, and repeatedly,
// because impairment is sustained by definition. When there is nowhere healthy to go,
// stability is the only value left.
//
// The RTT landscape is rigged so the wrong answer is VISIBLE in both directions: H is
// closer to u than P is, but only by 10 ms — inside DefaultStickinessMs — so incumbency,
// where it applies, is what keeps u at P, and nothing else can.
func TestImpairmentCouplingRule(t *testing.T) {
	// R roots. P is u's impaired incumbent parent. H is the other candidate, and it is
	// the ONLY thing that differs between the two branches.
	fleet := func(healthyCandidate bool) []Node {
		return []Node{
			{Name: "R", UploadKbps: 8000, Impaired: true},
			{Name: "P", UploadKbps: 8000, Impaired: true},
			{Name: "H", UploadKbps: 8000, Impaired: !healthyCandidate},
			{Name: "u", RTT: map[string]float64{"R": 95, "P": 100, "H": 90}},
		}
	}
	prev := &Topology{Epoch: 1, Rev: 1, Root: "R", Edges: []Edge{
		{Parent: "R", Child: "P"},
		{Parent: "R", Child: "H"},
		{Parent: "P", Child: "u"},
	}}
	cons := Constraints{Root: "R", MaxDepth: 3, StreamKbps: 2000, Epoch: 1, Rev: 2, StickinessMs: DefaultStickinessMs}

	t.Run("a healthy candidate exists: the void fires", func(t *testing.T) {
		nodes := fleet(true)
		next := mustBuild(t, nodes, prev, cons)
		if err := Validate(next, nodes, cons); err != nil {
			t.Fatalf("Validate: %v", err)
		}
		if got := next.ParentOf("u"); got != "H" {
			t.Errorf("u parent = %q, want H: with somewhere healthy to go, an impaired incumbent keeps no protection", got)
		}
		if err := ValidateLocalRepair(prev, next, nodes, cons, Churn{Impaired: []string{"P"}}); err != nil {
			t.Errorf("ValidateLocalRepair: %v", err)
		}
	})

	t.Run("every candidate is impaired: incumbency is preserved", func(t *testing.T) {
		nodes := fleet(false)
		next := mustBuild(t, nodes, prev, cons)
		if err := Validate(next, nodes, cons); err != nil {
			t.Fatalf("Validate: %v", err)
		}
		if got := next.ParentOf("u"); got != "P" {
			t.Errorf("u parent = %q, want P: with nowhere healthy to go, moving u between impaired parents is pure churn", got)
		}
		// Nothing at all may move in this branch — the whole tree is unchanged.
		if err := ValidateLocalRepair(prev, next, nodes, cons, Churn{Impaired: []string{"P"}}); err != nil {
			t.Errorf("ValidateLocalRepair: %v", err)
		}
		for _, e := range prev.Edges {
			if got := next.ParentOf(e.Child); got != e.Parent {
				t.Errorf("%s parent = %q, want %q: an all-impaired fleet must reshuffle nobody", e.Child, got, e.Parent)
			}
		}
	})

	// The two branches must genuinely differ, or the fleet does not exercise the rule.
	a := mustBuild(t, fleet(true), prev, cons)
	b := mustBuild(t, fleet(false), prev, cons)
	if a.ParentOf("u") == b.ParentOf("u") {
		t.Fatalf("both branches parented u to %q; the fixture does not discriminate the coupling", a.ParentOf("u"))
	}
}

// TestImpairedNodeIsNeverRooted covers the other two disqualifications an impaired node
// carries: it may not be root, and it may not be anyone's backup parent.
func TestImpairedNodeIsNeverRooted(t *testing.T) {
	nodes := []Node{
		{Name: "big", UploadKbps: 100000, Impaired: true},
		{Name: "small", UploadKbps: 4000},
	}
	if got := PickRoot(nodes, nil, 2000); got != "small" {
		t.Errorf("PickRoot = %q, want small (an impaired node cannot root a meet)", got)
	}
	// An impaired incumbent root is not defended either — the eligibility test is the
	// same one, so an incumbent that has become unfit hands the role over.
	prev := &Topology{Epoch: 1, Rev: 1, Root: "big"}
	if got := PickRoot(nodes, prev, 2000); got != "small" {
		t.Errorf("PickRoot = %q, want small (an impaired incumbent is not defended)", got)
	}
}

// TestImpairedNodeIsNeverABackup pins rank 0 of the backup pass: insurance written
// against a node the control plane has already classified as degraded is not insurance.
func TestImpairedNodeIsNeverABackup(t *testing.T) {
	nodes := []Node{
		{Name: "R", UploadKbps: 8000},
		{Name: "P", UploadKbps: 8000},
		{Name: "Q", UploadKbps: 8000, Impaired: true}, // the only candidate outside Subtree(P)
		{Name: "u", RTT: map[string]float64{"P": 5, "Q": 1, "R": 50}},
	}
	prev := &Topology{Epoch: 1, Rev: 1, Root: "R", Edges: []Edge{
		{Parent: "R", Child: "P"},
		{Parent: "R", Child: "Q"},
		{Parent: "P", Child: "u"},
	}}
	cons := Constraints{Root: "R", MaxDepth: 3, StreamKbps: 2000, Epoch: 1, Rev: 2, StickinessMs: DefaultStickinessMs}
	topo := mustBuild(t, nodes, prev, cons)
	if err := Validate(topo, nodes, cons); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if got := topo.BackupOf("u"); got == "Q" {
		t.Error("BackupOf(u) = Q, an impaired node; insurance must be written against a healthy relay")
	}
	if got := topo.BackupOf("u"); got != "R" {
		t.Errorf("BackupOf(u) = %q, want R (the only healthy candidate outside Subtree(P))", got)
	}
}
