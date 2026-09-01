package coordinator

// Config's meaningful zeros.
//
// `-stickiness-ms 0` is documented as a real request — Phase-4 memoryless behaviour —
// and was silently upgraded to 25 by a `0 ⇒ use the default` convention sitting in the
// same struct as fields whose zero genuinely means "derive it". That is the
// DisableBackup polarity bug in a different costume: a zero that carries meaning
// colliding with a zero-means-default convention.
//
// These tests assert on the CONSTRAINTS the coordinator hands the builder and then on
// the TREE those constraints produce, never on the stored config value — a test that
// only read back the field would have passed against the broken code while the
// behaviour was wrong.

import (
	"io"
	"log/slog"
	"testing"

	"github.com/SammyUrfen/conclave/internal/overlay"
)

// stickinessDriver builds a meet named for the RTT fixture below, so the constraints it
// produces can be used verbatim rather than reassembled by the test.
func stickinessDriver(t *testing.T, cfg Config) (*driver, *roomState) {
	t.Helper()
	clk := newStubClock()
	cfg.MaxDepth, cfg.StreamKbps, cfg.Clock = 2, 2000, clk
	cfg.DegradedAfter, cfg.GoneAfter = hourAway, hourAway
	c := New(slog.New(slog.NewTextHandler(io.Discard, nil)), cfg, nil, &gatedPublisher{})
	c.wake = clk.NewTimer(hourAway)
	c.wake.Stop()
	c.wakeArmed = false
	d := &driver{c: c, clk: clk}
	d.join("p1", "r", 8000)
	d.join("p2", "p", 4000)
	d.join("p3", "q", 4000)
	d.join("p4", "u", 0)
	rs := c.rooms["r"]
	if rs == nil {
		t.Fatal("fixture: no room")
	}
	return d, rs
}

// TestStickinessReachesTheBuilderUnchanged: whatever the operator asked for is what the
// builder is given. Unset means the default, and that is the SAFE direction — a caller
// who never thought about stickiness gets the stability-preserving behaviour, not the
// memoryless one.
func TestStickinessReachesTheBuilderUnchanged(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
		want float64
	}{
		{"unset means the default", Config{}, overlay.DefaultStickinessMs},
		{"an explicit zero means ZERO", Config{StickinessMs: Stickiness(0)}, 0},
		{"an explicit default is still the default", Config{StickinessMs: Stickiness(overlay.DefaultStickinessMs)}, overlay.DefaultStickinessMs},
		{"an explicit other value passes through", Config{StickinessMs: Stickiness(40)}, 40},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, rs := stickinessDriver(t, tc.cfg)
			if got := d.c.constraintsFor(rs, "r").StickinessMs; got != tc.want {
				t.Fatalf("the builder was given StickinessMs %v, want %v", got, tc.want)
			}
		})
	}
}

// TestConfigDoesNotAliasACallersPointer: normalize takes a copy, so a caller reusing or
// mutating its Config after New cannot retune a running coordinator behind its back.
func TestConfigDoesNotAliasACallersPointer(t *testing.T) {
	ms := 40.0
	cfg := Config{StickinessMs: &ms}
	d, rs := stickinessDriver(t, cfg)
	ms = 999
	if got := d.c.constraintsFor(rs, "r").StickinessMs; got != 40 {
		t.Fatalf("the coordinator aliased the caller's pointer; StickinessMs is now %v", got)
	}
}

// TestMemorylessRequestProducesAMemorylessTree is the obligation stated properly: assert
// on the TREE.
//
// The fleet carries measured RTT, which is the only condition under which stickiness is
// observable at all — with every RTT unknown, `unknown + margin < unknown` is false for
// any margin, so a memoryless build and a sticky one agree by construction. u's
// incumbent parent p is 100ms away and q is 80ms away, a 20ms improvement:
//
//   - at the default 25ms margin the improvement is not worth an interruption, so u
//     stays on p;
//   - at an explicitly requested 0 the builder is memoryless and takes the closer parent.
//
// Against the pre-fix code both runs get 25 and produce the SAME tree, so this fails.
func TestMemorylessRequestProducesAMemorylessTree(t *testing.T) {
	fleet := []overlay.Node{
		{Name: "r", UploadKbps: 8000, NAT: overlay.NATDirect},
		{Name: "p", UploadKbps: 4000, NAT: overlay.NATDirect},
		{Name: "q", UploadKbps: 4000, NAT: overlay.NATDirect},
		{Name: "u", NAT: overlay.NATDirect, RTT: map[string]float64{"p": 100, "q": 80}},
	}

	build := func(t *testing.T, cfg Config) *overlay.Topology {
		t.Helper()
		d, rs := stickinessDriver(t, cfg)
		cons := d.c.constraintsFor(rs, "r")
		prev := &overlay.Topology{
			Epoch: cons.Epoch, Rev: cons.Rev - 1, Root: "r",
			Edges: []overlay.Edge{{Parent: "r", Child: "p"}, {Parent: "r", Child: "q"},
				{Parent: "p", Child: "u"}},
		}
		next, err := overlay.BuildTree(fleet, prev, cons)
		if err != nil {
			t.Fatalf("build: %v", err)
		}
		if verr := overlay.Validate(next, fleet, cons); verr != nil {
			t.Fatalf("validate: %v", verr)
		}
		return next
	}

	sticky := build(t, Config{})
	if got := sticky.ParentOf("u"); got != "p" {
		t.Fatalf("at the default margin a 20ms gain must not buy an interruption; u's parent is %q, want p", got)
	}
	memoryless := build(t, Config{StickinessMs: Stickiness(0)})
	if got := memoryless.ParentOf("u"); got != "q" {
		t.Fatalf("an explicitly requested memoryless build must take the closer parent; "+
			"u's parent is %q, want q — the request was silently upgraded to the default", got)
	}
}

// TestMeaningfulZerosAreHonoured is the audit result as an executable statement.
//
// Config has fields whose zero is a REQUEST and fields whose zero means "no preference".
// They sit in one struct and only the doc comments tell them apart, which is exactly how
// -stickiness-ms broke. Each meaningful zero is pinned here so a future normalize cannot
// quietly absorb one.
func TestMeaningfulZerosAreHonoured(t *testing.T) {
	t.Run("SocketDetection 0 disables the floor", func(t *testing.T) {
		c := &Coordinator{cfg: normalize(Config{SocketDetection: 0, GoneAfter: hourAway})}
		if got := c.goneThreshold(&nodeState{}); got != hourAway {
			t.Fatalf("a zero socket window must not be replaced by a default; threshold %v", got)
		}
	})
	t.Run("DegradedAfter and GoneAfter 0 derive from the peer's cadence", func(t *testing.T) {
		c := &Coordinator{cfg: normalize(Config{})}
		if c.cfg.DegradedAfter != 0 || c.cfg.GoneAfter != 0 {
			t.Fatalf("these zeros mean 'derive per node' and must survive normalize; got %v/%v",
				c.cfg.DegradedAfter, c.cfg.GoneAfter)
		}
	})
	t.Run("DefaultUploadKbps 0 keeps an unproven peer a leaf", func(t *testing.T) {
		if got := normalize(Config{}).DefaultUploadKbps; got != 0 {
			t.Fatalf("a not-yet-reported peer must be treated as a leaf; got %d kbit/s", got)
		}
	})
	t.Run("SelfName empty means the arbiter hosts the role", func(t *testing.T) {
		if got := normalize(Config{}).SelfName; got != "" {
			t.Fatalf("an empty SelfName must survive normalize; got %q", got)
		}
	})
}
