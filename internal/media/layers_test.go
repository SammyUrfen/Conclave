package media

import (
	"reflect"
	"testing"
)

// TestLayerLadderIsOrderedLowToHigh pins the one thing every other layer decision
// reads: the ladder is a TOTAL ORDER, lowest first, and layerRank agrees with it.
//
// MUTATION CAUGHT: reversing layerLadder, or making layerRank return a constant.
// Either inverts every "highest available layer" resolution — a fresh leg would
// silently bind to the WORST encoding and stay there, which is invisible in any
// test that only asserts "some packets arrived".
func TestLayerLadderIsOrderedLowToHigh(t *testing.T) {
	if len(layerLadder) < 2 {
		t.Fatalf("layerLadder = %v; a one-rung ladder makes every selection test vacuous", layerLadder)
	}
	for i, id := range layerLadder {
		if got := layerRank(id); got != i {
			t.Errorf("layerRank(%q) = %d, want %d (its index in the ladder)", id, got, i)
		}
	}
	// An id this build does not know must rank BELOW every real rung, never at one
	// of them: a remote peer chooses these strings, and a hostile suffix that
	// aliased onto a real rank would let it pick which encoding a sibling receives.
	for _, unknown := range []string{"", "x", "video", "F", "q "} {
		if got := layerRank(unknown); got >= 0 {
			t.Errorf("layerRank(%q) = %d, want < 0 (unknown ids must not alias a rung)", unknown, got)
		}
	}
}

// TestSplitTrackName pins the (source, layer) recovery from a track id.
//
// The separator matters more than it looks: peer names legitimately contain '-'
// (`leaf-b`), so a '-'-separated layer suffix is ambiguous for exactly the names
// this repo's own tests and docs use. policy.PeerNamePattern forbids '.', which is
// why '.' is the separator.
//
// MUTATION CAUGHT: switching layerSep to "-" — "fwd-leaf-b" then splits as source
// "fwd-leaf" layer "b", so a relay files a source under a name no peer has and the
// child receives nothing at all.
func TestSplitTrackName(t *testing.T) {
	cases := []struct {
		id        string
		wantBase  string
		wantLayer string
	}{
		{"video", "video", ""},                   // today's single-layer camera track
		{"video.q", "video", "q"},                // a named layer
		{"video.f", "video", "f"},                //
		{"fwd-leaf-b", "fwd-leaf-b", ""},         // a forwarded track: '-' inside the NAME
		{"fwd-leaf-b.h", "fwd-leaf-b", "h"},      // …and a layer on top of it
		{"video.xyz", "video.xyz", ""},           // unknown suffix ⇒ not a layer at all
		{"video.", "video.", ""},                 // empty suffix ⇒ not a layer
		{"fwd-a.b.f", "fwd-a.b.f", ""},           // only the FIRST separator splits; "b.f" is not a rung
		{"", "", ""},                             //
		{"fwd-q", "fwd-q", ""},                   // a peer legitimately NAMED "q"
		{"fwd-leaf-b.q.f", "fwd-leaf-b.q.f", ""}, // ditto, deeper
		{"fwd-leaf-b.Q", "fwd-leaf-b.Q", ""},     // case-sensitive: not a rung
		{"video.h.", "video.h.", ""},             //
		{"fwd-relay.f", "fwd-relay", "f"},        //
		{"fwd-leaf_1.q", "fwd-leaf_1", "q"},      // '_' is legal in a peer name too
		{".f", ".f", ""},                         // an empty base is not a layer split
		{"fwd-.f", "fwd-", "f"},                  // degenerate but unambiguous
		{"video.hq", "video.hq", ""},             // a near-miss must not alias "h"
		{"fwd-leaf-b.ff", "fwd-leaf-b.ff", ""},   //
		{"fwd-leaf-b.f ", "fwd-leaf-b.f ", ""},   // trailing space is not "f"
		{"fwd-leaf-b .f", "fwd-leaf-b ", "f"},    // (names cannot contain spaces; still unambiguous)
		{"fwd-leaf-b-relay.h", "fwd-leaf-b-relay", "h"},
	}
	for _, tc := range cases {
		t.Run(tc.id, func(t *testing.T) {
			base, layer := splitTrackName(tc.id)
			if base != tc.wantBase || layer != tc.wantLayer {
				t.Errorf("splitTrackName(%q) = (%q, %q), want (%q, %q)",
					tc.id, base, layer, tc.wantBase, tc.wantLayer)
			}
		})
	}
}

// TestTrackSourceRecoversOriginAndLayer pins the rule the whole data plane keys
// off: a track is identified by the peer that ORIGINATED it, and a layer suffix
// never changes that answer.
//
// MUTATION CAUGHT: applying the fwd- prefix strip BEFORE the layer split (or vice
// versa in the wrong order) — "fwd-leaf-b.q" would resolve to source "leaf-b.q",
// which no forwardSource exists under, so the relay silently drains the track
// instead of fanning it out.
func TestTrackSourceRecoversOriginAndLayer(t *testing.T) {
	cases := []struct {
		id, peer  string
		wantSrc   string
		wantLayer string
	}{
		{"video", "leaf-b", "leaf-b", ""},         // own camera, single layer
		{"video.q", "leaf-b", "leaf-b", "q"},      // own camera, named layer
		{"fwd-leaf-b", "relay", "leaf-b", ""},     // forwarded, single layer
		{"fwd-leaf-b.f", "relay", "leaf-b", "f"},  // forwarded, named layer
		{"fwd-leaf-b", "leaf-b", "leaf-b", ""},    // origin == handler
		{"anything-else", "leaf-b", "leaf-b", ""}, // unknown id ⇒ the neighbour's own media
		{"video.xyz", "leaf-b", "leaf-b", ""},     // unknown suffix ⇒ single layer
	}
	for _, tc := range cases {
		t.Run(tc.id+"/"+tc.peer, func(t *testing.T) {
			src, layer := trackSource(tc.id, tc.peer)
			if src != tc.wantSrc || layer != tc.wantLayer {
				t.Errorf("trackSource(%q, %q) = (%q, %q), want (%q, %q)",
					tc.id, tc.peer, src, layer, tc.wantSrc, tc.wantLayer)
			}
		})
	}
}

// TestSelectLayer is the policy contract, asserted as a PURE state transition.
//
// The property under test is HYSTERESIS, and it is asymmetric on purpose:
// downgrading is cheap insurance against a child that is already losing packets,
// upgrading is speculative and can CAUSE the loss it was betting against. So a
// downgrade needs layerDownRounds of evidence and an upgrade needs layerUpRounds,
// with layerUpRounds strictly the larger.
//
// Each subtest names the mutation it catches.
func TestSelectLayer(t *testing.T) {
	ladder := []string{"q", "h", "f"}

	t.Run("one bad sample is not enough to downgrade", func(t *testing.T) {
		// MUTATION CAUGHT: layerDownRounds = 1, i.e. no hysteresis at all. A single
		// burst of loss — exactly what a jitter buffer exists to absorb — would then
		// cost the child a keyframe wait and a visible quality drop.
		got := selectLayer(ladder, layerChoice{Layer: "f"}, layerObs{LossPct: 20})
		if got.Layer != "f" {
			t.Errorf("layer = %q after ONE bad sample, want %q — a downgrade must need "+
				"layerDownRounds(%d) consecutive samples", got.Layer, "f", layerDownRounds)
		}
		if got.Bad != 1 {
			t.Errorf("Bad = %d, want 1 — the evidence must be COUNTED even when it does not act", got.Bad)
		}
	})

	t.Run("sustained loss downgrades exactly one rung", func(t *testing.T) {
		// MUTATION CAUGHT: dropping straight to the bottom rung on a downgrade. That
		// is the seductive "be safe" move and it is wrong: it throws away two rungs of
		// quality on evidence that only rules out one, and the slow upgrade path
		// (layerUpRounds) then makes the over-correction expensive to undo.
		st := layerChoice{Layer: "f"}
		for i := 0; i < layerDownRounds; i++ {
			st = selectLayer(ladder, st, layerObs{LossPct: 20})
		}
		if st.Layer != "h" {
			t.Fatalf("layer = %q after %d bad samples, want %q (exactly one rung down)",
				st.Layer, layerDownRounds, "h")
		}
		if st.Bad != 0 {
			t.Errorf("Bad = %d after acting, want 0 — leaving the counter armed makes the "+
				"NEXT bad sample drop a second rung with one sample of evidence", st.Bad)
		}
	})

	t.Run("the bottom rung is a floor", func(t *testing.T) {
		// MUTATION CAUGHT: an unclamped index decrement. Off the bottom of the ladder
		// there is no layer to name, and the leg would be switched to a layer no
		// upstream is sending — a permanently black child, with no error anywhere.
		st := layerChoice{Layer: "q"}
		for i := 0; i < layerDownRounds*3; i++ {
			st = selectLayer(ladder, st, layerObs{LossPct: 50})
		}
		if st.Layer != "q" {
			t.Errorf("layer = %q, want %q — the lowest rung is the floor", st.Layer, "q")
		}
	})

	t.Run("a clean link upgrades, but only after layerUpRounds", func(t *testing.T) {
		// MUTATION CAUGHT: layerUpRounds = layerDownRounds, i.e. a symmetric ladder.
		// Symmetry is a flapping machine: the upgrade that caused the loss is undone
		// by the downgrade in the same number of rounds, forever, and each transit
		// costs the child a keyframe wait.
		if layerUpRounds <= layerDownRounds {
			t.Fatalf("layerUpRounds(%d) must exceed layerDownRounds(%d): adding bitrate is the "+
				"speculative direction and must need more evidence than shedding it",
				layerUpRounds, layerDownRounds)
		}
		st := layerChoice{Layer: "q"}
		for i := 0; i < layerUpRounds-1; i++ {
			st = selectLayer(ladder, st, layerObs{LossPct: 0, RTTMs: 10, RTTKnown: true})
			if st.Layer != "q" {
				t.Fatalf("upgraded at round %d, want to wait for %d", i+1, layerUpRounds)
			}
		}
		st = selectLayer(ladder, st, layerObs{LossPct: 0, RTTMs: 10, RTTKnown: true})
		if st.Layer != "h" {
			t.Errorf("layer = %q after %d clean samples, want %q", st.Layer, layerUpRounds, "h")
		}
	})

	t.Run("the top rung is a ceiling", func(t *testing.T) {
		// MUTATION CAUGHT: an unclamped index increment — the leg would be switched to
		// a layer past the end of the ladder, i.e. to nothing.
		st := layerChoice{Layer: "f"}
		for i := 0; i < layerUpRounds*3; i++ {
			st = selectLayer(ladder, st, layerObs{LossPct: 0, RTTMs: 10, RTTKnown: true})
		}
		if st.Layer != "f" {
			t.Errorf("layer = %q, want %q — the highest rung is the ceiling", st.Layer, "f")
		}
	})

	t.Run("the middle band resets both counters", func(t *testing.T) {
		// The band between layerUpLossPct and layerDownLossPct is "no opinion". It must
		// RESET the run, not merely fail to increment it.
		//
		// MUTATION CAUGHT: leaving the counters alone in the middle band. Evidence
		// would then accumulate across unrelated episodes — four clean samples spread
		// over a minute of intermittent loss would read as a sustained clean run and
		// trigger an upgrade onto a link that is visibly struggling.
		st := layerChoice{Layer: "q"}
		for i := 0; i < layerUpRounds-1; i++ {
			st = selectLayer(ladder, st, layerObs{LossPct: 0, RTTMs: 10, RTTKnown: true})
		}
		mid := (layerUpLossPct + layerDownLossPct) / 2
		st = selectLayer(ladder, st, layerObs{LossPct: mid, RTTMs: 10, RTTKnown: true})
		if st.Good != 0 || st.Bad != 0 {
			t.Fatalf("counters = (Bad %d, Good %d) in the middle band, want both 0", st.Bad, st.Good)
		}
		st = selectLayer(ladder, st, layerObs{LossPct: 0, RTTMs: 10, RTTKnown: true})
		if st.Layer != "q" {
			t.Errorf("upgraded off a reset run; the clean streak must restart from zero")
		}
	})

	t.Run("high RTT blocks an upgrade but never forces a downgrade", func(t *testing.T) {
		// RTT is QUEUEING evidence, not capacity evidence. A deep queue means adding
		// bitrate will make it worse, so it gates the upgrade. It is NOT evidence the
		// current bitrate is too high — a distant peer has a large RTT permanently, and
		// downgrading on it would penalise geography rather than congestion.
		//
		// MUTATION CAUGHT: using RTT in the downgrade predicate. A clean-but-distant
		// peer would then be walked down to the bottom rung and pinned there.
		st := layerChoice{Layer: "h"}
		for i := 0; i < layerUpRounds*2; i++ {
			st = selectLayer(ladder, st, layerObs{LossPct: 0, RTTMs: layerUpRTTMs + 1, RTTKnown: true})
		}
		if st.Layer != "h" {
			t.Errorf("layer = %q with RTT above layerUpRTTMs, want %q (no upgrade)", st.Layer, "h")
		}
		if st.Bad != 0 {
			t.Errorf("Bad = %d on a lossless link, want 0 — RTT must not arm the downgrade", st.Bad)
		}
	})

	t.Run("unknown RTT does not block an upgrade", func(t *testing.T) {
		// A peer this node has never measured (first attachment is RTT-blind — see
		// RTTMemory) must not be frozen at the bottom rung forever.
		//
		// MUTATION CAUGHT: treating an absent RTT as infinite. Every leg on a
		// static-tree relay — which never calls LinkStats and so has no RTT at all —
		// would be permanently un-upgradable.
		st := layerChoice{Layer: "q"}
		for i := 0; i < layerUpRounds; i++ {
			st = selectLayer(ladder, st, layerObs{LossPct: 0})
		}
		if st.Layer != "h" {
			t.Errorf("layer = %q with no RTT measurement, want %q", st.Layer, "h")
		}
	})

	t.Run("an EMPTY ladder is not selectable", func(t *testing.T) {
		// The `len(ladder) < 2` guard is not merely an optimisation for the
		// single-layer case, and this subtest exists to say so in a form that fails.
		//
		// A source has legs from the moment the topology names it and a ladder only
		// once its tracks arrive, so reviewLayer legitimately runs with an EMPTY one —
		// see TestReviewLayerSurvivesASourceWithNoTracksYet for that path. With the
		// guard deleted, indexOf returns -1 and the off-ladder clamp below indexes
		// ladder[len(ladder)-1] — ladder[-1] — and PANICS on an RTCP-drain goroutine.
		//
		// MUTATION CAUGHT: deleting the guard. It panics rather than fails, which is
		// precisely what it does in production.
		st := selectLayer(nil, layerChoice{Layer: "f", Bad: 1, Good: 2}, layerObs{LossPct: 50})
		if st != (layerChoice{Layer: "f", Bad: 1, Good: 2}) {
			t.Errorf("state = %+v on an empty ladder, want it returned unchanged", st)
		}
	})

	t.Run("a one-rung ladder never moves", func(t *testing.T) {
		// The single-layer source. MUTATION CAUGHT: any index arithmetic that does not
		// clamp — a single-layer peer is the no-regression case and must be inert.
		one := []string{""}
		st := layerChoice{Layer: ""}
		for i := 0; i < 20; i++ {
			st = selectLayer(one, st, layerObs{LossPct: 90})
			st = selectLayer(one, st, layerObs{LossPct: 0, RTTMs: 1, RTTKnown: true})
		}
		if st.Layer != "" {
			t.Errorf("layer = %q, want %q — a one-rung ladder has nothing to choose", st.Layer, "")
		}
	})

	t.Run("a layer not on the ladder is clamped to the top", func(t *testing.T) {
		// The state can name a layer the source no longer publishes (an upstream went
		// away mid-run). Selecting from an index of -1 would step to ladder[-2].
		//
		// MUTATION CAUGHT: trusting cur.Layer's index without validating it.
		st := selectLayer(ladder, layerChoice{Layer: "gone"}, layerObs{LossPct: 0})
		if st.Layer != "f" {
			t.Errorf("layer = %q for an off-ladder input, want the top rung %q", st.Layer, "f")
		}
	})

	t.Run("selectLayer is pure", func(t *testing.T) {
		// MUTATION CAUGHT: mutating the ladder slice in place (e.g. sorting it), or
		// carrying state in a package-level variable. Either makes the choice depend on
		// call history rather than on its arguments, which is the one thing this
		// function's placement in the data plane cannot afford.
		before := append([]string(nil), ladder...)
		in := layerChoice{Layer: "h", Bad: 1, Good: 2}
		a := selectLayer(ladder, in, layerObs{LossPct: 20})
		b := selectLayer(ladder, in, layerObs{LossPct: 20})
		if !reflect.DeepEqual(a, b) {
			t.Errorf("same inputs gave %+v then %+v", a, b)
		}
		if !reflect.DeepEqual(ladder, before) {
			t.Errorf("ladder mutated to %v, want %v", ladder, before)
		}
		if in != (layerChoice{Layer: "h", Bad: 1, Good: 2}) {
			t.Errorf("input state mutated to %+v", in)
		}
	})
}

// TestMediaLayersFromPaths pins the origin-side flag translation: how many tracks a
// peer publishes and what each is called.
//
// MUTATION CAUGHT: naming the single-path layer "f" instead of "". That would
// rename today's `video` track to `video.f` for every existing deployment and every
// existing test — the far side's fallback to the neighbour's name still works, so
// nothing errors; the regression is invisible until a mixed fleet stops interoperating.
func TestMediaLayersFromPaths(t *testing.T) {
	cases := []struct {
		name  string
		paths []string
		want  []mediaLayer
	}{
		{"no media at all ⇒ one synthetic layer", nil, []mediaLayer{{ID: "", Path: ""}}},
		{"one file ⇒ exactly today's single track", []string{"a.ivf"},
			[]mediaLayer{{ID: "", Path: "a.ivf"}}},
		{"two files ⇒ the two LOWEST rungs, in order", []string{"lo.ivf", "hi.ivf"},
			[]mediaLayer{{ID: "q", Path: "lo.ivf"}, {ID: "h", Path: "hi.ivf"}}},
		{"three files ⇒ the whole ladder", []string{"a", "b", "c"},
			[]mediaLayer{{ID: "q", Path: "a"}, {ID: "h", Path: "b"}, {ID: "f", Path: "c"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := mediaLayersFor(tc.paths)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("mediaLayersFor(%v) = %+v, want %+v", tc.paths, got, tc.want)
			}
		})
	}

	// More paths than rungs must be REFUSED at the flag, not silently truncated: a
	// peer that publishes fewer layers than the operator asked for is a configuration
	// that reports success and does something else. (mediaLayersFor would in fact
	// index past the ladder and panic, so this rule is load-bearing, not cosmetic.)
	//
	// The paths must be NON-EMPTY. A first version of this passed
	// make([]string, n+1) — four empty strings — and was satisfied by the empty-path
	// rule below instead of the length rule it was aimed at, so deleting the length
	// check entirely left it green. A mutation sweep found it.
	tooMany := make([]string, len(layerLadder)+1)
	for i := range tooMany {
		tooMany[i] = "layer.ivf"
	}
	if err := ValidateMediaPaths(tooMany); err == nil {
		t.Errorf("ValidateMediaPaths accepted %d paths for a %d-rung ladder; it must fail loud",
			len(tooMany), len(layerLadder))
	}
	// An empty entry in a MULTI-layer list is its own refusal: it would publish a
	// synthetic-frame track as a real quality rung.
	if err := ValidateMediaPaths([]string{"a", ""}); err == nil {
		t.Error("ValidateMediaPaths accepted an empty path inside a multi-layer list")
	}
	if err := ValidateMediaPaths([]string{"a", "b"}); err != nil {
		t.Errorf("ValidateMediaPaths rejected a legal 2-layer config: %v", err)
	}
	if err := ValidateMediaPaths(nil); err != nil {
		t.Errorf("ValidateMediaPaths rejected the no-media case: %v", err)
	}
}
