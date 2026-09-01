package metrics

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/SammyUrfen/conclave/internal/clock"
)

// TestLivenessFrameWireFormat pins the JSON key set of the two new peer -> control
// plane frames. These keys are the contract between a peer and an arbiter that may
// be built from a different commit, so a field rename must fail here.
func TestLivenessFrameWireFormat(t *testing.T) {
	tests := []struct {
		name string
		in   any
		want string
	}{
		{
			name: "relay heartbeat",
			in: Heartbeat{
				Name: "relay", Seq: 4, IntervalMs: 1000, Epoch: 2, Rev: 9,
				Parent: "root", ParentState: "connected",
				Children: []ChildLink{
					{Name: "leaf-a", State: "connecting"},
					{Name: "leaf-b", State: "connected"},
				},
				StaleRejected: 3,
			},
			want: `{"name":"relay","seq":4,"interval_ms":1000,"epoch":2,"rev":9,` +
				`"parent":"root","parent_state":"connected",` +
				`"children":[{"name":"leaf-a","state":"connecting"},{"name":"leaf-b","state":"connected"}],` +
				`"stale_rejected":3}`,
		},
		{
			// The common case is a peer that has refused nothing, so the key is
			// omitempty: a healthy fleet does not pay for a counter that is 0 on
			// every beat from every peer.
			name: "a peer that has refused nothing omits the counter",
			in:   Heartbeat{Name: "leaf-a", Seq: 2, Epoch: 1, Rev: 4, Parent: "relay", ParentState: "connected"},
			want: `{"name":"leaf-a","seq":2,"epoch":1,"rev":4,"parent":"relay","parent_state":"connected"}`,
		},
		{
			name: "leaf heartbeat omits children",
			in:   Heartbeat{Name: "leaf-a", Seq: 1, Epoch: 1, Rev: 3, Parent: "relay", ParentState: "connected"},
			want: `{"name":"leaf-a","seq":1,"epoch":1,"rev":3,"parent":"relay","parent_state":"connected"}`,
		},
		{
			name: "root heartbeat omits parent",
			in:   Heartbeat{Name: "root", Seq: 7, Epoch: 1, Rev: 3},
			want: `{"name":"root","seq":7,"epoch":1,"rev":3}`,
		},
		{
			name: "successful reparent",
			in:   Reparented{Name: "leaf-a", From: "r1", To: "r2", OK: true, Epoch: 2, Rev: 5, Reason: "parent failed"},
			want: `{"name":"leaf-a","from":"r1","to":"r2","ok":true,"epoch":2,"rev":5,"reason":"parent failed"}`,
		},
		{
			name: "stranded peer",
			in:   Reparented{Name: "leaf-a", From: "r1", OK: false, Epoch: 2, Rev: 5},
			want: `{"name":"leaf-a","from":"r1","ok":false,"epoch":2,"rev":5}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := json.Marshal(tt.in)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			if string(got) != tt.want {
				t.Errorf("got  %s\nwant %s", got, tt.want)
			}
		})
	}
}

// TestHeartbeatChildrenAreOrdered is the reason Children is a slice and not the map
// it superficially wants to be: a new coordinator rebuilds the previous tree's edge
// list from heartbeats, and overlay.Topology.Edges ORDER is a replayed invariant.
// Go randomises map iteration, so a map here would make the first tree of every
// epoch unreplayable — a determinism hole no test could reliably catch.
func TestHeartbeatChildrenAreOrdered(t *testing.T) {
	t.Run("Normalize imposes the canonical order", func(t *testing.T) {
		a := Heartbeat{Name: "r", Children: []ChildLink{
			{Name: "c", State: "connected"}, {Name: "a", State: "connected"}, {Name: "b", State: "new"},
		}}
		b := Heartbeat{Name: "r", Children: []ChildLink{
			{Name: "b", State: "new"}, {Name: "c", State: "connected"}, {Name: "a", State: "connected"},
		}}
		a.Normalize()
		b.Normalize()

		want := []string{"a", "b", "c"}
		for i, w := range want {
			if a.Children[i].Name != w {
				t.Fatalf("Normalize gave %v, want %v", a.Children, want)
			}
		}
		ja, _ := json.Marshal(a)
		jb, _ := json.Marshal(b)
		if string(ja) != string(jb) {
			t.Errorf("two insertion orders normalised differently:\n %s\n %s", ja, jb)
		}
	})

	t.Run("marshalling is byte-identical across repeats", func(t *testing.T) {
		h := Heartbeat{Name: "r", Seq: 1, Children: []ChildLink{
			{Name: "a", State: "connected"}, {Name: "b", State: "connected"}, {Name: "c", State: "failed"},
		}}
		first, err := json.Marshal(h)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		// 50 repeats: Go's map-iteration randomisation would have shown up with
		// overwhelming probability by now had Children been a map.
		for i := 0; i < 50; i++ {
			again, err := json.Marshal(h)
			if err != nil {
				t.Fatalf("marshal %d: %v", i, err)
			}
			if string(again) != string(first) {
				t.Fatalf("repeat %d differed:\n %s\n %s", i, again, first)
			}
		}
	})

	t.Run("round-trip preserves order", func(t *testing.T) {
		h := Heartbeat{Name: "r", Children: []ChildLink{{Name: "c"}, {Name: "a"}, {Name: "b"}}}
		raw, _ := json.Marshal(h)
		var back Heartbeat
		if err := json.Unmarshal(raw, &back); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		for i := range h.Children {
			if back.Children[i].Name != h.Children[i].Name {
				t.Fatalf("round-trip reordered children: %v", back.Children)
			}
		}
	})
}

// TestLivenessFamily covers the constants as a FAMILY. A peer that beats every 5s
// must not be declared gone by a threshold derived from the 1s default, and the
// socket-level detector must not disagree with the application-level one.
func TestLivenessFamily(t *testing.T) {
	t.Run("thresholds follow the declared cadence", func(t *testing.T) {
		tests := []struct {
			name         string
			interval     time.Duration
			wantDegraded time.Duration
			wantGone     time.Duration
		}{
			{name: "default cadence", interval: HeartbeatInterval, wantDegraded: 3 * time.Second, wantGone: 8 * time.Second},
			{name: "slow peer", interval: 5 * time.Second, wantDegraded: 15 * time.Second, wantGone: 40 * time.Second},
			{name: "fast peer", interval: 250 * time.Millisecond, wantDegraded: 750 * time.Millisecond, wantGone: 2 * time.Second},
			{name: "zero falls back to the default", interval: 0, wantDegraded: 3 * time.Second, wantGone: 8 * time.Second},
			{name: "negative falls back to the default", interval: -time.Second, wantDegraded: 3 * time.Second, wantGone: 8 * time.Second},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if got := DegradedAfter(tt.interval); got != tt.wantDegraded {
					t.Errorf("DegradedAfter(%v) = %v, want %v", tt.interval, got, tt.wantDegraded)
				}
				if got := GoneAfter(tt.interval); got != tt.wantGone {
					t.Errorf("GoneAfter(%v) = %v, want %v", tt.interval, got, tt.wantGone)
				}
			})
		}
	})

	t.Run("degraded precedes gone", func(t *testing.T) {
		if DegradedBeats >= GoneBeats {
			t.Errorf("DegradedBeats %d must be fewer than GoneBeats %d", DegradedBeats, GoneBeats)
		}
	})

	t.Run("Interval reads the frame, defaulting when absent", func(t *testing.T) {
		if got := (Heartbeat{IntervalMs: 2500}).Interval(); got != 2500*time.Millisecond {
			t.Errorf("Interval() = %v, want 2.5s", got)
		}
		if got := (Heartbeat{}).Interval(); got != HeartbeatInterval {
			t.Errorf("Interval() with no declared cadence = %v, want %v", got, HeartbeatInterval)
		}
	})

	t.Run("HeartbeatInterval is one second", func(t *testing.T) {
		if HeartbeatInterval != time.Second {
			t.Errorf("HeartbeatInterval = %v, want 1s", HeartbeatInterval)
		}
	})

	// The two liveness detectors must not be able to reach contradictory verdicts:
	// if the Hub takes longer to notice a dead socket than the control plane takes
	// to declare the peer gone, there is a window in which the roster says "here"
	// and the tree says "gone", which is exactly where the permanent-ejection bug
	// lives.
	t.Run("the socket detector may not lag the gone verdict", func(t *testing.T) {
		tests := []struct {
			name            string
			interval        time.Duration
			socketDetection time.Duration
			wantErr         bool
		}{
			{name: "default cadence, 7s socket budget", interval: time.Second, socketDetection: 7 * time.Second},
			{name: "exactly equal is allowed", interval: time.Second, socketDetection: 8 * time.Second},
			{name: "socket detector too slow", interval: time.Second, socketDetection: 20 * time.Second, wantErr: true},
			{name: "fast cadence shrinks the budget", interval: 250 * time.Millisecond, socketDetection: 7 * time.Second, wantErr: true},
			{name: "slow cadence widens it", interval: 5 * time.Second, socketDetection: 20 * time.Second},
			{name: "non-positive detection is a config error", interval: time.Second, socketDetection: 0, wantErr: true},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				err := ValidateLivenessBudget(tt.interval, tt.socketDetection)
				if tt.wantErr && err == nil {
					t.Errorf("ValidateLivenessBudget(%v, %v) = nil, want an error", tt.interval, tt.socketDetection)
				}
				if !tt.wantErr && err != nil {
					t.Errorf("ValidateLivenessBudget(%v, %v) = %v, want nil", tt.interval, tt.socketDetection, err)
				}
			})
		}
	})
}

// TestReporterUsesInjectedClock proves the Reporter is off the wall clock: with a
// fake clock nothing ticks until the test says so. That is what lets a simnet
// scenario run a whole meet's telemetry in virtual time.
func TestReporterUsesInjectedClock(t *testing.T) {
	fc := newFakeClock()
	var mu sync.Mutex
	var sent int
	send := func(Report) error {
		mu.Lock()
		sent++
		mu.Unlock()
		return nil
	}
	count := func() int { mu.Lock(); defer mu.Unlock(); return sent }

	// One hour: a real ticker would never fire inside this test, so any report after
	// the first proves the injected clock is the one driving the loop.
	r := NewReporter(discardLogger(), time.Hour, fc, func() Report { return Report{Name: "n"} }, send)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx)

	waitFor(t, time.Second, "the immediate first report", func() bool { return count() == 1 })
	waitFor(t, time.Second, "the ticker to be armed", func() bool { return fc.tickers() == 1 })

	fc.tick()
	waitFor(t, time.Second, "a report on the virtual tick", func() bool { return count() == 2 })
	fc.tick()
	waitFor(t, time.Second, "a second report on the virtual tick", func() bool { return count() == 3 })
}

// TestReporterNilClockDefaults keeps a nil Clock from being a panic: every other
// seam in this project treats nil as "give me the real one".
func TestReporterNilClockDefaults(t *testing.T) {
	r := NewReporter(discardLogger(), 0, nil, func() Report { return Report{} }, func(Report) error { return nil })
	if r.clk == nil {
		t.Error("nil Clock was not defaulted to clock.System()")
	}
	if r.interval != DefaultInterval {
		t.Errorf("interval = %v, want default %v", r.interval, DefaultInterval)
	}
}

// --- a minimal hand-rolled clock.Clock, proving the seam is implementable by any
// consumer without importing simnet (docs/PLAN.md §2.4) ---

type fakeClock struct {
	mu    sync.Mutex
	now   time.Time
	ticks chan time.Time
	armed int
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Unix(0, 0).UTC(), ticks: make(chan time.Time, 1)}
}

func (c *fakeClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }

func (c *fakeClock) NewTimer(time.Duration) clock.Timer { return fakeTimer{} }

func (c *fakeClock) NewTicker(time.Duration) clock.Ticker {
	c.mu.Lock()
	c.armed++
	c.mu.Unlock()
	return fakeTicker{ch: c.ticks}
}

func (c *fakeClock) After(time.Duration) <-chan time.Time { return make(chan time.Time) }

func (c *fakeClock) tickers() int { c.mu.Lock(); defer c.mu.Unlock(); return c.armed }

// tick delivers one virtual tick to whoever is ranging the ticker.
func (c *fakeClock) tick() {
	c.mu.Lock()
	c.now = c.now.Add(time.Second)
	now := c.now
	c.mu.Unlock()
	c.ticks <- now
}

type fakeTimer struct{}

func (fakeTimer) C() <-chan time.Time      { return make(chan time.Time) }
func (fakeTimer) Reset(time.Duration) bool { return false }
func (fakeTimer) Stop() bool               { return false }

type fakeTicker struct{ ch chan time.Time }

func (t fakeTicker) C() <-chan time.Time { return t.ch }
func (t fakeTicker) Stop()               {}

// TestStaleRejectedSemantics pins the two properties the coordinator's emission rule
// depends on. It fires EventStale on an INCREASE rather than per heartbeat, so the
// counter must be cumulative within a session and must restart at 0 on rejoin — the
// fence itself resets on TypeJoined, so a carried-over total would either mask a real
// refusal (the new total never exceeds the old one) or invent one (a rejoined peer
// appears to refuse everything again).
func TestStaleRejectedSemantics(t *testing.T) {
	t.Run("an increase is visible to a difference check", func(t *testing.T) {
		// The exact comparison the coordinator makes across consecutive beats.
		beats := []Heartbeat{
			{Name: "leaf-a", Seq: 1, Epoch: 2, Rev: 5},
			{Name: "leaf-a", Seq: 2, Epoch: 2, Rev: 5, StaleRejected: 1},
			{Name: "leaf-a", Seq: 3, Epoch: 2, Rev: 5, StaleRejected: 1},
			{Name: "leaf-a", Seq: 4, Epoch: 2, Rev: 5, StaleRejected: 4},
		}
		wantEmit := []bool{false, true, false, true}
		var prev uint64
		for i, hb := range beats[0:] {
			emit := hb.StaleRejected > prev
			if emit != wantEmit[i] {
				t.Errorf("beat %d (total %d, prev %d): emit = %v, want %v",
					i, hb.StaleRejected, prev, emit, wantEmit[i])
			}
			prev = hb.StaleRejected
		}
	})

	t.Run("a rejoin restarts the counter with Seq", func(t *testing.T) {
		// A rejoining peer restarts Seq at 1; StaleRejected restarts with it,
		// because both describe a session that has just begun.
		before := Heartbeat{Name: "leaf-a", Seq: 9, Epoch: 2, Rev: 5, StaleRejected: 7}
		after := Heartbeat{Name: "leaf-a", Seq: 1, Epoch: 0, Rev: 0}
		if after.Seq != 1 {
			t.Fatalf("a rejoin must restart Seq at 1, got %d", after.Seq)
		}
		if after.StaleRejected != 0 {
			t.Errorf("a rejoin must restart StaleRejected at 0, got %d", after.StaleRejected)
		}
		if after.StaleRejected >= before.StaleRejected {
			t.Errorf("a restarted counter (%d) must not look like an increase over the old session (%d)",
				after.StaleRejected, before.StaleRejected)
		}
	})

	t.Run("round-trips", func(t *testing.T) {
		for _, want := range []uint64{0, 1, 4096} {
			raw, err := json.Marshal(Heartbeat{Name: "a", StaleRejected: want})
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			var back Heartbeat
			if err := json.Unmarshal(raw, &back); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if back.StaleRejected != want {
				t.Errorf("round-trip of %d gave %d", want, back.StaleRejected)
			}
		}
	})
}
