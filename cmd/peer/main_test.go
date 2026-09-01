package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/SammyUrfen/conclave/internal/arbiter"
	"github.com/SammyUrfen/conclave/internal/clock"
	"github.com/SammyUrfen/conclave/internal/logging"
	"github.com/SammyUrfen/conclave/internal/media"
	"github.com/SammyUrfen/conclave/internal/metrics"
	"github.com/SammyUrfen/conclave/internal/overlay"
	"github.com/SammyUrfen/conclave/internal/signaling"
	"github.com/SammyUrfen/conclave/internal/simnet"
)

func TestHealthURLFor(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    string
		wantErr bool
	}{
		{name: "full url", in: "http://localhost:9000", want: "http://localhost:9000/healthz"},
		{name: "https preserved", in: "https://conclave.example", want: "https://conclave.example/healthz"},
		{name: "trailing slash trimmed", in: "http://localhost:9000/", want: "http://localhost:9000/healthz"},
		{name: "bare host:port gets http", in: "localhost:9000", want: "http://localhost:9000/healthz"},
		{name: "whitespace trimmed", in: "  http://localhost:9000  ", want: "http://localhost:9000/healthz"},
		{name: "empty is error", in: "", wantErr: true},
		{name: "scheme only is error", in: "http://", wantErr: true},
		// Regression: these have a non-empty u.Host (":", ":9000") but no
		// hostname. They mirror the server's "-addr :9000" and previously slipped
		// through to build a hostless "http://:/healthz". Must be rejected.
		{name: "bare colon is error", in: ":", wantErr: true},
		{name: "port only is error", in: ":9000", wantErr: true},
		{name: "scheme and port no host is error", in: "http://:9000", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := healthURLFor(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("healthURLFor(%q): expected error, got %q", tt.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("healthURLFor(%q): unexpected error: %v", tt.in, err)
			}
			if got != tt.want {
				t.Errorf("healthURLFor(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

// --- WI-8: the Phase 5/6 peer wiring -----------------------------------------
//
// Everything below tests PURE helpers: flag parsing/validation, the two frame
// builders, and the heartbeat producer driven by a virtual clock. None of it opens
// a socket or a PeerConnection, which is the point — the parts of cmd/peer that can
// be wrong in a silent, hard-to-see way are exactly the parts that are pure.

// wantOptions is the frozen §10 default set for cmd/peer. It is spelled out in full
// rather than compared field-by-field so that ADDING a flag without a default here
// fails the test — a new knob with an unreviewed default is the failure mode this
// guards.
func wantOptions() options {
	return options{
		server:  "http://localhost:9000",
		level:   slog.LevelInfo,
		format:  logging.FormatText,
		timeout: 5 * time.Second,
		call:    false,
		cfg: callConfig{
			server:        "http://localhost:9000",
			room:          "default",
			heartbeat:     metrics.HeartbeatInterval,
			uploadKbps:    3000,
			nat:           overlay.NATDirect,
			backup:        true,
			coordinatable: true,
		},
	}
}

func TestParseArgsDefaults(t *testing.T) {
	got, err := parseArgs(nil)
	if err != nil {
		t.Fatalf("parseArgs(nil): unexpected error: %v", err)
	}
	if want := wantOptions(); !reflect.DeepEqual(got, want) {
		t.Errorf("parseArgs(nil) =\n\t%+v\nwant\n\t%+v", got, want)
	}
}

func TestParseArgsValidation(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr bool
	}{
		{name: "plain probe", args: nil},
		{name: "managed with a name", args: []string{"-call", "-managed", "-name", "relay"}},
		{name: "mesh call needs nothing", args: []string{"-call"}},

		{name: "unknown nat", args: []string{"-nat", "upnp"}, wantErr: true},
		{name: "unknown log level", args: []string{"-log-level", "trace"}, wantErr: true},
		{name: "unknown log format", args: []string{"-log-format", "yaml"}, wantErr: true},

		{name: "managed without a name", args: []string{"-call", "-managed"}, wantErr: true},
		{name: "topology without a name", args: []string{"-call", "-topology", "t.json"}, wantErr: true},
		{
			name:    "managed and topology together",
			args:    []string{"-call", "-managed", "-name", "relay", "-topology", "t.json"},
			wantErr: true,
		},

		{name: "zero heartbeat", args: []string{"-heartbeat", "0"}, wantErr: true},
		{name: "negative heartbeat", args: []string{"-heartbeat", "-1s"}, wantErr: true},
		// Sub-millisecond truncates to interval_ms=0 on the wire, which the
		// coordinator reads back as "the default" — a cadence the peer is not using.
		{name: "sub-millisecond heartbeat", args: []string{"-heartbeat", "500us"}, wantErr: true},
		{name: "one millisecond heartbeat is legal", args: []string{"-heartbeat", "1ms"}},

		{name: "negative upload budget", args: []string{"-upload-kbps", "-1"}, wantErr: true},
		{name: "zero upload budget is legal", args: []string{"-upload-kbps", "0"}},
		{name: "zero probe timeout", args: []string{"-timeout", "0"}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseArgs(tt.args)
			if tt.wantErr && err == nil {
				t.Fatalf("parseArgs(%q): expected an error, got none", tt.args)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("parseArgs(%q): unexpected error: %v", tt.args, err)
			}
		})
	}
}

// TestReportCarriesCoordinatable is the anti-INERT test for -coordinatable.
//
// It asserts on the MARSHALLED report, from argv, because the field's whole hazard
// is silence: absent (or false) decodes to false, arbiter.Score returns a hard 0 for
// every peer, and no peer is ever elected — with nothing logged anywhere. A test
// that only checked the Go struct would pass against a cmd/peer that never set the
// field at all, since the zero value and "declined" are the same bool.
func TestReportCarriesCoordinatable(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "default is willing and says so explicitly",
			args: []string{"-call", "-managed", "-name", "relay"},
			want: `{"name":"relay","upload_kbps":3000,"nat":"direct","coordinatable":true}`,
		},
		{
			name: "declined is on the wire as false, not absent",
			args: []string{"-call", "-managed", "-name", "relay", "-coordinatable=false"},
			want: `{"name":"relay","upload_kbps":3000,"nat":"direct","coordinatable":false}`,
		},
		{
			name: "a turn-bound leaf still declares willingness",
			args: []string{"-call", "-managed", "-name", "leaf-b", "-nat", "turn", "-upload-kbps", "500"},
			want: `{"name":"leaf-b","upload_kbps":500,"nat":"turn","coordinatable":true}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts, err := parseArgs(tt.args)
			if err != nil {
				t.Fatalf("parseArgs(%q): %v", tt.args, err)
			}
			body, err := json.Marshal(sampleReport(opts.cfg))
			if err != nil {
				t.Fatalf("marshal report: %v", err)
			}
			if string(body) != tt.want {
				t.Errorf("report JSON =\n\t%s\nwant\n\t%s", body, tt.want)
			}
		})
	}
}

// TestRouterConfigBackupPolarity is the anti-INERT test for -backup.
//
// It reads the field media ACTUALLY has, so it is the thing that will break loudly
// when RouterConfig.Backup is renamed to DisableBackup (§15.13) — which is exactly
// what should happen, because that rename inverts the polarity and cmd/peer is the
// single place the flip lives. A silently-disabled failover path is the wrong
// direction to fail in.
func TestRouterConfigBackupPolarity(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		wantEnabled bool
	}{
		{name: "default enables backup promotion", args: []string{"-call", "-managed", "-name", "relay"}, wantEnabled: true},
		{name: "explicit true", args: []string{"-call", "-managed", "-name", "relay", "-backup=true"}, wantEnabled: true},
		{name: "explicit false disables it", args: []string{"-call", "-managed", "-name", "relay", "-backup=false"}, wantEnabled: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts, err := parseArgs(tt.args)
			if err != nil {
				t.Fatalf("parseArgs(%q): %v", tt.args, err)
			}
			rc := routerConfigFor(opts.cfg, nil, nil, clock.System(),
				func(metrics.Reparented) {}, func([]byte) {})
			if got := backupEnabled(rc); got != tt.wantEnabled {
				t.Errorf("backup enabled = %v, want %v", got, tt.wantEnabled)
			}
			if rc.OnReparented == nil {
				t.Error("RouterConfig.OnReparented is nil: the peer would never report its own promotions")
			}
			if rc.OnCoordinator == nil {
				t.Error("RouterConfig.OnCoordinator is nil: the fence would never learn who the coordinator is")
			}
			if rc.Clock == nil {
				t.Error("RouterConfig.Clock is nil: the re-parent deadlines would fall back to the wall clock")
			}
			if rc.Managed != opts.cfg.managed || rc.SelfName != opts.cfg.name {
				t.Errorf("RouterConfig managed/name = %v/%q, want %v/%q",
					rc.Managed, rc.SelfName, opts.cfg.managed, opts.cfg.name)
			}
		})
	}
}

// backupEnabled reads the media field that means "promote the backup parent",
// through the inversion §15.13 introduced. Spelled once, here, so the assertions
// above read as intent ("backup is enabled") while still exercising the real field —
// a wiring that forgot the flip would report the exact opposite of the flag and
// every case in the table would fail.
func backupEnabled(rc media.RouterConfig) bool { return !rc.DisableBackup }

// TestRouterConfigCoordinatorGating: announcements are only wired where there is a
// control plane to obey, which is what media documents nil as meaning.
func TestRouterConfigCoordinatorGating(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantSet bool
	}{
		{name: "managed", args: []string{"-call", "-managed", "-name", "relay"}, wantSet: true},
		{name: "static tree", args: []string{"-call", "-topology", "t.json", "-name", "relay"}},
		{name: "mesh", args: []string{"-call"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts, err := parseArgs(tt.args)
			if err != nil {
				t.Fatalf("parseArgs(%q): %v", tt.args, err)
			}
			rc := routerConfigFor(opts.cfg, nil, nil, clock.System(),
				func(metrics.Reparented) {}, func([]byte) {})
			if got := rc.OnCoordinator != nil; got != tt.wantSet {
				t.Errorf("OnCoordinator set = %v, want %v", got, tt.wantSet)
			}
		})
	}
}

// TestAdoptAnnouncementAuthorizes is the anti-INERT test for the §6.5 fence, and it
// asserts the property rather than the plumbing: after cmd/peer has fed an
// announcement through, the SAME overlay.Fence type media.Router holds accepts a
// push from the announced coordinator and refuses one from anybody else.
//
// The "never adopted" case is the control, and it is the failure mode that exists
// today if this wiring is missing: media enforces Fence.Accept on every push, so an
// un-adopted fence refuses EVERYTHING — a peer fully connected and permanently
// treeless, with nothing above debug in the log.
func TestAdoptAnnouncementAuthorizes(t *testing.T) {
	announcement := func(epoch uint64, name, id string) []byte {
		body, err := json.Marshal(arbiter.Announcement{
			RoomID: "demo", Epoch: epoch, Coordinator: name, CoordinatorID: id,
			Reason: arbiter.ReasonBootstrap,
		})
		if err != nil {
			t.Fatalf("marshal announcement: %v", err)
		}
		return body
	}
	// push is a tree minted by "p3" at epoch 2 — the shape the fence must authorize.
	push := func(epoch, rev uint64) *overlay.Topology {
		return &overlay.Topology{Epoch: epoch, Rev: rev}
	}

	t.Run("adopting authorizes exactly the named coordinator", func(t *testing.T) {
		var f overlay.Fence
		adopted, _ := adoptAnnouncement(testLogger(), selfID("p3"), f.AdoptAnnouncement, announcement(2, "relay", "p3"))
		if !adopted {
			t.Fatal("a fresh announcement was not adopted")
		}
		if ok, reason := f.Accept("p3", push(2, 1)); !ok {
			t.Errorf("push from the announced coordinator rejected: %s", reason)
		}
		if ok, _ := f.Accept("p9", push(2, 2)); ok {
			t.Error("push from a peer the arbiter never named was ACCEPTED: the fence authorizes nothing")
		}
		// A higher epoch must be refused too: authority is raised only by an
		// announcement, never by the node claiming the job.
		if ok, _ := f.Accept("p3", push(3, 1)); ok {
			t.Error("push carrying a HIGHER epoch was accepted; that is self-promotion")
		}
	})

	t.Run("never adopted refuses everything", func(t *testing.T) {
		var f overlay.Fence
		if ok, _ := f.Accept("p3", push(2, 1)); ok {
			t.Fatal("an un-adopted fence accepted a push")
		}
	})

	t.Run("a stale or duplicate epoch is not adopted", func(t *testing.T) {
		var f overlay.Fence
		adoptAnnouncement(testLogger(), selfID("p3"), f.AdoptAnnouncement, announcement(5, "relay", "p3"))
		if dup, _ := adoptAnnouncement(testLogger(), selfID("p3"), f.AdoptAnnouncement, announcement(5, "leaf-a", "p7")); dup {
			t.Error("a duplicate epoch was adopted; a peer must not be re-pointed inside a term")
		}
		if stale, _ := adoptAnnouncement(testLogger(), selfID("p3"), f.AdoptAnnouncement, announcement(4, "leaf-a", "p7")); stale {
			t.Error("a stale epoch was adopted")
		}
		if f.CoordinatorID != "p3" {
			t.Errorf("CoordinatorID = %q, want p3 (a refused announcement must leave the fence untouched)", f.CoordinatorID)
		}
	})

	t.Run("a malformed body is dropped, not fatal", func(t *testing.T) {
		called := false
		got, _ := adoptAnnouncement(testLogger(), selfID("p3"),
			func(uint64, string) bool { called = true; return true },
			[]byte("{not json"))
		if got || called {
			t.Error("a malformed announcement must not reach the fence")
		}
	})

	// Self-recognition is by SERVER-ASSIGNED ID, never by -name. An announcement
	// names the coordinator by id, so a peer comparing it against its own name could
	// never recognise itself and would silently decline the job it was just given.
	t.Run("self-recognition is by id", func(t *testing.T) {
		cases := []struct {
			name     string
			id       string
			wantSelf bool
		}{
			{name: "this peer was named", id: "p3", wantSelf: true},
			{name: "another peer was named", id: "p7"},
			// SelfID is "" until the joined frame lands; "not me" is the safe
			// answer in that window, not a match against the empty coordinator id
			// of a vacancy announcement.
			{name: "before the joined frame", id: ""},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				var f overlay.Fence
				_, self := adoptAnnouncement(testLogger(), selfID(tc.id), f.AdoptAnnouncement,
					announcement(1, "relay", "p3"))
				if self != tc.wantSelf {
					t.Errorf("self = %v, want %v", self, tc.wantSelf)
				}
			})
		}
	})
}

// selfID builds the id provider adoptAnnouncement takes, so a test can pin the
// window before the joined frame lands as easily as a steady-state id.
func selfID(id string) func() string { return func() string { return id } }

// TestRealizedBeat pins the heartbeat's REALIZED half: what this peer has actually
// connected, which is what a coordinator promoted mid-call rebuilds the previous
// tree from (§6.6). Every field must pass through verbatim — a beat that reports
// intent instead of fact re-introduces the divergence rebuild-from-peers exists to
// avoid.
func TestRealizedBeat(t *testing.T) {
	tests := []struct {
		name string
		src  fakeOverlay
		want metrics.Heartbeat
	}{
		{
			name: "a relay reports its parent and its children",
			src: fakeOverlay{
				fence:  overlay.Fence{Epoch: 4, CoordinatorID: "p1", Rev: 9},
				parent: "root", state: "connected",
				children: []metrics.ChildLink{{Name: "leaf-a", State: "connected"}, {Name: "leaf-b", State: "connecting"}},
				stale:    3,
			},
			want: metrics.Heartbeat{
				Epoch: 4, Rev: 9, Parent: "root", ParentState: "connected",
				Children:      []metrics.ChildLink{{Name: "leaf-a", State: "connected"}, {Name: "leaf-b", State: "connecting"}},
				StaleRejected: 3,
			},
		},
		{
			name: "a leaf reports no children",
			src:  fakeOverlay{fence: overlay.Fence{Epoch: 4, Rev: 9}, parent: "relay", state: "connecting"},
			want: metrics.Heartbeat{Epoch: 4, Rev: 9, Parent: "relay", ParentState: "connecting"},
		},
		{
			// The pending target of a re-parent in flight is neither parent nor
			// child, and media reports it as neither. cmd/peer must NOT invent it:
			// filling it in as a child would hand the coordinator an edge pointing
			// the wrong way — our future parent listed as our current child.
			name: "a re-parent in flight still reports the OLD parent",
			src: fakeOverlay{
				fence:  overlay.Fence{Epoch: 5, Rev: 2},
				parent: "old-relay", state: "disconnected",
				children: []metrics.ChildLink{{Name: "leaf-c", State: "connected"}},
			},
			want: metrics.Heartbeat{
				Epoch: 5, Rev: 2, Parent: "old-relay", ParentState: "disconnected",
				Children: []metrics.ChildLink{{Name: "leaf-c", State: "connected"}},
			},
		},
		{
			// Passed through verbatim, at whatever total the fence currently holds.
			// The coordinator emits on an INCREASE, so clamping, smoothing or
			// carrying a value here would either suppress a real refusal or
			// manufacture one out of a peer that has refused nothing.
			name: "the refusal counter rides the same frame as the fence",
			src: fakeOverlay{
				fence: overlay.Fence{Epoch: 7, Rev: 3}, parent: "relay", state: "connected", stale: 12,
			},
			want: metrics.Heartbeat{
				Epoch: 7, Rev: 3, Parent: "relay", ParentState: "connected", StaleRejected: 12,
			},
		},
		{
			// The healthy case, and the reason the gap was invisible: 0 is what a
			// system with a working fence looks like on every beat.
			name: "a peer that has refused nothing reports zero",
			src:  fakeOverlay{fence: overlay.Fence{Epoch: 1, Rev: 1}, parent: "relay", state: "connected"},
			want: metrics.Heartbeat{Epoch: 1, Rev: 1, Parent: "relay", ParentState: "connected"},
		},
		{
			name: "the root has no parent",
			src:  fakeOverlay{fence: overlay.Fence{Epoch: 1, Rev: 1}, children: []metrics.ChildLink{{Name: "relay", State: "connected"}}},
			want: metrics.Heartbeat{Epoch: 1, Rev: 1, Children: []metrics.ChildLink{{Name: "relay", State: "connected"}}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := realizedBeat(&tt.src); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("realizedBeat =\n\t%+v\nwant\n\t%+v", got, tt.want)
			}
		})
	}
}

// fakeOverlay is a stand-in for media.Router's two read-only accessors. The
// compile-time assertion below is what keeps the fake honest: if the real Router's
// signatures drift, cmd/peer stops building rather than quietly beating stale data.
type fakeOverlay struct {
	fence    overlay.Fence
	parent   string
	state    string
	children []metrics.ChildLink
	stale    uint64
}

func (f *fakeOverlay) Fence() overlay.Fence { return f.fence }
func (f *fakeOverlay) Realized() (string, string, []metrics.ChildLink) {
	return f.parent, f.state, f.children
}
func (f *fakeOverlay) StaleRejected() uint64 { return f.stale }

var _ overlayReporter = (*fakeOverlay)(nil)
var _ overlayReporter = (*media.Router)(nil)

func TestShouldBeat(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want bool
	}{
		{name: "managed peer beats", args: []string{"-call", "-managed", "-name", "relay"}, want: true},
		{name: "static tree peer beats", args: []string{"-call", "-topology", "t.json", "-name", "relay"}, want: true},
		{name: "named mesh peer beats", args: []string{"-call", "-name", "solo"}, want: true},
		{name: "nameless mesh peer does not", args: []string{"-call"}, want: false},
		{name: "probe mode does not", args: nil, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts, err := parseArgs(tt.args)
			if err != nil {
				t.Fatalf("parseArgs(%q): %v", tt.args, err)
			}
			if got := shouldBeat(opts.call, opts.cfg); got != tt.want {
				t.Errorf("shouldBeat = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestBeaterFrames pins what one beat carries. The identity fields are stamped by
// the beater AFTER the realized-state provider runs, so a provider cannot file
// liveness under another name or misdeclare the cadence the coordinator computes its
// thresholds from.
func TestBeaterFrames(t *testing.T) {
	realized := func() metrics.Heartbeat {
		return metrics.Heartbeat{
			Name: "impostor", Seq: 99, IntervalMs: 7,
			Epoch: 3, Rev: 11,
			Parent: "root", ParentState: "connected",
			Children: []metrics.ChildLink{
				{Name: "leaf-b", State: "connected"},
				{Name: "leaf-a", State: "connecting"},
			},
		}
	}
	b := newBeater(testLogger(), "relay", 2500*time.Millisecond, clock.System(), realized, nil)

	first := b.next()
	if first.Seq != 1 {
		t.Errorf("first beat Seq = %d, want 1 (a per-peer counter starting at 1)", first.Seq)
	}
	if first.Name != "relay" {
		t.Errorf("Name = %q, want %q (the beater stamps it, the provider may not)", first.Name, "relay")
	}
	if first.IntervalMs != 2500 {
		t.Errorf("IntervalMs = %d, want 2500 (-heartbeat must reach the wire)", first.IntervalMs)
	}
	if first.Epoch != 3 || first.Rev != 11 || first.Parent != "root" || first.ParentState != "connected" {
		t.Errorf("realized fields dropped: %+v", first)
	}
	if len(first.Children) != 2 || first.Children[0].Name != "leaf-a" {
		t.Errorf("Children = %+v, want ascending by name (the §8.1 ordering invariant)", first.Children)
	}
	if second := b.next(); second.Seq != 2 {
		t.Errorf("second beat Seq = %d, want 2", second.Seq)
	}

	t.Run("no provider still beats", func(t *testing.T) {
		nb := newBeater(testLogger(), "leaf-a", time.Second, clock.System(), nil, nil)
		h := nb.next()
		if h.Name != "leaf-a" || h.Seq != 1 || h.IntervalMs != 1000 {
			t.Errorf("cadence-only beat = %+v", h)
		}
	})
}

// TestBeaterRunCadence is the anti-INERT test for the heartbeat itself: WI-3's
// coordinator reaps a member that sends NO frame within GoneBeats cadences of
// joining, so a peer that does not beat is ejected from every meet it joins. The
// virtual clock makes "it beats immediately, then once per interval" an assertion
// rather than a sleep.
func TestBeaterRunCadence(t *testing.T) {
	clk := simnet.NewVirtualClock(time.Unix(0, 0).UTC())
	beats := make(chan metrics.Heartbeat, 16)
	b := newBeater(testLogger(), "relay", time.Second, clk, nil, func(h metrics.Heartbeat) error {
		beats <- h
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); b.run(ctx) }()

	// The FIRST beat must not wait for a tick: a peer that stays silent for one
	// cadence after joining has already burned an eighth of its gone budget.
	first := recvBeat(t, beats)
	if first.Seq != 1 {
		t.Fatalf("first beat Seq = %d, want 1", first.Seq)
	}
	waitFor(t, "the ticker to be armed", func() bool { return clk.Pending() == 1 })

	for want := uint64(2); want <= 4; want++ {
		clk.Advance(time.Second)
		if got := recvBeat(t, beats); got.Seq != want {
			t.Fatalf("beat Seq = %d, want %d", got.Seq, want)
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("beater.run did not return after its context was cancelled")
	}
	waitFor(t, "the ticker to be stopped", func() bool { return clk.Pending() == 0 })
}

// TestBeaterSurvivesSendFailure: liveness is best-effort on the wire but must never
// stop beating. A transient send error that killed the loop would guarantee the
// ejection the beat exists to prevent.
func TestBeaterSurvivesSendFailure(t *testing.T) {
	var sent int
	b := newBeater(testLogger(), "relay", time.Second, clock.System(), nil, func(metrics.Heartbeat) error {
		sent++
		return errors.New("socket wedged")
	})
	b.beat()
	b.beat()
	if sent != 2 {
		t.Fatalf("sent %d beats, want 2 (a send error must not stop the cadence)", sent)
	}
	if b.next().Seq != 3 {
		t.Fatal("the sequence must keep advancing across a failed send")
	}
}

func TestControlFrame(t *testing.T) {
	tests := []struct {
		name string
		typ  signaling.Type
		body any
		want string
	}{
		{
			name: "heartbeat",
			typ:  signaling.TypeHeartbeat,
			body: metrics.Heartbeat{Name: "relay", Seq: 2, IntervalMs: 1000},
			want: `{"name":"relay","seq":2,"interval_ms":1000,"epoch":0,"rev":0}`,
		},
		{
			name: "reparented",
			typ:  signaling.TypeReparented,
			body: metrics.Reparented{Name: "leaf-a", From: "relay", To: "root", OK: true, Epoch: 2, Rev: 5},
			want: `{"name":"leaf-a","from":"relay","to":"root","ok":true,"epoch":2,"rev":5}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg, err := controlFrame(tt.typ, tt.body)
			if err != nil {
				t.Fatalf("controlFrame: %v", err)
			}
			if msg.Type != tt.typ {
				t.Errorf("Type = %q, want %q", msg.Type, tt.typ)
			}
			if string(msg.Payload) != tt.want {
				t.Errorf("Payload =\n\t%s\nwant\n\t%s", msg.Payload, tt.want)
			}
			if msg.From != "" || msg.To != "" {
				t.Error("a peer must not stamp From/To: the server does that")
			}
		})
	}
}

// TestReparentSenderNeverBlocks is the anti-INERT test for OnReparented, and it
// tests the property that actually matters: the callback runs on media's Run
// goroutine — the same goroutine that must process the answer completing the
// re-parent — so a blocking send there could deadlock the very promotion it is
// reporting. Overflow must drop and log, never block.
func TestReparentSenderNeverBlocks(t *testing.T) {
	var mu sync.Mutex
	var got []metrics.Reparented
	release := make(chan struct{})
	s := newReparentSender(testLogger(), func(rep metrics.Reparented) error {
		<-release
		mu.Lock()
		got = append(got, rep)
		mu.Unlock()
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); s.run(ctx) }()

	// Post far more than the queue can hold while the drain is blocked. If post
	// blocked, this loop would never finish and the test would time out.
	for i := 0; i < reparentQueueDepth*4; i++ {
		s.post(metrics.Reparented{Name: "leaf-a", From: "relay", OK: false, Reason: "stranded"})
	}
	if n := s.Dropped(); n == 0 {
		t.Fatal("expected the sender to drop on overflow, not block")
	}

	close(release)
	waitFor(t, "the queued reports to drain", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got) > 0
	})
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("reparentSender.run did not return after its context was cancelled")
	}
}

// TestInertManagedFlags: a flag that silently does nothing is worse than one that
// errors, so cmd/peer names the managed-only flags a user set outside managed mode.
// They are warnings, not errors, because their defaults are non-zero and would
// otherwise make every plain -call invocation fail.
func TestInertManagedFlags(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want []string
	}{
		{name: "managed uses them all", args: []string{"-call", "-managed", "-name", "relay", "-coordinatable=false"}},
		{name: "defaults are never inert", args: []string{"-call"}},
		{
			name: "explicitly set outside managed mode",
			args: []string{"-call", "-coordinatable=false", "-upload-kbps", "100"},
			want: []string{"coordinatable", "upload-kbps"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts, err := parseArgs(tt.args)
			if err != nil {
				t.Fatalf("parseArgs(%q): %v", tt.args, err)
			}
			if !reflect.DeepEqual(opts.inert, tt.want) {
				t.Errorf("inert flags = %v, want %v", opts.inert, tt.want)
			}
		})
	}
}

// --- test helpers ------------------------------------------------------------

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func recvBeat(t *testing.T, ch <-chan metrics.Heartbeat) metrics.Heartbeat {
	t.Helper()
	select {
	case h := <-ch:
		return h
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a heartbeat")
		return metrics.Heartbeat{}
	}
}

// waitForSlow is waitFor with a deadline sized for real ICE on loopback rather than
// for a goroutine handoff. Split from waitFor so the fast assertions keep a tight
// deadline: a 30 s budget everywhere would turn a genuine hang into a slow test
// instead of a failure.
func waitForSlow(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// waitFor polls cond until it holds or the deadline passes. Polling is the right
// tool for the handful of assertions here that straddle a goroutine boundary the
// virtual clock does not itself synchronise (a ticker being armed, a queue
// draining); the beats themselves are asserted on a channel, not polled.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// TestPeerBeatsAndDeclaresOnTheWire is the end-to-end proof for the two features
// that fail SILENTLY when they are merely unwired.
//
// It runs the real runCall against a real signaling.Hub over a real WebSocket and
// reads what the server's Observer actually receives. Nothing here is mocked at the
// wire, which is the point: a unit test on sampleReport proves the struct marshals,
// not that the frame is ever sent — and "the frame is never sent" is exactly the
// shape of both bugs. No PeerConnection is created, because this peer is alone in
// the meet, so the test costs a socket and no media.
func TestPeerBeatsAndDeclaresOnTheWire(t *testing.T) {
	tests := []struct {
		name              string
		coordinatable     string
		wantCoordinatable bool
	}{
		{name: "willing by default", coordinatable: "-coordinatable=true", wantCoordinatable: true},
		{name: "declining", coordinatable: "-coordinatable=false", wantCoordinatable: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			obs := &captureObserver{}
			hub := signaling.NewHub(testLogger())
			hub.SetObserver(obs)
			mux := http.NewServeMux()
			mux.HandleFunc("GET /ws", hub.ServeWS)
			srv := httptest.NewServer(mux)
			defer srv.Close()

			// A 20 ms cadence keeps the test quick. It is legal configuration — the
			// coordinator sizes every threshold from the cadence a peer DECLARES, and
			// floors it at the socket-detection budget, so a fast beater is safe.
			opts, err := parseArgs([]string{
				"-call", "-managed", "-name", "relay", "-server", srv.URL,
				"-room", "wire-test", "-heartbeat", "20ms", "-upload-kbps", "1500",
				tt.coordinatable,
			})
			if err != nil {
				t.Fatalf("parseArgs: %v", err)
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- runCall(ctx, testLogger(), opts.cfg) }()

			waitFor(t, "a telemetry report and two heartbeats on the wire", func() bool {
				return obs.count(signaling.TypeMetrics) >= 1 && obs.count(signaling.TypeHeartbeat) >= 2
			})

			var rep metrics.Report
			if err := json.Unmarshal(obs.last(signaling.TypeMetrics), &rep); err != nil {
				t.Fatalf("decode report: %v", err)
			}
			if rep.Coordinatable != tt.wantCoordinatable {
				t.Errorf("report.Coordinatable on the wire = %v, want %v", rep.Coordinatable, tt.wantCoordinatable)
			}
			if rep.Name != "relay" || rep.UploadKbps != 1500 {
				t.Errorf("report = %+v, want name=relay upload_kbps=1500", rep)
			}

			var hb metrics.Heartbeat
			if err := json.Unmarshal(obs.last(signaling.TypeHeartbeat), &hb); err != nil {
				t.Fatalf("decode heartbeat: %v", err)
			}
			if hb.Name != "relay" {
				t.Errorf("heartbeat name = %q, want relay", hb.Name)
			}
			if hb.Seq < 2 {
				t.Errorf("heartbeat seq = %d, want the counter to be advancing", hb.Seq)
			}
			// The cadence must be the one -heartbeat asked for: the coordinator reads
			// it back to size this peer's degraded/gone thresholds, and a wrong value
			// here declares a perfectly healthy peer gone.
			if got := hb.Interval(); got != 20*time.Millisecond {
				t.Errorf("declared cadence = %v, want 20ms (-heartbeat must reach interval_ms)", got)
			}

			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("runCall returned %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("runCall did not return after its context was cancelled")
			}
		})
	}
}

// captureObserver records the control-plane frames the Hub terminates, so the test
// can assert on what the SERVER received rather than on what the peer intended.
type captureObserver struct {
	mu   sync.Mutex
	seen map[signaling.Type][][]byte
}

func (o *captureObserver) record(t signaling.Type, payload []byte) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.seen == nil {
		o.seen = make(map[signaling.Type][][]byte)
	}
	o.seen[t] = append(o.seen[t], append([]byte(nil), payload...))
}

func (o *captureObserver) count(t signaling.Type) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.seen[t])
}

func (o *captureObserver) all(t signaling.Type) [][]byte {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([][]byte(nil), o.seen[t]...)
}

func (o *captureObserver) last(t signaling.Type) []byte {
	o.mu.Lock()
	defer o.mu.Unlock()
	frames := o.seen[t]
	if len(frames) == 0 {
		return nil
	}
	return frames[len(frames)-1]
}

func (o *captureObserver) PeerJoined(string, string, string) {}
func (o *captureObserver) PeerLeft(string, string)           {}
func (o *captureObserver) Metrics(_, _ string, payload []byte) {
	o.record(signaling.TypeMetrics, payload)
}
func (o *captureObserver) Heartbeat(_, _ string, payload []byte) {
	o.record(signaling.TypeHeartbeat, payload)
}
func (o *captureObserver) Reparented(_, _ string, payload []byte) {
	o.record(signaling.TypeReparented, payload)
}

// TestPeerReportsRealizedTreeOnTheWire is the end-to-end proof for the REALIZED half
// of the heartbeat — the last piece of §6.6.
//
// It drives the real Phase 5 path: three MANAGED peers join, the test plays arbiter
// (broadcasting a coordinator announcement) and coordinator (pushing a tree), the
// peers fence it, apply it, negotiate real PeerConnections, and then the assertions
// read what the server's Observer actually received. The relay must name both its
// children, ascending BY NAME, and each leaf must name the relay as its parent with
// a genuine pion connection state.
//
// Nothing here is mocked at the wire, because "the realized fields never leave the
// process" is precisely the bug being excluded — and it is the bug that makes a
// Phase 6 handover report ReasonNoRealizedState and degrade to a full rebuild
// instead of reconstructing the tree it already had.
func TestPeerReportsRealizedTreeOnTheWire(t *testing.T) {
	const room = "realized-test"

	obs := &captureObserver{}
	hub := signaling.NewHub(testLogger())
	hub.SetObserver(obs)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ws", hub.ServeWS)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	// Cancel BEFORE joining, in one defer: the peers only return once their context
	// is cancelled, so the reverse order would deadlock every failure path.
	defer func() {
		cancel()
		wg.Wait()
	}()

	// leaf-b first, then leaf-a, then the relay: joining out of alphabetical order is
	// what makes the ordering assertion mean something.
	for _, name := range []string{"leaf-b", "leaf-a", "relay"} {
		opts, err := parseArgs([]string{
			"-call", "-managed", "-name", name,
			"-server", srv.URL, "-room", room, "-heartbeat", "50ms",
		})
		if err != nil {
			t.Fatalf("parseArgs(%s): %v", name, err)
		}
		wg.Add(1)
		go func() { defer wg.Done(); _ = runCall(ctx, testLogger(), opts.cfg) }()
	}

	waitForSlow(t, "all three peers to join the meet", func() bool {
		return len(hub.Roster(room)) == 3
	})

	// The test is the arbiter. Without this the peers' fences stay at the zero value
	// and reject every push — the exact silent failure §6.5 describes — so the
	// announcement is load-bearing here, not scene-setting.
	ann, err := json.Marshal(arbiter.Announcement{
		RoomID: room, Epoch: 1, Coordinator: "", CoordinatorID: signaling.ServerID,
		Reason: arbiter.ReasonBootstrap,
	})
	if err != nil {
		t.Fatalf("marshal announcement: %v", err)
	}
	if n := hub.SendRoom(room, signaling.Message{Type: signaling.TypeCoordinator, Payload: ann}); n != 3 {
		t.Fatalf("announcement reached %d peers, want 3", n)
	}

	// The test is also the coordinator: relay is the root, serving both leaves.
	tree, err := json.Marshal(overlay.Topology{
		Epoch: 1, Rev: 1, Root: "relay",
		Edges: []overlay.Edge{{Parent: "relay", Child: "leaf-a"}, {Parent: "relay", Child: "leaf-b"}},
	})
	if err != nil {
		t.Fatalf("marshal topology: %v", err)
	}
	for _, p := range hub.Roster(room) {
		if !hub.SendTo(room, p.ID, signaling.Message{Type: signaling.TypeTopology, Payload: tree}) {
			t.Fatalf("could not push the tree to %s", p.Name)
		}
	}

	// Real ICE on loopback needs a moment; at a 50 ms cadence the heartbeat carrying
	// the settled state follows promptly once the edges connect.
	waitForSlow(t, "every peer to report a connected realized tree", func() bool {
		relay, ok := lastHeartbeatFrom(t, obs, "relay")
		if !ok || len(relay.Children) != 2 {
			return false
		}
		for _, c := range relay.Children {
			if c.State != connectedState {
				return false
			}
		}
		for _, leaf := range []string{"leaf-a", "leaf-b"} {
			hb, ok := lastHeartbeatFrom(t, obs, leaf)
			if !ok || hb.Parent != "relay" || hb.ParentState != connectedState {
				return false
			}
		}
		return true
	})

	relay := mustHeartbeatFrom(t, obs, "relay")
	wantChildren := []metrics.ChildLink{
		{Name: "leaf-a", State: connectedState},
		{Name: "leaf-b", State: connectedState},
	}
	if !reflect.DeepEqual(relay.Children, wantChildren) {
		t.Errorf("relay children = %+v, want %+v (by NAME, ascending)", relay.Children, wantChildren)
	}
	if relay.Parent != "" || relay.ParentState != "" {
		t.Errorf("the root reported a parent: %q/%q", relay.Parent, relay.ParentState)
	}
	// The fence half rides the same frame: a coordinator uses it to spot a peer
	// running behind and re-push to it specifically.
	if relay.Epoch != 1 || relay.Rev != 1 {
		t.Errorf("relay reported epoch/rev %d/%d, want 1/1", relay.Epoch, relay.Rev)
	}

	for _, leaf := range []string{"leaf-a", "leaf-b"} {
		hb := mustHeartbeatFrom(t, obs, leaf)
		if hb.Parent != "relay" {
			t.Errorf("%s parent = %q, want relay (by NAME, not a runtime id)", leaf, hb.Parent)
		}
		if hb.ParentState != connectedState {
			t.Errorf("%s parent_state = %q, want %q", leaf, hb.ParentState, connectedState)
		}
		if len(hb.Children) != 0 {
			t.Errorf("%s reported children %+v; a leaf has none", leaf, hb.Children)
		}
	}

	// --- the fence's refusal counter, end to end -----------------------------
	//
	// Every other link in this chain exists — media counts refusals, the
	// coordinator emits on an increase, the dashboard renders the total — so the
	// only way the panel can read 0 forever is if the number never leaves the peer.
	// A healthy system also reads 0, which is why this has to be asserted against a
	// peer that has genuinely refused something.
	if n := mustHeartbeatFrom(t, obs, "relay").StaleRejected; n != 0 {
		t.Fatalf("relay reported %d refusals before any bogus push; want 0", n)
	}

	// Two pushes that are perfectly well-formed and strictly NEWER (epoch 1, rev 2
	// and 3) but come from a sender the arbiter never named. Only the authorization
	// clause can reject them, so the count is unambiguous — an ordering-only fence
	// would accept both and the assertion below would read 0.
	relayID := peerIDFor(t, hub, room, "relay")
	for _, rev := range []uint64{2, 3} {
		bogus, err := json.Marshal(overlay.Topology{
			Epoch: 1, Rev: rev, Root: "leaf-a",
			Edges: []overlay.Edge{{Parent: "leaf-a", Child: "relay"}, {Parent: "leaf-a", Child: "leaf-b"}},
		})
		if err != nil {
			t.Fatalf("marshal bogus topology: %v", err)
		}
		// An explicit From survives stampServer, so this arrives exactly as a
		// peer-originated push would: a node claiming the coordinator's job.
		if !hub.SendTo(room, relayID, signaling.Message{
			Type: signaling.TypeTopology, From: "impostor", Payload: bogus,
		}) {
			t.Fatal("could not deliver the bogus push")
		}
	}

	waitForSlow(t, "the relay to report both refusals on a heartbeat", func() bool {
		hb, ok := lastHeartbeatFrom(t, obs, "relay")
		return ok && hb.StaleRejected == 2
	})
	if got := mustHeartbeatFrom(t, obs, "relay").StaleRejected; got != 2 {
		t.Errorf("relay stale_rejected = %d, want 2", got)
	}
	// The count is per peer and read from that peer's own fence, never fabricated:
	// the leaves were sent nothing and must still report nothing.
	for _, leaf := range []string{"leaf-a", "leaf-b"} {
		if n := mustHeartbeatFrom(t, obs, leaf).StaleRejected; n != 0 {
			t.Errorf("%s stale_rejected = %d, want 0 (it was sent no bogus push)", leaf, n)
		}
	}
	// The refused pushes must not have been APPLIED: the relay is still the root
	// serving both leaves, not a child of leaf-a.
	if hb := mustHeartbeatFrom(t, obs, "relay"); hb.Parent != "" || len(hb.Children) != 2 {
		t.Errorf("a refused push changed the tree: relay parent=%q children=%+v", hb.Parent, hb.Children)
	}
}

// peerIDFor resolves a topology name to the runtime id the server assigned it.
func peerIDFor(t *testing.T, hub *signaling.Hub, room, name string) string {
	t.Helper()
	for _, p := range hub.Roster(room) {
		if p.Name == name {
			return p.ID
		}
	}
	t.Fatalf("no peer named %q in the meet", name)
	return ""
}

// connectedState is pion's PeerConnectionState string for a live edge. Spelled once
// so the assertions read as intent rather than as a magic literal repeated six times.
const connectedState = "connected"

// lastHeartbeatFrom returns the most recent heartbeat whose body names peer. The
// Observer is keyed by the server-assigned id, so the NAME in the payload is what
// identifies the sender here — which is also the property under test.
func lastHeartbeatFrom(t *testing.T, obs *captureObserver, name string) (metrics.Heartbeat, bool) {
	t.Helper()
	frames := obs.all(signaling.TypeHeartbeat)
	for i := len(frames) - 1; i >= 0; i-- {
		var hb metrics.Heartbeat
		if err := json.Unmarshal(frames[i], &hb); err != nil {
			continue
		}
		if hb.Name == name {
			return hb, true
		}
	}
	return metrics.Heartbeat{}, false
}

func mustHeartbeatFrom(t *testing.T, obs *captureObserver, name string) metrics.Heartbeat {
	t.Helper()
	hb, ok := lastHeartbeatFrom(t, obs, name)
	if !ok {
		t.Fatalf("no heartbeat from %q reached the server", name)
	}
	return hb
}
