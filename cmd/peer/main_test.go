package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
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
		if !adoptAnnouncement(testLogger(), "relay", f.AdoptAnnouncement, announcement(2, "relay", "p3")) {
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
		adoptAnnouncement(testLogger(), "relay", f.AdoptAnnouncement, announcement(5, "relay", "p3"))
		if adoptAnnouncement(testLogger(), "relay", f.AdoptAnnouncement, announcement(5, "leaf-a", "p7")) {
			t.Error("a duplicate epoch was adopted; a peer must not be re-pointed inside a term")
		}
		if adoptAnnouncement(testLogger(), "relay", f.AdoptAnnouncement, announcement(4, "leaf-a", "p7")) {
			t.Error("a stale epoch was adopted")
		}
		if f.CoordinatorID != "p3" {
			t.Errorf("CoordinatorID = %q, want p3 (a refused announcement must leave the fence untouched)", f.CoordinatorID)
		}
	})

	t.Run("a malformed body is dropped, not fatal", func(t *testing.T) {
		called := false
		got := adoptAnnouncement(testLogger(), "relay",
			func(uint64, string) bool { called = true; return true },
			[]byte("{not json"))
		if got || called {
			t.Error("a malformed announcement must not reach the fence")
		}
	})
}

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
