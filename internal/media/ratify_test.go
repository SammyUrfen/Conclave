package media

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/pion/webrtc/v4"

	"github.com/SammyUrfen/conclave/internal/metrics"
	"github.com/SammyUrfen/conclave/internal/overlay"
)

// reparentFixture builds a Router mid-move: attached to oldParent, with a session
// already registered to newParent so startReparent has something to resolve and
// startPeer is a no-op. Nothing here touches the network — the state machine is
// driven by EVENTS, which is exactly how pion drives it in production.
func reparentFixture(t *testing.T, topo *overlay.Topology, self, oldParent, newParent string) (*Router, chan metrics.Reparented) {
	t.Helper()
	reports := make(chan metrics.Reparented, 8)
	r := NewRouter(discardLog(), nil, RouterConfig{
		SelfName: self, Managed: true,
		OnReparented: func(rep metrics.Reparented) { reports <- rep },
	})
	r.topo = topo
	linkStaticPeer(t, r, "p-"+oldParent, oldParent)
	linkStaticPeer(t, r, "p-"+newParent, newParent)
	r.setParent(oldParent, "")
	r.startReparent(context.Background(), oldParent, newParent, true)
	if r.rp == nil {
		t.Fatal("no re-parent started")
	}
	return r, reports
}

// TestReparentRatifiesWithoutMediaWhenNothingIsExpected fixes the premise of §5.6
// step 4.
//
// The rule demands an ARRIVING track before a re-parent may be ratified. For a peer
// that only sends, that is not an unmet condition — it is an UNDEFINED one: it
// subscribes to nothing, so no track will ever arrive over that edge no matter how
// healthy it is. The rule silently assumed every peer has a downstream to observe,
// and a pure source consequently tore down the very leg it was successfully sending
// on, every time, until it was abandoned.
//
// Only the peer can tell: the coordinator has no field that says "I subscribe to
// nothing". The peer derives it from the tree — the set of origins that arrive over
// that edge — and when it is empty its evidence is its own live outbound sender.
//
// The second row is the load-bearing control. A relay orphaned by a correlated
// failure has healthy PeerConnections and nothing to forward, and that is precisely
// the case step 4 exists to reject; it expects media, so it must still wait for it.
func TestReparentRatifiesWithoutMediaWhenNothingIsExpected(t *testing.T) {
	// a is the new parent and a relay, so it publishes nothing of its own; s only
	// sends. Nothing will ever arrive over the s–a edge.
	pureSource := &overlay.Topology{
		Epoch: 1, Rev: 1, Root: "a",
		Edges: []overlay.Edge{{Parent: "a", Child: "s"}},
	}
	// v is a leaf, so it publishes: s does expect to receive over the same edge.
	withPeer := &overlay.Topology{
		Epoch: 1, Rev: 1, Root: "a",
		Edges: []overlay.Edge{{Parent: "a", Child: "s"}, {Parent: "a", Child: "v"}},
	}

	t.Run("a peer that subscribes to nothing ratifies on connect", func(t *testing.T) {
		r, reports := reparentFixture(t, pureSource, "s", "b", "a")
		r.onPeerState(context.Background(), "a", webrtc.PeerConnectionStateConnected)

		if r.rp != nil {
			t.Fatalf("still mid-move after connecting: %+v — a source cannot wait for a "+
				"track it never subscribed to, so the wait can only ever time out", r.rp)
		}
		if got := r.currentParent(); got != "a" {
			t.Errorf("parentInUse = %q, want a", got)
		}
		select {
		case rep := <-reports:
			if !rep.OK {
				t.Errorf("reported %+v, want OK — the move succeeded on every observable count", rep)
			}
			if rep.To != "a" || rep.From != "b" {
				t.Errorf("reported %+v, want b→a", rep)
			}
		default:
			t.Error("no report at all")
		}
	})

	t.Run("a peer that does expect media still waits for it", func(t *testing.T) {
		r, reports := reparentFixture(t, withPeer, "s", "b", "a")
		r.onPeerState(context.Background(), "a", webrtc.PeerConnectionStateConnected)

		if r.rp == nil {
			t.Fatal("ratified on ICE alone; this is exactly what §5.6 step 4 rejects — " +
				"reaching the new parent proves nothing about the new parent reaching the root")
		}
		if r.rp.phase != rpAwaitingMedia {
			t.Errorf("phase = %v, want awaiting-media", r.rp.phase)
		}
		select {
		case rep := <-reports:
			t.Errorf("reported %+v before any media arrived", rep)
		default:
		}
	})
}

// TestReparentKeepsAConnectedLegWhenMediaIsUnverified is the bug underneath the bug,
// and it stands even once the rule above is corrected.
//
// A peer that reports a failed ratification has told the truth about what it could
// OBSERVE. Destroying a live transport path on the strength of an unobservable
// condition converts "I cannot confirm this works" into "this definitely does not
// work" — and that is where the measured 27-second outage came from, because the leg
// being torn down was carrying the peer's own outbound media the whole time.
//
// So the two timeouts are not the same failure. Never connecting leaves nothing worth
// keeping; connecting and then not being able to verify leaves a working path.
func TestReparentKeepsAConnectedLegWhenMediaIsUnverified(t *testing.T) {
	topo := &overlay.Topology{
		Epoch: 1, Rev: 1, Root: "a",
		Edges: []overlay.Edge{{Parent: "a", Child: "s"}, {Parent: "a", Child: "v"}},
	}

	t.Run("a media timeout on a connected leg keeps it and reports honestly", func(t *testing.T) {
		r, reports := reparentFixture(t, topo, "s", "b", "a")
		ctx := context.Background()
		r.onPeerState(ctx, "a", webrtc.PeerConnectionStateConnected)
		gen := r.rp.gen

		r.handleInternal(ctx, routerEvent{kind: evReparentMedia, gen: gen})

		if !r.hasSession("a") {
			t.Fatal("the connected leg was torn down: a path we are SENDING on was destroyed " +
				"because we could not observe anything arriving back over it")
		}
		if got := r.currentParent(); got != "a" {
			t.Errorf("parentInUse = %q, want a — the move happened, it just could not be verified", got)
		}
		if r.rp != nil {
			t.Errorf("still mid-move: %+v", r.rp)
		}
		select {
		case rep := <-reports:
			if rep.OK {
				t.Error("reported OK; nothing was verified, and the coordinator must be told so")
			}
			if rep.To != "a" {
				t.Errorf("reported To = %q, want a — the peer IS attached there", rep.To)
			}
		default:
			t.Error("no report at all")
		}
	})

	t.Run("a connect timeout still tears down the half-open session", func(t *testing.T) {
		// The control: with no connection there is no working leg to protect, and
		// leaving the session behind would strand a peer in nobody's topology.
		r, reports := reparentFixture(t, topo, "s", "b", "a")
		ctx := context.Background()
		gen := r.rp.gen

		r.handleInternal(ctx, routerEvent{kind: evReparentConnect, gen: gen})

		if r.hasSession("a") {
			t.Error("a session that never connected was left behind")
		}
		if got := r.currentParent(); got != "b" {
			t.Errorf("parentInUse = %q, want b — an unrealised move must leave us where we were", got)
		}
		select {
		case rep := <-reports:
			if rep.OK || rep.To != "" {
				t.Errorf("reported %+v, want a failure with no new parent", rep)
			}
		default:
			t.Error("no report at all")
		}
	})
}

// TestPionTeardownErrorsAreNotLoggedAsFaults pins the log level of an EXPECTED
// outcome. Tearing a session down while a topology apply is in flight makes pion
// return ErrConnectionClosed from whatever was mid-flight, and logging that at ERROR
// made one ordinary migration look like dozens of faults — 48 of them in a single
// verified-working run. A log level that cries wolf is a defect in its own right.
//
// Anything that is NOT a concurrent teardown stays at ERROR, which is the half that
// keeps this from being a blanket downgrade.
func TestPionTeardownErrorsAreNotLoggedAsFaults(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want slog.Level
	}{
		{"a closed connection is expected during teardown", webrtc.ErrConnectionClosed, slog.LevelDebug},
		// Session wraps with %w, so the real call sites hand this shape over and the
		// check must see through it.
		{"a wrapped closed connection is the same outcome",
			fmt.Errorf("add forward track: %w", webrtc.ErrConnectionClosed), slog.LevelDebug},
		{"a message that merely mentions it is not", errors.New("add track: " + webrtc.ErrConnectionClosed.Error()), slog.LevelError},
		{"anything else is a real fault", errors.New("codec mismatch"), slog.LevelError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
			logPionError(log, "add forward track mid-call", tc.err)
			out := buf.String()
			want := "level=" + tc.want.String()
			if !strings.Contains(out, want) {
				t.Errorf("logged %q, want %s", strings.TrimSpace(out), want)
			}
		})
	}
}
