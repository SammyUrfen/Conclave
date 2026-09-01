package media

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/SammyUrfen/conclave/internal/signaling"
)

// TestRouterSurfacesControlFrames closes the seam an in-process coordinator lives on.
//
// The Router is the sole consumer of the peer's one signaling stream (§8 forbids a
// second control link), and NewRouter takes a concrete *signaling.Client, so a host
// cannot interpose — Transport is per-Session, not per-Router. Four frame types had
// no case in handle at all, which meant an elected peer adopted the coordinator role
// and then heard nothing its control loop runs on: no membership snapshot, no
// metrics, no heartbeats, no reparent reports. The meet never got repaired, so
// electing a coordinator made things strictly worse than not electing one.
//
// Discrimination: the four types below are exactly the ones a host observing the
// Router could not see before, so every one of them is a row that fails without the
// callback. The two lifecycle frames are the other half — they must be surfaced AS
// WELL AS, not INSTEAD OF, the Router's own bookkeeping, which the second and third
// subtests pin.
func TestRouterSurfacesControlFrames(t *testing.T) {
	ctx := context.Background()

	// Managed with no topology pushed yet: maybeStartPeer returns immediately, so
	// this test is about frame routing and nothing else.
	newRouterWithSink := func() (*Router, *[]signaling.Message) {
		var got []signaling.Message
		r := NewRouter(discardLog(), nil, RouterConfig{
			SelfName: "self", Managed: true,
			OnControlFrame: func(msg signaling.Message) { got = append(got, msg) },
		})
		return r, &got
	}

	t.Run("every control frame reaches the host with its sender preserved", func(t *testing.T) {
		r, got := newRouterWithSink()

		// The four that had no case at all, plus the two the Router already acts on.
		// Each carries a DISTINCT From: a forwarded heartbeat arrives stamped with
		// the ORIGINAL peer's id, which is what coordinator.Heartbeat keys on, so a
		// callback that passed along the wrong id would be worse than no callback.
		in := []signaling.Message{
			{Type: signaling.TypeMembership, From: signaling.ServerID,
				Peers: []signaling.Peer{{ID: "p9", Name: "zoe"}}},
			{Type: signaling.TypeMetrics, From: "p9", Payload: json.RawMessage(`{"name":"zoe"}`)},
			{Type: signaling.TypeHeartbeat, From: "p8", Payload: json.RawMessage(`{"name":"fred"}`)},
			{Type: signaling.TypeReparented, From: "p7", Payload: json.RawMessage(`{"name":"bob"}`)},
			{Type: signaling.TypePeerJoined, From: "p9", Name: "zoe"},
			{Type: signaling.TypePeerLeft, From: "p9", Name: "zoe"},
		}
		for _, msg := range in {
			r.handle(ctx, msg)
		}
		if !reflect.DeepEqual(*got, in) {
			t.Fatalf("control frames surfaced:\n got %+v\nwant %+v", *got, in)
		}
	})

	t.Run("the frames the Router owns are not surfaced", func(t *testing.T) {
		// The callback is a seam for the control plane, not a tap on the whole
		// stream. Media signaling and the Router's own authority frames stay inside,
		// or a host would have to learn to ignore them — and an offer leaking to a
		// coordinator loop is a confusing failure to debug.
		r, got := newRouterWithSink()
		for _, msg := range []signaling.Message{
			{Type: signaling.TypeJoined, To: "p1"},
			{Type: signaling.TypeOffer, From: "p2"},
			{Type: signaling.TypeAnswer, From: "p2"},
			{Type: signaling.TypeCandidate, From: "p2"},
			{Type: signaling.TypeTopology, From: "p2", Payload: json.RawMessage(`{}`)},
			{Type: signaling.TypeCoordinator, From: signaling.ServerID, Payload: json.RawMessage(`{}`)},
			{Type: signaling.TypeError, Error: "boom"},
		} {
			r.handle(ctx, msg)
		}
		if len(*got) != 0 {
			t.Errorf("surfaced %+v; the callback must carry control frames only", *got)
		}
	})

	t.Run("surfacing a lifecycle frame does not replace the Router acting on it", func(t *testing.T) {
		r, got := newRouterWithSink()

		r.handle(ctx, signaling.Message{Type: signaling.TypePeerJoined, From: "p9", Name: "zoe"})
		if id := r.idForName("zoe"); id != "p9" {
			t.Errorf("after peer-joined, idForName(zoe) = %q, want p9 — the Router's own "+
				"name↔id bookkeeping must still run", id)
		}
		r.handle(ctx, signaling.Message{Type: signaling.TypePeerLeft, From: "p9", Name: "zoe"})
		if id := r.idForName("zoe"); id != "" {
			t.Errorf("after peer-left, idForName(zoe) = %q, want empty", id)
		}
		if len(*got) != 2 {
			t.Errorf("the host saw %d lifecycle frames, want 2", len(*got))
		}
	})

	t.Run("a host that wants none of this is unaffected", func(t *testing.T) {
		// Nil callback is the mesh / static-tree / non-electing case, which is most
		// peers most of the time.
		r := NewRouter(discardLog(), nil, RouterConfig{SelfName: "self", Managed: true})
		r.handle(ctx, signaling.Message{Type: signaling.TypeHeartbeat, From: "p8"})
		r.handle(ctx, signaling.Message{Type: signaling.TypePeerJoined, From: "p9", Name: "zoe"})
		if id := r.idForName("zoe"); id != "p9" {
			t.Errorf("idForName(zoe) = %q, want p9", id)
		}
	})
}
