package media

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"

	"github.com/SammyUrfen/conclave/internal/metrics"
	"github.com/SammyUrfen/conclave/internal/overlay"
	"github.com/SammyUrfen/conclave/internal/signaling"
)

// meetFixture is a live room on a real Hub with a set of managed Routers plus one
// extra client standing in for the coordinator, which is the only way to exercise
// applyTopology end to end: a pushed topology is the input the whole diff-and-apply
// path exists to consume.
type meetFixture struct {
	srv     *httptest.Server
	routers map[string]*Router
	coord   *signaling.Client
	wg      *sync.WaitGroup
	cancel  context.CancelFunc
	log     *slog.Logger

	mu  sync.Mutex
	ids map[string]string // peer name → server-assigned id, learned from the roster
}

func newMeetFixture(t *testing.T, ctx context.Context, room string, cfgs map[string]RouterConfig) *meetFixture {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	if os.Getenv("MESH_DEBUG") == "" {
		logger = discardLog()
	}

	hub := signaling.NewHub(logger)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ws", hub.ServeWS)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	runCtx, cancel := context.WithCancel(ctx)
	f := &meetFixture{srv: srv, routers: map[string]*Router{}, wg: &sync.WaitGroup{},
		cancel: cancel, log: logger, ids: map[string]string{}}

	// The coordinator joins first so it observes every peer-joined and can address
	// the whole meet by name.
	coord, err := signaling.Dial(runCtx, logger, srv.URL, room, "coord")
	if err != nil {
		t.Fatalf("coord dial: %v", err)
	}
	t.Cleanup(func() { _ = coord.Close() })
	f.coord = coord

	// The coordinator's own frame stream is where name↔id comes from; it joined
	// first, so every peer arrives as a peer-joined it can see.
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		for {
			select {
			case <-runCtx.Done():
				return
			case msg, ok := <-coord.Incoming():
				if !ok {
					return
				}
				switch msg.Type {
				case signaling.TypeJoined:
					f.mu.Lock()
					for _, p := range msg.Peers {
						f.ids[p.Name] = p.ID
					}
					f.mu.Unlock()
				case signaling.TypePeerJoined:
					f.mu.Lock()
					f.ids[msg.Name] = msg.From
					f.mu.Unlock()
				}
			}
		}
	}()

	names := make([]string, 0, len(cfgs))
	for name := range cfgs {
		names = append(names, name)
	}
	sortStrings(names)
	for _, name := range names {
		client, err := signaling.Dial(runCtx, logger, srv.URL, room, name)
		if err != nil {
			t.Fatalf("%s dial: %v", name, err)
		}
		t.Cleanup(func() { _ = client.Close() })
		cfg := cfgs[name]
		cfg.SelfName = name
		cfg.Managed = true
		cfg.Clock = scaledClock{factor: 4}
		r := NewRouter(logger, client, cfg)
		f.routers[name] = r
		f.wg.Add(1)
		go func() {
			defer f.wg.Done()
			_ = r.Run(runCtx)
		}()
	}
	t.Cleanup(func() {
		cancel()
		done := make(chan struct{})
		go func() { f.wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			t.Error("routers did not shut down cleanly — a goroutine leaked")
		}
	})
	return f
}

// idOf resolves a peer name to the id the server assigned it, by reading the
// coordinator client's own roster stream.
func (f *meetFixture) idOf(t *testing.T, name string) string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		f.mu.Lock()
		id := f.ids[name]
		f.mu.Unlock()
		if id != "" {
			return id
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("never learned an id for %q", name)
	return ""
}

// push sends topo to every named peer, the way the coordinator does.
func (f *meetFixture) push(t *testing.T, topo *overlay.Topology) {
	t.Helper()
	payload, err := json.Marshal(topo)
	if err != nil {
		t.Fatalf("marshal topology: %v", err)
	}
	for name := range f.routers {
		id := f.idOf(t, name)
		if err := f.coord.Send(signaling.Message{
			Type: signaling.TypeTopology, To: id, Payload: payload,
		}); err != nil {
			t.Fatalf("push topology to %s: %v", name, err)
		}
	}
}

// TestRouterAppliesTopologyDiff drives one meet through a re-shape that touches
// every bucket of the §7.4 diff at once:
//
//	a→b→c   becomes   a→b, a→c
//
//	  * c RE-PARENTS from b to a (§7.3) — asynchronously, make-before-break;
//	  * the surviving a↔b edge INVERTS its offerer role (C11), because b stops
//	    being a relay and the tie-break no longer names it;
//	  * b DROPS its session to c.
//
// Discrimination. The re-parent assertion fails against the blocking design in the
// contract's v1: waiting for `connected` inside applyTopology starves the very
// offer/answer frames the Run goroutine must deliver, so c never attaches to a. The
// inversion assertion fails against any diff that compares only neighbour SETS: the
// a↔b session survives such a diff untouched, keeping a baked in as the answerer it
// may no longer be.
func TestRouterAppliesTopologyDiff(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// b and c both send: after the re-shape c must RECEIVE something over its new
	// edge, and a relay never sends media of its own, so a fixture with a single
	// sender would starve the peer whose promotion is under test.
	f := newMeetFixture(t, ctx, "diff", map[string]RouterConfig{
		"a": {},
		"b": {SendMedia: true},
		"c": {SendMedia: true},
	})

	var phaseMu sync.Mutex
	var sawOverlap bool
	f.routers["c"].reparentHook = func(phase string, oldParentOpen bool) {
		phaseMu.Lock()
		if phase == "connected" && oldParentOpen {
			sawOverlap = true
		}
		phaseMu.Unlock()
	}

	topo1 := tree("a", [2]string{"a", "b"}, [2]string{"b", "c"})
	f.push(t, topo1)

	waitFor(t, "the chain a→b→c to converge", 30*time.Second, func() bool {
		return allConnected(f.routers["a"].Stats(), 1) &&
			allConnected(f.routers["b"].Stats(), 2) &&
			allConnected(f.routers["c"].Stats(), 1) &&
			receivedAny(f.routers["a"].Stats())
	})

	// Under the chain both a and b are relays, so the name tie-break gives the
	// offer to b: a's live session is the ANSWERER on that edge.
	if got := f.offererToward(t, "a", "b"); got {
		t.Fatalf("under a→b→c, a should be the answerer toward b (both are relays, b > a)")
	}

	topo2 := tree("a", [2]string{"a", "b"}, [2]string{"a", "c"})
	topo2.Rev = 2
	f.push(t, topo2)

	waitFor(t, "the star a→{b,c} to converge", 30*time.Second, func() bool {
		return allConnected(f.routers["a"].Stats(), 2) &&
			allConnected(f.routers["b"].Stats(), 1) &&
			allConnected(f.routers["c"].Stats(), 1)
	})

	// C11: b is a leaf now, so a must be the offerer on the surviving edge — which
	// is only true if the diff noticed the role inversion and re-created it.
	if got := f.offererToward(t, "a", "b"); !got {
		t.Error("a is still the answerer toward b after b stopped being a relay: " +
			"the inverted edge was not re-created, so a can never publish its forwarded m-lines")
	}
	// And media still reaches the root over the re-parented edge.
	waitFor(t, "a to receive c's media over the new edge", 30*time.Second, func() bool {
		return f.routers["a"].Stats().Received[f.idOf(t, "c")]
	})

	phaseMu.Lock()
	defer phaseMu.Unlock()
	if !sawOverlap {
		t.Error("the new parent connected only after the old session was gone: " +
			"make-before-break did not happen, so the media gap is a full ICE+DTLS round")
	}
}

// TestRouterPromotesBackupParent pins §7.5: when the parent edge fails, the peer
// promotes its precomputed backup ITSELF, without waiting for a coordinator, and
// reports the outcome only once media has actually arrived over the new edge.
//
// Discrimination: the reported outcome is OK only if a remote track landed inside
// ReparentMediaTimeout, so a version that ratified on `connected` alone (which
// proves reachability of the backup, not of the root) would report OK for a
// backup that forwards nothing — and this test's fixture gives the backup real
// media to forward, so the two are distinguishable.
func TestRouterPromotesBackupParent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	reports := make(chan metrics.Reparented, 4)
	f := newMeetFixture(t, ctx, "backup", map[string]RouterConfig{
		"a": {},
		"b": {},
		"c": {SendMedia: true, Backup: true, OnReparented: func(r metrics.Reparented) {
			select {
			case reports <- r:
			default:
			}
		}},
	})

	topo := tree("a", [2]string{"a", "b"}, [2]string{"b", "c"})
	topo.Backups = []overlay.Backup{{Node: "c", Parent: "a"}}
	f.push(t, topo)

	waitFor(t, "the chain a→b→c to converge", 30*time.Second, func() bool {
		return allConnected(f.routers["b"].Stats(), 2) &&
			allConnected(f.routers["c"].Stats(), 1) &&
			receivedAny(f.routers["a"].Stats())
	})

	// Simulate the parent edge dying. Posting the state event is exactly what pion's
	// OnState callback does; driving it directly keeps the test off pion's ~25s ICE
	// failure timer while exercising the identical code path.
	f.routers["c"].postEvent(routerEvent{
		kind: evPeerState, peerName: "b", state: webrtc.PeerConnectionStateFailed,
	})

	select {
	case rep := <-reports:
		if !rep.OK {
			t.Fatalf("backup promotion reported failure: %+v", rep)
		}
		if rep.From != "b" || rep.To != "a" {
			t.Errorf("Reparented{From:%q,To:%q}, want b→a", rep.From, rep.To)
		}
		if rep.Name != "c" {
			t.Errorf("Reparented.Name = %q, want c", rep.Name)
		}
		if rep.Epoch != topo.Epoch || rep.Rev != topo.Rev {
			t.Errorf("Reparented stamped (%d,%d), want the acting topology's (%d,%d)",
				rep.Epoch, rep.Rev, topo.Epoch, topo.Rev)
		}
	case <-time.After(40 * time.Second):
		t.Fatal("no Reparented report: the peer never promoted its backup parent")
	}

	// The backup edge is live and the old one is gone.
	if got := len(f.routers["c"].Stats().Peers); got != 1 {
		t.Errorf("c holds %d sessions after promotion, want 1 (the backup only)", got)
	}
	// §5.6 step 3: the local backup is cleared on promotion, so a SECOND failure
	// reports failure at once instead of re-attempting the parent it already has.
	if got := f.routers["c"].currentTopo().BackupOf("c"); got != "" {
		t.Errorf("local backup is still %q after promoting it; a retry would re-target the current parent", got)
	}
}

// offererToward reports the baked-in offerer role of holder's live session toward
// peer, which is the state C11 is about.
func (f *meetFixture) offererToward(t *testing.T, holder, peer string) bool {
	t.Helper()
	r := f.routers[holder]
	id := f.idOf(t, peer)
	r.mu.Lock()
	defer r.mu.Unlock()
	link := r.peers[id]
	if link == nil || link.session == nil {
		t.Fatalf("%s has no live session toward %s", holder, peer)
	}
	return link.session.Offerer()
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}
