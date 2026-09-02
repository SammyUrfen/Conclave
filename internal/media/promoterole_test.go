package media

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/SammyUrfen/conclave/internal/overlay"
	"github.com/SammyUrfen/conclave/internal/signaling"
)

// syncBuf is a log sink safe to read while pion's dispatch goroutines are still
// writing through the handler. slog serialises writes; it does not serialise a
// reader, and -race is in the gate.
type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func captureLog() (*slog.Logger, *syncBuf) {
	buf := &syncBuf{}
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})), buf
}

// linkDerivedPeer registers a live session toward peerName in the role the CURRENT
// TOPOLOGY derives for that edge — the state startPeer would have left behind, over
// a transport whose far end never answers.
//
// It goes through NewSession rather than startPeer because these Routers hold no
// signaling client: an offerer session built by startPeer sends its first offer down
// a nil *signaling.Client from a pion goroutine and panics. The subject here is
// startPeerOpt's "a session already exists" guard, and what that guard reads is the
// peerLink, so a session registered this way exercises it exactly.
func linkDerivedPeer(t *testing.T, r *Router, id, peerName string) *Session {
	t.Helper()
	tr, _ := newGatedPair(id, "self")
	offerer := r.topo.Offers(r.selfName, peerName)
	sess, err := NewSession(SessionConfig{
		Log: discardLog(), SelfID: "self", PeerID: id, Transport: tr, Offerer: &offerer,
	})
	if err != nil {
		t.Fatalf("new session for %s: %v", peerName, err)
	}
	t.Cleanup(func() { _ = sess.Close() })
	r.learnPeer(id, peerName)
	r.mu.Lock()
	r.peers[id] = &peerLink{session: sess, cancel: func() {}}
	r.mu.Unlock()
	return sess
}

// warnedRoleDiscard reports whether the log carries a WARN line naming this peer
// and the role the caller wanted — the two facts §7.1's "silent, looks identical to
// its opposite" rule demands of a discarded override.
func warnedRoleDiscard(out, peerField, roleField string) bool {
	for line := range strings.SplitSeq(out, "\n") {
		if strings.Contains(line, "level=WARN") &&
			strings.Contains(line, peerField) && strings.Contains(line, roleField) {
			return true
		}
	}
	return false
}

// TestRoleOverrideIsNeverSilentlyDiscarded is the backup edge's offerer assignment
// asked as a question about EVIDENCE rather than about the role.
//
// startPeerOpt is idempotent per peer: it returns early when a session already
// exists, BEFORE it reads opts.offerer. Both callers that pass a role override are
// therefore best-effort, and the parent's handler logs "accepting a backup
// promotion; offering the forwarded tracks" before it finds out. A log line
// asserting the opposite of what the code did is worse than no line at all: the
// operator reading it sees a promotion that succeeded, while the child sits in
// ReparentConnectTimeout waiting for an offer that was never going to come.
//
// Reachability. assignBackups never names a neighbour as a backup and Validate
// enforces it — but Router.applyTopology unmarshals and fences a pushed tree and
// never calls Validate (§8.6), so a coordinator bug or a hand-authored -topology
// file puts exactly these trees into a peer. Both subtests therefore use a tree
// Validate would reject, on purpose, and say so.
//
// The invariant asserted is not "the role was applied" — that would pin behaviour
// this change is explicitly not allowed to alter — but the conjunction: EITHER the
// wanted role is in force, OR the log says at WARN that it is not. Both subtests
// fail against the pre-fix code on the second half of it.
func TestRoleOverrideIsNeverSilentlyDiscarded(t *testing.T) {
	ctx := context.Background()

	t.Run("a promote for a child we already hold a session to", func(t *testing.T) {
		log, out := captureLog()
		r := NewRouter(log, nil, RouterConfig{SelfName: "a", Managed: true})
		t.Cleanup(r.closeAll)

		// a is c's PARENT and, illegally, also c's backup. c has a child of its own
		// so that both ends are relays and the "a" < "c" tiebreak makes a the
		// ANSWERER on the in-tree edge — which is the half that makes the discard
		// observable, because the promote wants a to OFFER.
		r.topo = &overlay.Topology{
			Epoch: 1, Rev: 1, Root: "a",
			Edges:   []overlay.Edge{{Parent: "a", Child: "c"}, {Parent: "c", Child: "d"}},
			Backups: []overlay.Backup{{Node: "c", Parent: "a"}},
		}
		if sess := linkDerivedPeer(t, r, "p-c", "c"); sess.Offerer() {
			t.Fatal("fixture: a must hold an ANSWERER session to c before the promote arrives — " +
				"otherwise the promote asks for the role the edge already has and the test " +
				"discriminates nothing")
		}

		r.handle(ctx, signaling.Message{Type: signaling.TypeBackupPromote, From: "p-c"})

		applied := r.sessionByName("c").Offerer()
		claimed := strings.Contains(out.String(), "accepting a backup promotion")
		// WRONG BEHAVIOUR: the handler announces that it is offering the forwarded
		// tracks and then offers nothing, because startPeerOpt returned at the
		// already-exists guard. The child's ReparentConnectTimeout is the only thing
		// that ever notices, and the parent's log says the promotion worked.
		if claimed && !applied {
			t.Error("the backup-promote handler logged success while the session is still the " +
				"ANSWERER: it cannot add a single forwarded m-line, and the log asserts the opposite")
		}
		// WRONG BEHAVIOUR: refusing quietly is no better than claiming falsely. The
		// discard must name the peer and the role it could not apply, or the next
		// person debugging a stuck failover has nothing to read.
		if !applied && !warnedRoleDiscard(out.String(), "peer_name=c", "wanted_offerer=true") {
			t.Errorf("the offerer override was discarded with no WARN naming the peer and the "+
				"wanted role; log was:\n%s", out.String())
		}
	})

	t.Run("a promotion onto a peer we already hold a session to", func(t *testing.T) {
		log, out := captureLog()
		r := NewRouter(log, nil, RouterConfig{SelfName: "c", Managed: true})
		t.Cleanup(r.closeAll)

		// The mirror image, on the promoter's side (startReparent's viaBackup arm):
		// c's backup is illegally its own CHILD d, so the session c is told to open
		// as the answerer is one it already holds — as the offerer, since a relay
		// offers toward a leaf.
		r.topo = &overlay.Topology{
			Epoch: 1, Rev: 1, Root: "a",
			Edges: []overlay.Edge{
				{Parent: "a", Child: "b"}, {Parent: "b", Child: "c"}, {Parent: "c", Child: "d"},
			},
			Backups: []overlay.Backup{{Node: "c", Parent: "d"}},
		}
		if sess := linkDerivedPeer(t, r, "p-d", "d"); !sess.Offerer() {
			t.Fatal("fixture: c must already hold an OFFERER session to d — the discard is only " +
				"observable when the role in force differs from the one the promotion wants")
		}
		r.setParent("b", "")

		r.onParentLost(ctx, "b")

		if r.rp == nil || r.rp.phase != rpOpening {
			t.Fatalf("no re-parent left in flight: rp=%+v — the promotion must stay in the "+
				"ReparentConnectTimeout ladder rather than acquiring a new failure path", r.rp)
		}
		applied := !r.sessionByName("d").Offerer()
		// WRONG BEHAVIOUR: the promoter announces "re-parenting via_backup=true",
		// keeps the offerer role the tree gave the edge, and asks the far end to
		// offer as well. Nothing in the log distinguishes that from a promotion whose
		// answerer role took, and the two fail in completely different ways.
		if !applied && !warnedRoleDiscard(out.String(), "new_parent=d", "wanted_offerer=false") {
			t.Errorf("the answerer override was discarded with no WARN naming the peer and the "+
				"wanted role; log was:\n%s", out.String())
		}
	})
}
