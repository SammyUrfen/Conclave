package coordinator

import (
	"context"
	"sort"
	"time"

	"github.com/SammyUrfen/conclave/internal/metrics"
	"github.com/SammyUrfen/conclave/internal/overlay"
)

// RoomSnapshot is a consistent, deep-copied view of one meet's control state, for
// the dashboard's initial frame and for a client recovering from a sequence gap.
type RoomSnapshot struct {
	RoomID string
	Epoch  uint64
	Rev    uint64
	// StaleRejected is the meet-wide total across members: the visible proof that the
	// fence is doing work. It is in the SNAPSHOT, not only in the event stream, so a
	// dashboard connecting mid-meeting shows the right number without replaying every
	// event that ever produced it.
	StaleRejected uint64
	// Topo is the last PUBLISHED tree — what the fleet was actually told to run,
	// never the coordinator's working copy. It is a deep copy: a consumer may not
	// corrupt coordinator state by writing through it.
	Topo    *overlay.Topology
	Members []MemberSnapshot
	At      time.Time
}

// MemberSnapshot is one member's control state. Every slice in it is ordered, and
// the Members slice itself is ordered by Name then ID — a map, or an unordered
// slice, would make the dashboard's replay of the same history differ run to run.
type MemberSnapshot struct {
	ID       string
	Name     string
	Health   Health
	Report   metrics.Report
	Reported bool
	Parent   string   // realized, from the last heartbeat
	Children []string // realized, from the last heartbeat, ascending
	Backup   string   // assigned by the current tree
	// StaleRejected is this member's latest reported refusal count — cumulative since
	// the peer last JOINED, and reset by the peer when its fence resets.
	StaleRejected uint64
	LastBeatSeq   uint64
	LastBeatAt    time.Time
}

// Snapshot returns a consistent view of roomID. It round-trips through the Run
// goroutine (like Sync) so the snapshot can never tear against a concurrent
// recompute, and it deep copies everything it returns so the caller cannot mutate
// coordinator state. An unknown meet is a zero RoomSnapshot and no error — "there is
// no such meet" is an answer, not a failure.
//
// Unlike Sync it does NOT round-trip the outbound queue, and that difference is
// deliberate: a snapshot must still be answerable while a wedged Sender has the push
// plane backed up, because that is exactly when an operator is looking at it.
func (c *Coordinator) Snapshot(ctx context.Context, roomID string) (RoomSnapshot, error) {
	ch := make(chan RoomSnapshot, 1)
	select {
	case c.events <- event{kind: evSnapshot, roomID: roomID, snap: ch}:
	case <-ctx.Done():
		return RoomSnapshot{}, ctx.Err()
	case <-c.done:
		return RoomSnapshot{}, errStopped
	}
	select {
	case snap := <-ch:
		return snap, nil
	case <-ctx.Done():
		return RoomSnapshot{}, ctx.Err()
	case <-c.done:
		return RoomSnapshot{}, errStopped
	}
}

// snapshotOf runs on the Run goroutine and is the only place coordinator state is
// copied out.
func (c *Coordinator) snapshotOf(roomID string) RoomSnapshot {
	rs := c.rooms[roomID]
	if rs == nil {
		return RoomSnapshot{}
	}
	snap := RoomSnapshot{
		RoomID:  rs.id,
		Epoch:   rs.epoch,
		Rev:     rs.rev,
		Topo:    copyTopology(rs.published),
		At:      c.cfg.Clock.Now(),
		Members: make([]MemberSnapshot, 0, len(rs.nodes)),
	}
	for _, id := range c.sortedPeerIDs(rs) {
		ns := rs.nodes[id]
		m := MemberSnapshot{
			ID:            ns.id,
			Name:          ns.name,
			Health:        ns.health,
			Report:        ns.report,
			Reported:      ns.reported,
			Parent:        ns.realParent,
			Children:      append([]string(nil), ns.realChildren...),
			StaleRejected: ns.staleRejected,
			LastBeatSeq:   ns.beatSeq,
			LastBeatAt:    ns.lastBeatAt,
		}
		snap.StaleRejected += ns.staleRejected
		sort.Strings(m.Children)
		if rs.published != nil {
			m.Backup = rs.published.BackupOf(ns.name)
		}
		snap.Members = append(snap.Members, m)
	}
	sort.Slice(snap.Members, func(i, j int) bool {
		if snap.Members[i].Name != snap.Members[j].Name {
			return snap.Members[i].Name < snap.Members[j].Name
		}
		return snap.Members[i].ID < snap.Members[j].ID
	})
	return snap
}

// copyTopology deep copies a tree so a snapshot consumer holds no reference into
// coordinator state. The slices are the whole reason this exists: copying the struct
// alone would share Edges and Backups, and one dashboard handler sorting them in
// place would silently break the topological-order invariant the rebuild depends on.
func copyTopology(t *overlay.Topology) *overlay.Topology {
	if t == nil {
		return nil
	}
	out := &overlay.Topology{Epoch: t.Epoch, Rev: t.Rev, Root: t.Root}
	out.Edges = append([]overlay.Edge(nil), t.Edges...)
	out.Backups = append([]overlay.Backup(nil), t.Backups...)
	return out
}
