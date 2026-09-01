package media

import (
	"sort"

	"github.com/SammyUrfen/conclave/internal/overlay"
)

// liveState is what the Router actually has in place right now, expressed in
// topology NAMES. It is passed to diffTopology as a value so the whole diff is a
// pure function of (self, wanted tree, current reality) — no Router, no pion, no
// locks — and therefore table-testable, which is the only way the ugly cases below
// get pinned at all.
type liveState struct {
	// roles maps each neighbour we hold a live session to onto that session's
	// BAKED-IN offerer role. Presence is "a session exists"; the value is the role
	// it was constructed with, which is what makes role inversion detectable.
	roles map[string]bool
	// legs are the (source → child) forwarding relationships the forwarder holds.
	legs []leg
	// parent is the neighbour we are currently attached to as our upstream, or ""
	// if we have none (we are the root, or not yet attached).
	parent string
}

// topoDiff is the work one topology push implies. Everything is sorted so an apply
// is deterministic and a test can compare it whole.
type topoDiff struct {
	// add are neighbours with no live session. The re-parent target is deliberately
	// NOT here: it is opened by the re-parent state machine, which must keep the old
	// parent alive until the new one carries media.
	add []string
	// remove are live sessions the new tree drops. The OLD parent of a re-parent is
	// deliberately not here, for the same reason.
	remove []string
	// invert are SURVIVING neighbours whose offerer role for the edge no longer
	// matches the live session's baked-in role, i.e.
	//     next.Offers(self, peer) != session.Offerer()
	//
	// This bucket exists because Topology.Offers is evaluated ONCE, in NewSession.
	// A diff keeps a surviving edge — that is the point of a diff — but the tree
	// around it can change such that the role inverts: a leaf attached to a relay is
	// the answerer, and a rebuild that gives it children of its own makes both ends
	// relays, so the tie-break may now name it. Its session is still baked as an
	// answerer, onNegotiationNeeded returns early for a non-offerer, and an answerer
	// cannot add m-lines. The failure is not "both offer" but "NEITHER CAN" — no
	// glare, no error, no log line, just a relay that never publishes its forwarded
	// tracks. The only cure pion allows is to re-create the session.
	invert []string
	// addLegs and removeLegs are forwarding legs to mutate on sessions that SURVIVE
	// this apply untouched. Legs on added or inverted neighbours are built by that
	// session's own setup, and legs on removed neighbours die with it, so listing
	// them here would double up.
	addLegs    []leg
	removeLegs []leg
	// reparent reports that our own upstream moved; oldParent must stay alive until
	// newParent carries media (§7.3).
	reparent  bool
	oldParent string
	newParent string
}

// empty reports whether this diff asks for nothing at all — the common case, since
// most pushes re-state a tree that already holds.
func (d topoDiff) empty() bool {
	return len(d.add) == 0 && len(d.remove) == 0 && len(d.invert) == 0 &&
		len(d.addLegs) == 0 && len(d.removeLegs) == 0 && !d.reparent
}

// diffTopology computes what must change for self to realise next, given what is
// live today. It is total: a tree that does not mention self at all yields "close
// everything", never a half-applied state.
func diffTopology(self string, next *overlay.Topology, live liveState) topoDiff {
	var d topoDiff

	wanted := map[string]bool{}
	for _, n := range next.NeighborsOf(self) {
		wanted[n] = true
	}
	newParent := next.ParentOf(self)

	// A re-parent is only a re-parent when we HAVE a parent and it changed. Gaining
	// a first parent is an ordinary add; losing one entirely (a tree that drops us)
	// is an ordinary remove.
	if live.parent != "" && newParent != "" && live.parent != newParent {
		d.reparent = true
		d.oldParent = live.parent
		d.newParent = newParent
	}

	for name := range wanted {
		if _, live := live.roles[name]; live {
			continue
		}
		if d.reparent && name == d.newParent {
			continue // the state machine opens this one, not the plain add path
		}
		d.add = append(d.add, name)
	}
	for name, bakedRole := range live.roles {
		switch {
		case !wanted[name]:
			if d.reparent && name == d.oldParent {
				continue // held open until the new parent carries media
			}
			d.remove = append(d.remove, name)
		case next.Offers(self, name) != bakedRole:
			d.invert = append(d.invert, name)
		}
	}
	sort.Strings(d.add)
	sort.Strings(d.remove)
	sort.Strings(d.invert)

	// A session survives this apply untouched only if it is live, wanted, and in
	// none of the three churn buckets. Those are the only ones whose legs we may
	// mutate in place.
	survives := func(name string) bool {
		if _, ok := live.roles[name]; !ok || !wanted[name] {
			return false
		}
		if d.reparent && (name == d.oldParent || name == d.newParent) {
			return false
		}
		return !contains(d.invert, name)
	}

	// The relay forwards every neighbour's media to every OTHER neighbour, so the
	// wanted leg set is exactly the ordered pairs of distinct neighbours. A leaf has
	// at most one neighbour and therefore no legs, which falls out for free.
	want := map[leg]bool{}
	for src := range wanted {
		for child := range wanted {
			if src != child {
				want[leg{src: src, child: child}] = true
			}
		}
	}
	have := map[leg]bool{}
	for _, l := range live.legs {
		have[l] = true
	}
	for l := range want {
		if !have[l] && survives(l.child) {
			d.addLegs = append(d.addLegs, l)
		}
	}
	for _, l := range live.legs {
		if !want[l] && survives(l.child) {
			d.removeLegs = append(d.removeLegs, l)
		}
	}
	sortLegs(d.addLegs)
	sortLegs(d.removeLegs)
	return d
}

func sortLegs(ls []leg) {
	sort.Slice(ls, func(i, j int) bool {
		if ls[i].src != ls[j].src {
			return ls[i].src < ls[j].src
		}
		return ls[i].child < ls[j].child
	})
}

func contains(ss []string, s string) bool {
	for _, v := range ss {
		if v == s {
			return true
		}
	}
	return false
}
