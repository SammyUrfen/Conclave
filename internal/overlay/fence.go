package overlay

import "fmt"

// Fence is a peer's view of control-plane AUTHORITY: which epoch the arbiter last told
// it is current, who the arbiter named coordinator for that epoch, and the highest
// revision it has applied.
//
// ***Fence answers AUTHORIZATION; Topology.Supersedes answers ORDERING.*** They are
// different questions and answering the second in place of the first is a
// privilege-escalation bug, not a shortcut: Supersedes only asks "is this tree newer",
// so a peer that stamped Epoch = MaxUint64-1 on a tree it computed itself would
// supersede everything, and if Supersedes gated application it would be universally
// obeyed. Accept is the ONLY predicate that may gate applying a pushed topology, and it
// is used in exactly one place — the peer's apply path.
//
// It lives in overlay because it is pure logic over strings and integers, so every
// fencing rule is a table-driven test with no sockets and no media, and because both the
// peer side and the coordinator (which must reason about what peers will accept) need it.
//
// The zero Fence is the UNAUTHORISED state: Epoch 0, no coordinator, Rev 0, and Accept
// returns false for everything until an announcement is adopted. That is deliberate — a
// peer that has not been told who is in charge obeys nobody.
type Fence struct {
	// Epoch is the arbiter-announced coordinator term this peer currently believes in.
	// 0 means "no announcement adopted yet".
	Epoch uint64
	// CoordinatorID is the peer id the arbiter named coordinator for Epoch. Compared
	// against a frame's server-stamped From, which is why the comparison is trustworthy.
	CoordinatorID string
	// Rev is the highest topology revision successfully APPLIED within Epoch.
	Rev uint64
}

// AdoptAnnouncement applies an arbiter announcement. It is the ONLY method that may
// raise Epoch, and that exclusivity is the entire mechanism of the fencing safety
// argument: authority flows from the single writer that mints it, never from the node
// claiming the job.
//
// Returns true if adopted (epoch > f.Epoch), in which case Rev resets to 0 — a new term
// starts its revision count over — and CoordinatorID is replaced. Returns false for a
// stale or duplicate announcement, leaving f untouched, so a replayed frame cannot
// reset a peer's revision watermark and make it re-apply trees it has already applied.
func (f *Fence) AdoptAnnouncement(epoch uint64, coordinatorID string) bool {
	if epoch <= f.Epoch {
		return false
	}
	f.Epoch = epoch
	f.CoordinatorID = coordinatorID
	f.Rev = 0
	return true
}

// Reset returns f to the unauthorised zero state. Called when the peer (re)joins and
// only there: a reconnecting peer must adopt whatever the arbiter announces next rather
// than trusting what it remembers, which is what closes the arbiter-restart hole — an
// arbiter that restarted and is minting epochs from 1 again would otherwise be unable to
// command a peer still holding a higher epoch from the previous process.
func (f *Fence) Reset() {
	*f = Fence{}
}

// Accept decides whether a pushed topology may be applied, and if not, why. from is the
// frame's server-stamped sender id. reason is a short, stable string for the
// stale-rejection dashboard event and the debug log; it is never parsed.
//
// Accepts iff ALL of:
//   - f.Epoch != 0            (this peer has been told who is in charge)
//   - from == f.CoordinatorID (the sender is that node)
//   - t.Epoch == f.Epoch      (EXACT match — a HIGHER epoch is REJECTED)
//   - t.Rev > f.Rev           (strictly newer within the term)
//
// The third clause is equality, not ≥, and that asymmetry is what makes the fence mean
// anything: a peer may learn about a new coordinator only from the arbiter. If a
// topology could raise the epoch, any peer could promote itself by stamping a bigger
// number and the fence would authenticate nothing. The cost is one extra round trip on
// handover, which is bounded and acceptable.
//
// Accept never mutates f. Recording an application is Applied's job, separately.
func (f *Fence) Accept(from string, t *Topology) (ok bool, reason string) {
	if t == nil {
		return false, "nil topology"
	}
	if f.Epoch == 0 {
		return false, "unfenced: no coordinator announced yet"
	}
	if from != f.CoordinatorID {
		return false, fmt.Sprintf("sender %q is not the announced coordinator %q", from, f.CoordinatorID)
	}
	if t.Epoch != f.Epoch {
		if t.Epoch > f.Epoch {
			// Explicitly distinguished: this is the privilege-escalation attempt, and
			// an operator seeing it wants to know the difference between "someone is
			// ahead of the arbiter" and "someone is behind it".
			return false, fmt.Sprintf("epoch %d is ahead of the announced epoch %d; only the arbiter may raise it", t.Epoch, f.Epoch)
		}
		return false, fmt.Sprintf("epoch %d is stale; current is %d", t.Epoch, f.Epoch)
	}
	if t.Rev <= f.Rev {
		return false, fmt.Sprintf("rev %d does not advance the applied rev %d", t.Rev, f.Rev)
	}
	return true, ""
}

// Applied records a successful application, advancing Rev.
//
// Separate from Accept so a caller can Accept, attempt the fallible media work, and
// advance only on success — which means a failed apply is retried by the next push
// rather than being silently skipped because the watermark moved without the tree ever
// being realized.
func (f *Fence) Applied(t *Topology) {
	if t == nil || t.Epoch != f.Epoch {
		return
	}
	if t.Rev > f.Rev {
		f.Rev = t.Rev
	}
}
