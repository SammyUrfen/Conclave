// Package meetconfig converts the tuning that rides a coordinator announcement into
// the configuration a coordinator actually runs under.
//
// THE TUNING IS A PROPERTY OF THE MEET, NOT OF WHICHEVER NODE HOLDS THE ROLE. That is
// the whole reason arbiter.CoordinatorConfig travels on the announcement in the first
// place: if one holder coordinates at MaxDepth 2 and its successor at MaxDepth 3, the
// subnet changes shape for a reason nobody chose — the tree reconfigures because the
// COORDINATOR moved, which is precisely what the stability work exists to prevent.
//
// So there must be exactly ONE conversion from that wire type to coordinator.Config,
// and both nodes that perform it must perform the same one. cmd/server translates it
// for the coordinator it hosts in-process; cmd/peer translates the announcement it
// receives when it is elected. Two copies of this function would drift, and the drift's
// symptom — a meet that re-shapes on handover — looks like a bug in the tree builder
// rather than in a conversion nobody thought to compare.
//
// WHY A PACKAGE AND NOT A METHOD SOMEWHERE. The function names types owned by two
// different packages, and the dependency graph (docs/PLAN.md §2.3) forbids the obvious
// homes: arbiter may not import coordinator, and coordinator may not import arbiter —
// the same constraint that sent RebuildWindow to metrics. cmd/server had it first, but
// Go cannot import a main package, so cmd/peer could not reach it. internal/dashboard
// is the only existing package that legally sees both types, and a config translator is
// not observability surface; putting it there is the junk-drawer move this contract has
// already refused once. Hence one small package on dashboard's tier, holding one
// function, imported by both binaries.
//
// Leaf-ward otherwise: it imports arbiter and coordinator and nothing else, and nothing
// under internal/ imports it.
package meetconfig

import (
	"github.com/SammyUrfen/conclave/internal/arbiter"
	"github.com/SammyUrfen/conclave/internal/coordinator"
)

// Coordinator translates a meet's announced tuning into a coordinator.Config.
//
// It is a pure function of the announcement, which is what lets the arbiter's
// in-process coordinator and an elected peer's reach identical configurations from
// identical frames.
//
// TWO FIELDS ARE DELIBERATELY LEFT ZERO, and a caller must fill them in itself:
//
//   - SelfName is the HOLDER's identity and differs per holder by definition, which is
//     why the wire type has no field for it. The arbiter leaves it empty (that empty
//     value is what marks a coordinator running inside the arbiter process); an elected
//     peer sets its own name.
//   - Clock is a per-process capability rather than a value, so it cannot ride an
//     announcement. Nil makes coordinator.New use the system clock, while a test can
//     still inject a virtual one.
//
// Everything else on coordinator.Config that shapes a meet is carried.
func Coordinator(cc arbiter.CoordinatorConfig) coordinator.Config {
	return coordinator.Config{
		MaxDepth:          cc.MaxDepth,
		StreamKbps:        cc.StreamKbps,
		DefaultUploadKbps: cc.DefaultUploadKbps,
		// The pointer is a LOCAL API convenience and never crosses the wire: nil is
		// coordinator.Config's zero value and resolves to the safe stability-preserving
		// default, while Stickiness(0) is an unambiguous request for memoryless. The
		// announced value is always known, so it is always an explicit request —
		// including when it is 0, which is the case the pointer exists for. Passing it
		// through a "zero means unset" convention here would turn a memoryless meet
		// into its opposite, silently.
		StickinessMs: coordinator.Stickiness(cc.StickinessMs),
		// The accessors, not raw assignment: these arrive as milliseconds and
		// time.Duration counts nanoseconds, so assigning the int64 would configure a
		// coordinator a million times too fast — a mistake that leaves every value a
		// valid duration and every tree still building, so nothing downstream catches
		// it.
		Dwell:             cc.Dwell(),
		RecomputeCooldown: cc.RecomputeCooldown(),
		// These two stay whatever the meet said, INCLUDING zero: 0 means "derive the
		// threshold per node from the cadence each peer declared", which is the shipped
		// design and the only thing keeping a slow-beating peer from being reaped.
		DegradedAfter: cc.DegradedAfter(),
		GoneAfter:     cc.GoneAfter(),
		JoinSettle:    cc.JoinSettle(),
		// The field a peer could not possibly supply for itself: it describes the
		// SERVER's transport. Without it a peer coordinator leaves the
		// permanent-ejection window open.
		SocketDetection: cc.SocketDetection(),
	}
}
