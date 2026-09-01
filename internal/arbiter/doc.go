// Package arbiter is the central server's authority over WHO COORDINATES a meet.
//
// It is the single writer of the epoch — the fencing token every peer checks before
// it accepts a topology — and it is the only author of the announcement that moves
// that token. Nothing else in the system may advance a peer's epoch, which is what
// makes "two coordinators can never hold the same term" a structural property
// rather than a timing argument (docs/PLAN.md 6.4).
//
// It is NEVER a media relay, and it deliberately scores peers on CONTROL-plane
// evidence only: upload bandwidth is a relay resource and mixing it in here would
// collapse the control and data roles into the "supernode" the architecture forbids.
//
// The package holds no sockets. Its inputs arrive as the same four Observer
// callbacks the signaling Hub already produces, and its outputs go through the
// Announcer and Publisher seams declared here and adapted in cmd/server — so this
// package imports neither signaling nor coordinator, and the whole election is
// unit-testable with no network and no wall clock.
package arbiter
