package policy

import "regexp"

// MeetIDPattern is the one rule for what a meet id may look like. A meet id is
// interpolated verbatim into a `?room=` query value AND into an HTTP path segment
// (the dashboard's REST surface), so it is restricted to characters that are
// unambiguous in both, at the boundary, rather than escaped at every use site.
//
// The {0,63} bound (64 chars total) is not arbitrary: it is the longest single DNS
// label, which keeps a meet id usable verbatim as a subdomain if this ever grows
// per-meet hosts, and it is far above any human-typed room name while being far
// below anything that could bloat a log line or a URL. A bound was needed; this is
// the one with a reason attached.
const MeetIDPattern = `^[a-z0-9][a-z0-9_-]{0,63}$`

// meetIDRe is the compiled MeetIDPattern. A package-level var holding an
// immutable compiled regexp is not the mutable global state this project
// forbids; it is the standard Go idiom for "compile once, at init, and fail
// loudly if the literal is wrong".
var meetIDRe = regexp.MustCompile(MeetIDPattern)

// ValidMeetID reports whether id is an acceptable meet id (MeetIDPattern). Every
// externally-facing surface that admits a meet id — /ws?room=, POST /api/meets —
// must reject on this, not merely normalise: a silently rewritten id would put a
// caller in a different meet than the one it asked for, which is far harder to
// debug than a refusal.
func ValidMeetID(id string) bool { return meetIDRe.MatchString(id) }
