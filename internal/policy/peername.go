package policy

import "regexp"

// PeerNamePattern is the one rule for what a peer may call itself.
//
// It is deliberately IDENTICAL in shape to MeetIDPattern, because the two travel to
// the same places and are dangerous in the same ways: a name is interpolated into a
// `?name=` query, fanned out to every member of the meet on join and leave, used as
// a map key and as the relay tree's node identity, re-serialised into every
// dashboard snapshot, and interpolated UNESCAPED into the dashboard's copy-pasteable
// `peer -name …` command. Two similar-but-different rules would be two things to get
// right; one rule is one.
//
// The charset is what survives all of those unescaped: no shell metacharacters, no
// quotes, no whitespace, no path separators, nothing that needs URL-encoding. The
// leading character may not be '-' or '_' so a name can never read as a command-line
// flag when pasted. Lowercase only, because names are compared for uniqueness within
// a meet and case-varying duplicates would be a roster ambiguity dressed as two
// distinct peers.
//
// The {0,63} bound (64 characters) is inherited from MeetIDPattern for the same
// reason it was chosen there, and it is the part that matters most here: a name is
// amplified N-subscribers-wide on every membership change and every snapshot, so
// leaving it bounded only by MaxHeaderBytes turns one request into a broadcast.
const PeerNamePattern = `^[a-z0-9][a-z0-9_-]{0,63}$`

// peerNameRe is the compiled PeerNamePattern. See meetIDRe for why a package-level
// compiled regexp is not the mutable global state this project forbids.
var peerNameRe = regexp.MustCompile(PeerNamePattern)

// ValidPeerName reports whether name is an acceptable peer label.
//
// An EMPTY name is valid and means "unnamed": mesh peers (Phases 1-2) declare no
// name at all, many may share the absence, and the hub's uniqueness rule exempts it.
// Every other value must match PeerNamePattern, and a surface that admits one must
// REJECT rather than truncate or sanitise — a silently shortened name would collide
// with another peer's, and name collisions are precisely what the hub refuses joins
// to prevent.
func ValidPeerName(name string) bool {
	return name == "" || peerNameRe.MatchString(name)
}
