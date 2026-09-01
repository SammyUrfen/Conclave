package policy

import (
	"strings"
	"testing"
)

// TestValidPeerName pins the peer-name boundary rule. The dangerous inputs are the
// unbounded one (a name is fanned out to every member and re-serialised into every
// dashboard snapshot) and the shell-metacharacter one (a name is interpolated
// unescaped into the dashboard's PeerCommand).
func TestValidPeerName(t *testing.T) {
	tests := []struct {
		label string
		name  string
		want  bool
	}{
		{label: "empty is a mesh peer, not an error", name: "", want: true},
		{label: "ordinary", name: "relay", want: true},
		{label: "hyphenated", name: "leaf-b", want: true},
		{label: "underscored", name: "node_1", want: true},
		{label: "digit first", name: "1st", want: true},
		{label: "at the bound", name: strings.Repeat("n", 64), want: true},
		{label: "one over the bound", name: strings.Repeat("n", 65)},
		{label: "amplification payload", name: strings.Repeat("n", 1<<20)},
		{label: "uppercase", name: "Relay"},
		{label: "space", name: "two words"},
		{label: "semicolon", name: "a;rm -rf /"},
		{label: "backtick", name: "a`id`"},
		{label: "dollar", name: "a$(id)"},
		{label: "quote", name: `a"b`},
		{label: "path separator", name: "a/b"},
		{label: "leading hyphen reads as a flag", name: "-rf"},
		{label: "leading underscore", name: "_x"},
		{label: "dot", name: "a.b"},
		{label: "newline", name: "a\nb"},
		{label: "non-ascii", name: "café"},
	}
	for _, tt := range tests {
		t.Run(tt.label, func(t *testing.T) {
			if got := ValidPeerName(tt.name); got != tt.want {
				t.Errorf("ValidPeerName(%q) = %v, want %v", truncate(tt.name), got, tt.want)
			}
		})
	}
}

// TestPeerNameAndMeetIDAgreeOnCharset documents that the two boundary identifiers
// share one shape deliberately. They reach the same places — a URL query, a JSON
// snapshot, a log line, an unescaped shell command — so two similar-but-different
// rules would be two things to get right instead of one.
func TestPeerNameAndMeetIDAgreeOnCharset(t *testing.T) {
	for _, s := range []string{"relay", "leaf-b", "node_1", strings.Repeat("n", 64)} {
		if ValidPeerName(s) != ValidMeetID(s) {
			t.Errorf("%q: ValidPeerName=%v but ValidMeetID=%v", s, ValidPeerName(s), ValidMeetID(s))
		}
	}
	// The one deliberate difference: a meet id is required, a peer name is optional.
	if !ValidPeerName("") {
		t.Error("an absent peer name must be valid; mesh peers send none")
	}
	if ValidMeetID("") {
		t.Error("an empty meet id must be invalid")
	}
}

func truncate(s string) string {
	if len(s) > 32 {
		return s[:32] + "...(" + string(rune('0'+len(s)/1000%10)) + "k)"
	}
	return s
}
