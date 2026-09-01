package policy

import (
	"strings"
	"testing"
)

// TestValidMeetID pins the frozen MeetIDPattern's edges: leading '-', the 64 vs 65
// char boundary, and case. This is the moved half of what shipped as
// signaling.TestRoomIDValidation/ValidRoomID — see hub_test.go / wire_test.go for
// the pin proving signaling still agrees with this package exactly.
func TestValidMeetID(t *testing.T) {
	tests := []struct {
		id   string
		want bool
	}{
		{id: "demo", want: true},
		{id: "default", want: true},
		{id: "a", want: true},
		{id: "9lives", want: true},
		{id: "a_b-c9", want: true},
		{id: strings.Repeat("a", 64), want: true},
		{id: "", want: false},
		{id: strings.Repeat("a", 65), want: false},
		{id: "Demo", want: false},
		{id: "-lead", want: false},
		{id: "_lead", want: false},
		{id: "a b", want: false},
		{id: "a/b", want: false},
		{id: "a.b", want: false},
		{id: "café", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.id, func(t *testing.T) {
			if got := ValidMeetID(tt.id); got != tt.want {
				t.Errorf("ValidMeetID(%q) = %v, want %v", tt.id, got, tt.want)
			}
		})
	}
}
