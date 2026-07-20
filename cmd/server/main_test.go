package main

import (
	"strings"
	"testing"
)

// TestValidateCoordinatorFlags pins the fail-loud config check: bad coordinator
// constraints must be rejected at startup, but only when the coordinator is on (the
// flags are inert otherwise).
func TestValidateCoordinatorFlags(t *testing.T) {
	tests := []struct {
		name       string
		coordinate bool
		streamKbps int
		maxDepth   int
		wantErr    string // substring; "" means expect success
	}{
		{name: "disabled ignores bad values", coordinate: false, streamKbps: 0, maxDepth: 0},
		{name: "valid enabled", coordinate: true, streamKbps: 2000, maxDepth: 2},
		{name: "zero stream cost", coordinate: true, streamKbps: 0, maxDepth: 2, wantErr: "stream-kbps"},
		{name: "negative stream cost", coordinate: true, streamKbps: -1, maxDepth: 2, wantErr: "stream-kbps"},
		{name: "zero depth", coordinate: true, streamKbps: 2000, maxDepth: 0, wantErr: "max-depth"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateCoordinatorFlags(tt.coordinate, tt.streamKbps, tt.maxDepth)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("got %v, want error containing %q", err, tt.wantErr)
			}
		})
	}
}
