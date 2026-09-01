package main

import (
	"strings"
	"testing"
)

// TestParseMediaPorts covers the flag's accepted and rejected spellings. Bad config
// must stop the peer at startup, never surface later as a peer that gathers no
// candidates and never connects — the failure mode with no visible cause.
func TestParseMediaPorts(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    [2]uint16
		wantErr string
	}{
		{name: "empty means ephemeral", in: "", want: [2]uint16{}},
		{name: "a range", in: "47000-47019", want: [2]uint16{47000, 47019}},
		{name: "a single port is a range of one", in: "47000", want: [2]uint16{47000, 47000}},
		{name: "spaces tolerated", in: " 47000 - 47019 ", want: [2]uint16{47000, 47019}},

		{name: "reversed", in: "47019-47000", wantErr: "below"},
		{name: "not a number", in: "abc-47019", wantErr: "invalid"},
		{name: "out of range high", in: "47000-70000", wantErr: "invalid"},
		{name: "zero is not a usable port", in: "0-100", wantErr: "invalid"},
		{name: "too many parts", in: "1-2-3", wantErr: "invalid"},
		{name: "empty half", in: "47000-", wantErr: "invalid"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseMediaPorts(tt.in)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("want an error containing %q, got %v", tt.wantErr, got)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("error %q does not mention %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

// TestMediaPortsReachTheRouterConfig pins the WIRING. parseMediaPorts being correct
// proves a string parses; it does not prove the value ever reaches pion. A flag that
// is accepted, validated, logged and then dropped is indistinguishable from one that
// works until someone checks which port media actually used.
func TestMediaPortsReachTheRouterConfig(t *testing.T) {
	opts, err := parseArgs([]string{
		"-call", "-managed", "-name", "relay", "-server", "http://127.0.0.1:1",
		"-room", "r", "-media-ports", "47000-47019",
	})
	if err != nil {
		t.Fatalf("parseArgs: %v", err)
	}
	if got := opts.cfg.mediaPorts; got != [2]uint16{47000, 47019} {
		t.Fatalf("callConfig.mediaPorts = %v, want {47000 47019}", got)
	}
	rc := routerConfigFor(opts.cfg, nil, nil, nil, nil, nil)
	if rc.MediaPortRange != [2]uint16{47000, 47019} {
		t.Errorf("RouterConfig.MediaPortRange = %v; the flag never reaches media", rc.MediaPortRange)
	}
}

// TestMediaPortsDefaultsToEphemeral pins that a peer run without the flag is
// unchanged — the zero value must mean "let pion choose", not "port 0".
func TestMediaPortsDefaultsToEphemeral(t *testing.T) {
	opts, err := parseArgs([]string{
		"-call", "-managed", "-name", "relay", "-server", "http://127.0.0.1:1", "-room", "r",
	})
	if err != nil {
		t.Fatalf("parseArgs: %v", err)
	}
	if got := routerConfigFor(opts.cfg, nil, nil, nil, nil, nil).MediaPortRange; got != [2]uint16{} {
		t.Errorf("MediaPortRange = %v with no flag, want the zero value", got)
	}
}

// TestBadMediaPortsIsFatalAtStartup pins the fail-loud rule: an unusable range must
// be rejected by parseArgs, not carried into a peer that then silently fails to
// gather candidates.
func TestBadMediaPortsIsFatalAtStartup(t *testing.T) {
	_, err := parseArgs([]string{
		"-call", "-managed", "-name", "relay", "-server", "http://127.0.0.1:1",
		"-room", "r", "-media-ports", "99-1",
	})
	if err == nil {
		t.Fatal("a reversed port range was accepted at startup")
	}
}
