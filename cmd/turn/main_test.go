package main

import (
	"log/slog"
	"reflect"
	"testing"

	"github.com/pion/turn/v5"

	"github.com/SammyUrfen/conclave/internal/logging"
)

// wantOptions is the frozen default set for cmd/turn, spelled out in full so that
// adding a flag without reviewing its default fails this test — the same guard
// cmd/peer's wantOptions provides.
func wantOptions() options {
	return options{
		addr:   ":3478",
		realm:  defaultRealm,
		users:  map[string]string{},
		level:  slog.LevelInfo,
		format: logging.FormatText,
	}
}

func TestParseArgsDefaults(t *testing.T) {
	got, err := parseArgs([]string{"-public-ip", "127.0.0.1", "-users", "conclave=secret"})
	if err != nil {
		t.Fatalf("parseArgs: %v", err)
	}
	want := wantOptions()
	want.publicIP = "127.0.0.1"
	want.users = map[string]string{"conclave": "secret"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("parseArgs =\n\t%+v\nwant\n\t%+v", got, want)
	}
}

// TestParseArgsValidation pins that every configuration which would produce a server
// that starts and then relays nothing is rejected at startup instead. A TURN server
// that answers but hands out an unreachable relay address is the single most
// time-consuming failure in this whole area: nothing logs, ICE just never completes.
func TestParseArgsValidation(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr bool
	}{
		{name: "minimal", args: []string{"-public-ip", "10.0.0.1", "-users", "a=b"}},
		{name: "several users", args: []string{"-public-ip", "10.0.0.1", "-users", "a=b,c=d"}},

		// The relay address is what the server ADVERTISES; it cannot be guessed from
		// the listener, which is normally 0.0.0.0.
		{name: "no public ip", args: []string{"-users", "a=b"}, wantErr: true},
		{name: "public ip is not an ip", args: []string{"-public-ip", "turn.example.com", "-users", "a=b"}, wantErr: true},

		// An open relay is not a default anyone should reach by omission.
		{name: "no users", args: []string{"-public-ip", "10.0.0.1"}, wantErr: true},
		{name: "user with no password", args: []string{"-public-ip", "10.0.0.1", "-users", "a"}, wantErr: true},
		{name: "empty username", args: []string{"-public-ip", "10.0.0.1", "-users", "=b"}, wantErr: true},
		{name: "empty password", args: []string{"-public-ip", "10.0.0.1", "-users", "a="}, wantErr: true},

		{name: "unknown log level", args: []string{"-public-ip", "10.0.0.1", "-users", "a=b", "-log-level", "trace"}, wantErr: true},
		{name: "unknown log format", args: []string{"-public-ip", "10.0.0.1", "-users", "a=b", "-log-format", "yaml"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseArgs(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseArgs(%v) error = %v, wantErr %v", tt.args, err, tt.wantErr)
			}
		})
	}
}

// TestParseRelayPorts pins the pinned range. It exists for the same reason
// media.SessionConfig.MediaPortRange does — a firewall rule can only be written
// against ports that are known in advance, and the live verification of this whole
// feature is exactly such a rule — so a range that silently degrades to ephemeral
// would make the verification unfalsifiable rather than merely inconvenient.
//
// The mutations these catch: accepting an inverted range (pion allocates nothing and
// says so only at allocation time), and accepting port 0 (which the kernel reads as
// "pick any", quietly disabling the confinement the flag exists to provide).
func TestParseRelayPorts(t *testing.T) {
	tests := []struct {
		in      string
		want    [2]uint16
		wantErr bool
	}{
		{in: "", want: [2]uint16{}},
		{in: "49160-49200", want: [2]uint16{49160, 49200}},
		{in: "49160", want: [2]uint16{49160, 49160}},
		{in: " 49160 - 49200 ", want: [2]uint16{49160, 49200}},
		{in: "49200-49160", wantErr: true},
		{in: "0-100", wantErr: true},
		{in: "1-65536", wantErr: true},
		{in: "49160-", wantErr: true},
		{in: "abc", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, err := parseRelayPorts(tt.in)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseRelayPorts(%q) error = %v, wantErr %v", tt.in, err, tt.wantErr)
			}
			if err == nil && got != tt.want {
				t.Errorf("parseRelayPorts(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

// TestAuthHandlerKeysAreRealmScoped pins that the realm actually reaches the key
// derivation. STUN long-term credentials hash username:realm:password, so a server
// that derives keys under one realm and advertises another rejects every correct
// password with a bare 401 and no explanation.
//
// The mutations this catches: hardcoding a realm in the handler, or deriving the key
// from the password alone.
func TestAuthHandlerKeysAreRealmScoped(t *testing.T) {
	h := authHandler(map[string]string{"alice": "hunter2"}, "conclave")

	id, key, ok := h(&turn.RequestAttributes{Username: "alice", Realm: "conclave"})
	if !ok || id != "alice" {
		t.Fatalf("authHandler rejected a known user: id=%q ok=%v", id, ok)
	}
	if want := turn.GenerateAuthKey("alice", "conclave", "hunter2"); !reflect.DeepEqual(key, want) {
		t.Errorf("key = %x, want %x (the realm is not part of the derivation)", key, want)
	}
	if same := turn.GenerateAuthKey("alice", "other-realm", "hunter2"); reflect.DeepEqual(key, same) {
		t.Errorf("the key is identical under a different realm, so the realm is ignored")
	}
	if _, _, ok := h(&turn.RequestAttributes{Username: "mallory", Realm: "conclave"}); ok {
		t.Errorf("authHandler admitted an unknown user")
	}
}
