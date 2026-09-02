package main

import (
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/pion/turn/v5"

	"github.com/SammyUrfen/conclave/internal/logging"
)

// wantOptions is the frozen default set for cmd/turn, spelled out in full so that
// adding a flag without reviewing its default fails this test — the same guard
// cmd/peer's wantOptions provides. It caught -denied-peers, which is exactly the kind
// of default (deny by omission) that must not arrive unreviewed.
func wantOptions(t *testing.T) options {
	t.Helper()
	denied, err := parseDeniedPeers(defaultDeniedPeers)
	if err != nil {
		t.Fatalf("defaultDeniedPeers does not parse: %v", err)
	}
	return options{
		addr:        ":3478",
		realm:       defaultRealm,
		users:       map[string]string{},
		deniedPeers: denied,
		level:       slog.LevelInfo,
		format:      logging.FormatText,
	}
}

func TestParseArgsDefaults(t *testing.T) {
	got, err := parseArgs([]string{"-public-ip", "127.0.0.1", "-users", "conclave=secret"})
	if err != nil {
		t.Fatalf("parseArgs: %v", err)
	}
	want := wantOptions(t)
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

		{name: "malformed denied peers", args: []string{"-public-ip", "10.0.0.1", "-users", "a=b", "-denied-peers", "10.0.0.0"}, wantErr: true},
		{name: "empty denied peers is allowed", args: []string{"-public-ip", "10.0.0.1", "-users", "a=b", "-denied-peers", ""}},

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

// --- the peer-address restriction ------------------------------------------
//
// deploy/docker-compose.yml denies coturn 10/8, 192.168/16, 172.16/12, 127/8 and
// multicast, with the comment "a TURN server that will relay into 127.0.0.0/8 or a LAN
// is a pivot into whatever network it sits in". cmd/turn had no equivalent and would
// relay anywhere. These tests pin the predicate, and pin the ONE tension that makes the
// default non-obvious: the live verification runs entirely inside 10.99.0.0/24.

// TestPeerAllowedUnderTheDefaults pins that the shipped default actually denies the
// ranges compose denies. The mutation this catches is the state this branch was in:
// no PermissionHandler at all, so pion installs DefaultPermissionHandler and every
// peer address on earth is relayable.
func TestPeerAllowedUnderTheDefaults(t *testing.T) {
	denied, err := parseDeniedPeers(defaultDeniedPeers)
	if err != nil {
		t.Fatalf("the shipped default does not parse: %v", err)
	}
	tests := []struct {
		ip   string
		want bool
	}{
		// The compose deny set, address by address.
		{ip: "10.0.0.5"},
		{ip: "10.99.0.1"}, // the verification's own range — see the test below
		{ip: "192.168.1.1"},
		{ip: "172.16.0.1"},
		{ip: "172.31.255.254"},
		{ip: "127.0.0.1"},
		{ip: "169.254.1.1"},
		{ip: "0.0.0.1"},
		{ip: "239.255.255.250"}, // multicast, coturn's --no-multicast-peers

		// Public addresses, which is the whole point of a relay.
		{ip: "8.8.8.8", want: true},
		{ip: "1.2.3.4", want: true},
		// Just outside 172.16/12 — the mask is the easiest one in the set to get
		// wrong, and writing it as /16 would quietly allow 172.17-172.31.
		{ip: "172.32.0.1", want: true},
		{ip: "172.15.255.255", want: true},
	}
	for _, tt := range tests {
		t.Run(tt.ip, func(t *testing.T) {
			if got := peerAllowed(denied, net.ParseIP(tt.ip)); got != tt.want {
				t.Errorf("peerAllowed(%s) = %v, want %v", tt.ip, got, tt.want)
			}
		})
	}
}

// TestVerifyTurnDeniedPeersAdmitsTheNamespace is the test that stops a defensible
// default from silently voiding the branch's reason to exist. docs/verify-turn.md runs
// every process inside one netns on 10.99.0.0/24, which the DEFAULT deny set covers —
// so the recipe passes an explicit -denied-peers, and the string below is copied from
// it. If either the default or the recipe moves without the other, this fails here
// rather than as an unexplained allocation refusal twenty minutes into a manual run.
//
// The mutation this catches: relaxing the default instead of the recipe (which would
// make this pass while TestPeerAllowedUnderTheDefaults fails), or tightening the
// recipe's list back to the default.
func TestVerifyTurnDeniedPeersAdmitsTheNamespace(t *testing.T) {
	denied, err := parseDeniedPeers(verifyTurnDeniedPeers)
	if err != nil {
		t.Fatalf("the value docs/verify-turn.md passes does not parse: %v", err)
	}
	if !peerAllowed(denied, net.ParseIP("10.99.0.1")) {
		t.Errorf("-denied-peers %q refuses 10.99.0.1: docs/verify-turn.md cannot relay at all",
			verifyTurnDeniedPeers)
	}
	// It is a RELAXATION of one range, not a disabling of the filter: everything the
	// default denies outside 10/8 must still be denied, or the recipe would be
	// documenting an open relay.
	for _, ip := range []string{"192.168.1.1", "172.16.0.1", "127.0.0.1", "169.254.1.1"} {
		if peerAllowed(denied, net.ParseIP(ip)) {
			t.Errorf("-denied-peers %q admits %s; the recipe should relax 10/8 only",
				verifyTurnDeniedPeers, ip)
		}
	}

	// And the recipe must actually PASS it. The constant above being correct is worth
	// nothing if the command line in the document says something else — that is the
	// drift this whole test exists to prevent, and it is only prevented by reading the
	// document. The mutation this catches: editing the flag value in one place.
	doc, err := os.ReadFile(filepath.Join("..", "..", "docs", "verify-turn.md"))
	if err != nil {
		t.Fatalf("read the verification recipe: %v", err)
	}
	if !strings.Contains(string(doc), "-denied-peers '"+verifyTurnDeniedPeers+"'") {
		t.Errorf("docs/verify-turn.md does not pass -denied-peers %q; its relay will refuse "+
			"every permission in the 10.99.0.0/24 namespace and the run will prove nothing",
			verifyTurnDeniedPeers)
	}
}

// TestParseDeniedPeers pins the parse. The mutations these catch: accepting a bare IP
// (which net.ParseCIDR rejects and a hand-rolled parser would not, silently denying
// nothing), and swallowing a malformed entry — a typo'd CIDR that parses to nothing
// would disable exactly the range the operator meant to protect, with no error.
func TestParseDeniedPeers(t *testing.T) {
	tests := []struct {
		in      string
		wantN   int
		wantErr bool
	}{
		{in: "", wantN: 0},
		{in: "10.0.0.0/8", wantN: 1},
		{in: " 10.0.0.0/8 , 127.0.0.0/8 ", wantN: 2},
		{in: "10.0.0.1", wantErr: true},    // a bare IP is not a CIDR
		{in: "10.0.0.0/33", wantErr: true}, // impossible prefix length
		{in: "not-an-address", wantErr: true},
		{in: "10.0.0.0/8,,127.0.0.0/8", wantErr: true}, // an empty entry is a typo
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, err := parseDeniedPeers(tt.in)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseDeniedPeers(%q) error = %v, wantErr %v", tt.in, err, tt.wantErr)
			}
			if err == nil && len(got) != tt.wantN {
				t.Errorf("parseDeniedPeers(%q) gave %d networks, want %d", tt.in, len(got), tt.wantN)
			}
		})
	}
}

// TestPeerAllowedWithNoDenySet pins the escape hatch, and pins that it is spelled as
// an EMPTY list rather than as a separate boolean. An operator who empties the flag has
// asked for an open relay and gets one; nobody reaches that state by omission.
func TestPeerAllowedWithNoDenySet(t *testing.T) {
	if !peerAllowed(nil, net.ParseIP("10.0.0.5")) {
		t.Errorf("an empty -denied-peers still denied a peer")
	}
}
