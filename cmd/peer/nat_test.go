package main

import (
	"reflect"
	"testing"

	"github.com/pion/webrtc/v4"

	"github.com/SammyUrfen/conclave/internal/metrics"
	"github.com/SammyUrfen/conclave/internal/overlay"
)

// --- -nat: from a declaration to a measurement, with an override -------------
//
// overlay.NATRelayed is load-bearing in two places — BuildTree/Validate force such a
// node to a leaf, and arbiter.Fitness disqualifies it as coordinator outright. Until
// Phase 7 it was a value an operator TYPED. These tests pin the three properties the
// change has to hold: the sensor is now the default source, the flag still forces a
// value when set, and an unmeasured peer is never demoted on no evidence.

// TestNATFlagDefaultsToMeasuring pins the default. The mutation it catches: leaving
// the flag's default at "direct", which would keep every peer on the declared value
// and make the whole sensor dead code that nothing calls.
func TestNATFlagDefaultsToMeasuring(t *testing.T) {
	got, err := parseArgs(nil)
	if err != nil {
		t.Fatalf("parseArgs(nil): %v", err)
	}
	if got.cfg.nat != natMeasure {
		t.Errorf("default -nat parsed to %q, want the measure sentinel %q", got.cfg.nat, natMeasure)
	}
}

// TestNATFlagStillForcesAValue pins the override. An operator who knows their peer is
// CGNAT-bound must be able to pin it, and — the reason this is not negotiable — every
// existing harness and doc that passes `-nat turn` has to keep meaning what it meant.
//
// The mutation this catches: deleting the flag in favour of the sensor.
func TestNATFlagStillForcesAValue(t *testing.T) {
	tests := []struct {
		arg     string
		want    overlay.NATType
		wantErr bool
	}{
		{arg: "auto", want: natMeasure},
		{arg: "direct", want: overlay.NATDirect},
		{arg: "turn", want: overlay.NATRelayed},
		{arg: "upnp", wantErr: true},
		// A typo must not silently fall through to the permissive class, which is the
		// same reasoning parseNAT already carried.
		{arg: "", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.arg, func(t *testing.T) {
			got, err := parseArgs([]string{"-nat", tt.arg})
			if tt.wantErr {
				if err == nil {
					t.Fatalf("-nat %q: want an error, got nat=%q", tt.arg, got.cfg.nat)
				}
				return
			}
			if err != nil {
				t.Fatalf("-nat %q: %v", tt.arg, err)
			}
			if got.cfg.nat != tt.want {
				t.Errorf("-nat %q parsed to %q, want %q", tt.arg, got.cfg.nat, tt.want)
			}
		})
	}
}

// TestTelemetryUsesTheMeasuredNATClass pins that the sensor actually reaches the wire.
// Every other sensor in this file fails loudly when unwired; this one fails SILENTLY,
// because "direct" is both the pre-sensor default and the most common true answer — a
// sensor that is never consulted looks exactly like a fleet with no TURN in it.
//
// The mutation this catches: sampleReport keeping the flag as the only source of NAT.
func TestTelemetryUsesTheMeasuredNATClass(t *testing.T) {
	tel := &telemetry{
		cfg:   callConfig{name: "behind-cgnat", uploadKbps: 6000, nat: natMeasure},
		links: links(nil, 0, overlay.NATRelayed),
	}
	if got := tel.sample().NAT; got != overlay.NATRelayed {
		t.Errorf("NAT = %q, want %q: the measured class did not reach the report", got, overlay.NATRelayed)
	}
}

// TestNATFlagOverridesTheSensor pins the override in the direction that matters in
// BOTH senses — an operator pinning a peer OPEN when the sensor would relay it, and
// pinning it CLOSED when the sensor would not.
//
// The mutation this catches: letting the sensor win unconditionally, which makes the
// flag inert while still parsing, warning about nothing, and appearing in -help.
func TestNATFlagOverridesTheSensor(t *testing.T) {
	tests := []struct {
		name     string
		declared overlay.NATType
		measured overlay.NATType
		want     overlay.NATType
	}{
		{name: "pinned direct beats a relayed measurement", declared: overlay.NATDirect, measured: overlay.NATRelayed, want: overlay.NATDirect},
		{name: "pinned turn beats a direct measurement", declared: overlay.NATRelayed, measured: overlay.NATDirect, want: overlay.NATRelayed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tel := &telemetry{
				cfg:   callConfig{name: "p", nat: tt.declared},
				links: links(nil, 0, tt.measured),
			}
			if got := tel.sample().NAT; got != tt.want {
				t.Errorf("NAT = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestReportNATFailsSafeWithoutASensor is the chicken-and-egg case, and the one this
// whole design has to get right. The classification is only observable AFTER a
// PeerConnection exists, so a peer that has just joined is unclassified — exactly the
// structural limitation media.RTTMemory already records for pairwise RTT ("first
// attachment is RTT-blind"). An unclassified peer must report the PERMISSIVE class.
//
// The mutations this catches:
//   - defaulting an unmeasured peer to NATRelayed: every peer would be a forced leaf
//     for its first seconds, so a meet's first tree could not be built at all and the
//     first coordinator election would have no eligible candidate.
//   - emitting the zero NATType: `nat,omitempty` drops it from the frame entirely and
//     the coordinator reads back "", which is neither class.
func TestReportNATFailsSafeWithoutASensor(t *testing.T) {
	// No Router at all — a probe-mode peer, or a managed peer in the window between
	// joining and its first edge.
	tel := &telemetry{cfg: callConfig{name: "fresh", nat: natMeasure}}
	if got := tel.sample().NAT; got != overlay.NATDirect {
		t.Errorf("NAT = %q from a peer with no sensor, want the permissive %q", got, overlay.NATDirect)
	}
	if got := sampleReport(callConfig{name: "fresh", nat: natMeasure}).NAT; got != overlay.NATDirect {
		t.Errorf("sampleReport NAT = %q, want the permissive %q", got, overlay.NATDirect)
	}
}

// --- TURN plumbing ----------------------------------------------------------

// TestTurnFlagsRequireACompleteCredentialSet pins the loud failure. A TURN URL with no
// credentials is worse than no TURN at all: the peer gathers no relay candidate, gets
// no error, and simply never connects to the one peer it needed the relay for — a
// symptom that points nowhere near the flag. Same reasoning as -media-ports.
//
// The mutation this catches: appending the URL to ICEServers and letting pion silently
// fail the allocation.
func TestTurnFlagsRequireACompleteCredentialSet(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr bool
	}{
		{name: "no turn at all", args: nil},
		{name: "complete set", args: []string{"-turn", "turn:1.2.3.4:3478", "-turn-user", "u", "-turn-pass", "p"}},
		{name: "url with no credentials", args: []string{"-turn", "turn:1.2.3.4:3478"}, wantErr: true},
		{name: "url with no password", args: []string{"-turn", "turn:1.2.3.4:3478", "-turn-user", "u"}, wantErr: true},
		{name: "url with no user", args: []string{"-turn", "turn:1.2.3.4:3478", "-turn-pass", "p"}, wantErr: true},
		{name: "credentials with no url", args: []string{"-turn-user", "u", "-turn-pass", "p"}, wantErr: true},
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

// TestICEServersFor pins how the two reachability aids reach pion. STUN and TURN are
// independent — a peer may have either, both or neither — and the TURN entry is
// useless without its credentials attached, which is the field a hand-rolled literal
// forgets.
//
// The mutation this catches: dropping Username/Credential, or overwriting the STUN
// entry rather than appending to it.
func TestICEServersFor(t *testing.T) {
	tests := []struct {
		name string
		cfg  callConfig
		want []webrtc.ICEServer
	}{
		{name: "neither", cfg: callConfig{}},
		{
			name: "stun only",
			cfg:  callConfig{stun: "stun:stun.example:19302"},
			want: []webrtc.ICEServer{{URLs: []string{"stun:stun.example:19302"}}},
		},
		{
			name: "turn only",
			cfg:  callConfig{turn: "turn:1.2.3.4:3478", turnUser: "u", turnPass: "p"},
			want: []webrtc.ICEServer{{URLs: []string{"turn:1.2.3.4:3478"}, Username: "u", Credential: "p"}},
		},
		{
			name: "both, STUN first",
			cfg:  callConfig{stun: "stun:stun.example:19302", turn: "turn:1.2.3.4:3478", turnUser: "u", turnPass: "p"},
			want: []webrtc.ICEServer{
				{URLs: []string{"stun:stun.example:19302"}},
				{URLs: []string{"turn:1.2.3.4:3478"}, Username: "u", Credential: "p"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := iceServersFor(tt.cfg)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("iceServersFor = %#v, want %#v", got, tt.want)
			}
		})
	}
}

// TestLinksSensorShapeIsWiredThrough is a compile-shaped assertion with a runtime
// tail: it exists so that adding the NAT return to media.Router.LinkStats without
// consuming it in telemetry is a build failure rather than a silently-ignored value.
func TestLinksSensorShapeIsWiredThrough(t *testing.T) {
	tel := &telemetry{
		cfg:   callConfig{name: "relay", nat: natMeasure},
		links: links([]metrics.PeerRTT{{Name: "a", RTTMs: 3}}, 1.5, overlay.NATRelayed),
	}
	rep := tel.sample()
	if len(rep.PeerRTT) != 1 || rep.LossPct != 1.5 || rep.NAT != overlay.NATRelayed {
		t.Errorf("got %+v, want all three link signals folded in", rep)
	}
}
