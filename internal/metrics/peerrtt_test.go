package metrics

import (
	"encoding/json"
	"testing"

	"github.com/SammyUrfen/conclave/internal/overlay"
)

// TestPeerRTTWireFormat pins the new field's JSON shape AND that adding it moved
// nothing else on a frozen wire type. The second half is the point: Report is
// consumed by a coordinator that may be running an older build, so the field must be
// additive and absent-by-default.
func TestPeerRTTWireFormat(t *testing.T) {
	tests := []struct {
		name string
		in   Report
		want string
	}{
		{
			name: "peer RTT rides alongside the existing keys",
			in: Report{
				Name: "relay", UploadKbps: 6000, NAT: overlay.NATDirect,
				RTTServerMs: 24, LossPct: 0.5, CPUPct: 31, Coordinatable: true,
				PeerRTT: []PeerRTT{{Name: "a", RTTMs: 12.5}, {Name: "b", RTTMs: 3}},
			},
			want: `{"name":"relay","upload_kbps":6000,"nat":"direct",` +
				`"rtt_server_ms":24,"loss_pct":0.5,"cpu_pct":31,"coordinatable":true,` +
				`"peer_rtt":[{"name":"a","rtt_ms":12.5},{"name":"b","rtt_ms":3}]}`,
		},
		{
			// A peer with no measured neighbours — every peer, on its first report —
			// must not emit the key at all. An empty array would be indistinguishable
			// on the receiving side from "measured, and everything is zero".
			name: "no measurements omits the key entirely",
			in:   Report{Name: "leaf-b", UploadKbps: 1200},
			want: `{"name":"leaf-b","upload_kbps":1200,"coordinatable":false}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := json.Marshal(tt.in)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			if string(got) != tt.want {
				t.Errorf("got  %s\nwant %s", got, tt.want)
			}
		})
	}
}

// TestPeerRTTIsAnOrderedSlice pins the same invariant metrics.Heartbeat.Children
// carries, for the same reason: this value becomes overlay.Node.RTT, which shapes
// the tree. A map on the wire would marshal in sorted order but would give the
// PRODUCER a map to iterate, and Go randomises that — so two peers with identical
// measurements could emit different frames, and the first tree of an epoch would
// depend on nothing but a hash seed.
//
// The mutation this catches: declaring the field map[string]float64.
func TestPeerRTTIsAnOrderedSlice(t *testing.T) {
	r := Report{PeerRTT: []PeerRTT{
		{Name: "zulu", RTTMs: 1},
		{Name: "alpha", RTTMs: 2},
		{Name: "mike", RTTMs: 3},
	}}
	r.Normalize()
	want := []string{"alpha", "mike", "zulu"}
	if len(r.PeerRTT) != len(want) {
		t.Fatalf("got %d entries, want %d", len(r.PeerRTT), len(want))
	}
	for i, w := range want {
		if r.PeerRTT[i].Name != w {
			t.Errorf("entry %d = %q, want %q", i, r.PeerRTT[i].Name, w)
		}
	}
}

// TestNormalizeIsStableAcrossRepeats pins that Normalize is idempotent — a receiver
// may call it defensively after a sender already did, exactly as Heartbeat.Normalize
// documents.
func TestNormalizeIsStableAcrossRepeats(t *testing.T) {
	r := Report{PeerRTT: []PeerRTT{{Name: "b", RTTMs: 1}, {Name: "a", RTTMs: 2}}}
	r.Normalize()
	first, _ := json.Marshal(r)
	r.Normalize()
	second, _ := json.Marshal(r)
	if string(first) != string(second) {
		t.Errorf("Normalize is not idempotent:\n%s\n%s", first, second)
	}
}
