package metrics

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/SammyUrfen/conclave/internal/overlay"
)

// TestReportWireFormat pins Report's JSON key set now that it carries the election
// opt-in. The other keys are asserted alongside it deliberately: this field was
// added to a frozen wire type, and the thing to prove is that nothing else moved.
func TestReportWireFormat(t *testing.T) {
	tests := []struct {
		name string
		in   Report
		want string
	}{
		{
			name: "willing peer",
			in: Report{
				Name: "relay", UploadKbps: 6000, NAT: overlay.NATDirect,
				RTTServerMs: 24, LossPct: 0.5, CPUPct: 31, Coordinatable: true,
			},
			want: `{"name":"relay","upload_kbps":6000,"nat":"direct",` +
				`"rtt_server_ms":24,"loss_pct":0.5,"cpu_pct":31,"coordinatable":true}`,
		},
		{
			// The declaration must be on the wire even when false, or a "no" is
			// indistinguishable from an old build that never had the field.
			name: "unwilling peer still emits the key",
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

// TestCoordinatableFailsClosed pins the conservative direction and, more importantly,
// the shape of the hazard: a bool has two states but the system has three situations
// ("willing", "unwilling", "has not said"). Decoding cannot tell the third from the
// second, which is why the consumer must gate on whether a report arrived AT ALL and
// never on this field alone.
func TestCoordinatableFailsClosed(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want bool
	}{
		{name: "explicit true", raw: `{"name":"a","coordinatable":true}`, want: true},
		{name: "explicit false", raw: `{"name":"a","coordinatable":false}`, want: false},
		// The dangerous one: an older peer that predates the field looks exactly
		// like a peer that declined. Failing closed means it is never conscripted.
		{name: "absent means unwilling", raw: `{"name":"a"}`, want: false},
		{name: "null means unwilling", raw: `{"name":"a","coordinatable":null}`, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got Report
			if err := json.Unmarshal([]byte(tt.raw), &got); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if got.Coordinatable != tt.want {
				t.Errorf("Coordinatable = %v, want %v", got.Coordinatable, tt.want)
			}
		})
	}

	t.Run("round-trips", func(t *testing.T) {
		for _, want := range []bool{true, false} {
			raw, err := json.Marshal(Report{Name: "a", Coordinatable: want})
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			var back Report
			if err := json.Unmarshal(raw, &back); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if back.Coordinatable != want {
				t.Errorf("round-trip of %v gave %v", want, back.Coordinatable)
			}
		}
	})
}

// TestRebuildWindow pins the constant and the family relationship that justifies its
// value. It sits in this package because it is a metrics-plane cadence — "how long
// until every peer has reported at least once" — and because both arbiter and
// coordinator import metrics while coordinator may not import arbiter.
func TestRebuildWindow(t *testing.T) {
	t.Run("is three seconds", func(t *testing.T) {
		if RebuildWindow != 3*time.Second {
			t.Errorf("RebuildWindow = %v, want 3s", RebuildWindow)
		}
	})

	// The load-bearing relationship: a window shorter than one telemetry period
	// could not collect a full round of reports however lucky the timing, so a
	// freshly promoted coordinator would be guaranteed to build on a partial view.
	t.Run("spans at least one telemetry period", func(t *testing.T) {
		if RebuildWindow < DefaultInterval {
			t.Errorf("RebuildWindow %v is shorter than one metrics tick %v", RebuildWindow, DefaultInterval)
		}
	})

	// And it must stay well under the gone threshold: a rebuild that outlasted the
	// liveness verdict would publish its first tree using peers already declared
	// dead.
	t.Run("finishes before peers can be declared gone", func(t *testing.T) {
		if gone := GoneAfter(HeartbeatInterval); RebuildWindow >= gone {
			t.Errorf("RebuildWindow %v must be shorter than the gone threshold %v", RebuildWindow, gone)
		}
	})

	t.Run("covers several heartbeats", func(t *testing.T) {
		if RebuildWindow < DegradedAfter(HeartbeatInterval) {
			t.Errorf("RebuildWindow %v is shorter than the degraded threshold %v",
				RebuildWindow, DegradedAfter(HeartbeatInterval))
		}
	})
}
