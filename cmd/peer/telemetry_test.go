package main

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/SammyUrfen/conclave/internal/metrics"
	"github.com/SammyUrfen/conclave/internal/signaling"
)

func measured(v float64) func() (float64, bool) { return func() (float64, bool) { return v, true } }

// unmeasured returns a NON-ZERO value alongside ok=false, and the non-zero is the
// whole point. A sensor that reports (0, false) makes "the field was left alone" and
// "the field was overwritten with the sensor's zero" indistinguishable, so a test
// built on it passes against the exact mutation it exists to catch.
func unmeasured() func() (float64, bool) { return func() (float64, bool) { return 999, false } }
func links(r []metrics.PeerRTT, loss float64) func() ([]metrics.PeerRTT, float64) {
	return func() ([]metrics.PeerRTT, float64) { return r, loss }
}

// TestTelemetrySampleFoldsInEverySensor pins that all four live signals reach the
// Report. Every one of these was structurally zero before the sensors existed, and
// every one fails SILENTLY when unwired — a zero CPUPct, LossPct or RTTServerMs is a
// legal value that reads as "this machine is perfect", not as "nobody measured".
func TestTelemetrySampleFoldsInEverySensor(t *testing.T) {
	tel := &telemetry{
		cfg:   callConfig{name: "relay", uploadKbps: 6000, coordinatable: true},
		cpu:   measured(73.5),
		rtt:   measured(18.25),
		links: links([]metrics.PeerRTT{{Name: "b", RTTMs: 9}, {Name: "a", RTTMs: 3}}, 4.5),
	}
	rep := tel.sample()

	if rep.Name != "relay" || rep.UploadKbps != 6000 || !rep.Coordinatable {
		t.Errorf("the config half of the report was lost: %+v", rep)
	}
	if rep.CPUPct != 73.5 {
		t.Errorf("CPUPct = %v, want 73.5: the CPU sensor is not wired", rep.CPUPct)
	}
	if rep.RTTServerMs != 18.25 {
		t.Errorf("RTTServerMs = %v, want 18.25: the control-link sensor is not wired", rep.RTTServerMs)
	}
	if rep.LossPct != 4.5 {
		t.Errorf("LossPct = %v, want 4.5: the loss sensor is not wired", rep.LossPct)
	}
	if len(rep.PeerRTT) != 2 {
		t.Fatalf("PeerRTT = %v, want 2 entries: the pairwise-RTT sensor is not wired", rep.PeerRTT)
	}
	// Normalised on the way out, as metrics.Report.PeerRTT requires — the ordering is
	// what keeps two peers holding identical measurements emitting identical frames.
	if rep.PeerRTT[0].Name != "a" || rep.PeerRTT[1].Name != "b" {
		t.Errorf("PeerRTT is not normalised: %v", rep.PeerRTT)
	}
}

// TestTelemetryLeavesUnmeasuredFieldsAlone guards the distinction the wire format
// cannot express. Report.CPUPct and RTTServerMs are float64s whose zero means BOTH
// "not measured" and "perfect", and arbiter.Score reads the perfect meaning. A sensor
// with nothing to say must therefore leave the field untouched rather than write its
// own zero — the same value, reached by a path that would also overwrite a good
// reading the moment a sensor started failing.
//
// The mutation this catches: dropping the ok checks and assigning unconditionally.
// Every other test in this file still passes, because they all supply live sensors.
func TestTelemetryLeavesUnmeasuredFieldsAlone(t *testing.T) {
	tel := &telemetry{
		cfg: callConfig{name: "leaf", uploadKbps: 1200},
		cpu: unmeasured(),
		rtt: unmeasured(),
		// links nil: this peer has no Router.
	}
	rep := tel.sample()

	if rep.CPUPct != 0 {
		t.Errorf("CPUPct = %v from a sampler that measured nothing", rep.CPUPct)
	}
	if rep.RTTServerMs != 0 {
		t.Errorf("RTTServerMs = %v from a probe that has not run", rep.RTTServerMs)
	}
	if rep.PeerRTT != nil {
		t.Errorf("PeerRTT = %v with no Router wired", rep.PeerRTT)
	}
	if rep.LossPct != 0 {
		t.Errorf("LossPct = %v with no Router wired", rep.LossPct)
	}
}

// TestTelemetryToleratesNilSensors pins that a peer with no sensors at all still
// reports. Every seam here is optional — a probe-mode peer has no Router and a
// non-Linux host has no CPU source — and a nil deref in the telemetry path would
// crash the peer rather than degrade it. Telemetry must never fault the peer; that is
// the same rule metrics.Reporter already applies to a failed send.
func TestTelemetryToleratesNilSensors(t *testing.T) {
	tel := &telemetry{cfg: callConfig{name: "bare", uploadKbps: 100}}
	rep := tel.sample()
	if rep.Name != "bare" || rep.UploadKbps != 100 {
		t.Errorf("got %+v, want the config half intact", rep)
	}
}

// TestTelemetryReportsAnUnreachableArbiter pins the sharp direction end to end at the
// peer: with the control link gone, the peer reports the unreachable ceiling rather
// than the 0 that would score it as having a perfect link. This is the one failure
// where a broken sensor makes a peer look BETTER than a healthy one, which is why it
// is pinned again here and not only in metrics.
func TestTelemetryReportsAnUnreachableArbiter(t *testing.T) {
	probe := metrics.NewRTTProbe(testLogger(), 0, nil, func(context.Context) error {
		return errors.New("socket closed")
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	probe.Run(ctx) // one immediate probe, then the cancelled context ends the loop

	tel := &telemetry{cfg: callConfig{name: "cut-off"}, rtt: probe.LastMs}
	if got := tel.sample().RTTServerMs; got != metrics.UnreachableRTTMs {
		t.Errorf("RTTServerMs = %v, want the unreachable ceiling %v", got, metrics.UnreachableRTTMs)
	}
}

// TestTelemetryMatchesSampleReportOnTheConfigHalf pins that adding sensors changed
// nothing about what the flags contribute. sampleReport stays the pure config half
// that the existing wire tests assert against; telemetry.sample only ADDS to it.
func TestTelemetryMatchesSampleReportOnTheConfigHalf(t *testing.T) {
	cfg := callConfig{name: "n", uploadKbps: 4200, nat: "direct", coordinatable: false}
	pure := sampleReport(cfg)
	got := (&telemetry{cfg: cfg, cpu: measured(50), rtt: measured(5)}).sample()

	if got.Name != pure.Name || got.UploadKbps != pure.UploadKbps ||
		got.NAT != pure.NAT || got.Coordinatable != pure.Coordinatable {
		t.Errorf("the config half drifted:\n got  %+v\n want %+v", got, pure)
	}
}

// TestTelemetryAlwaysMarshals pins that no sensor reading can stop this peer's
// telemetry. A NaN or an infinity marshals to invalid JSON — encoding/json returns an
// error rather than a frame — so one bad reading would silently end the report stream
// and the coordinator would watch the peer go quiet with nothing logged to say why.
//
// The sensors clamp at their own boundaries; this asserts the consequence at the one
// place that matters, which is the only place a reader can check it without trusting
// three packages at once.
func TestTelemetryAlwaysMarshals(t *testing.T) {
	poison := []struct {
		name string
		val  float64
	}{
		{name: "NaN", val: math.NaN()},
		{name: "+Inf", val: math.Inf(1)},
		{name: "-Inf", val: math.Inf(-1)},
	}
	for _, p := range poison {
		t.Run(p.name, func(t *testing.T) {
			tel := &telemetry{
				cfg:   callConfig{name: "n"},
				cpu:   measured(p.val),
				rtt:   measured(p.val),
				links: links([]metrics.PeerRTT{{Name: "a", RTTMs: p.val}}, p.val),
			}
			if _, err := json.Marshal(tel.sample()); err != nil {
				t.Errorf("a %s reading made the report unmarshallable: %v", p.name, err)
			}
		})
	}
}

// TestSensorsReachTheWire is the end-to-end proof, and it exists because every test
// above it can pass while the peer sends none of this.
//
// telemetry.sample being correct proves a struct is populated. It does not prove that
// runCall builds a telemetry rather than calling the old pure sampleReport, that the
// RTT probe goroutine is ever started, or that the probe is wired to the real
// signaling client rather than to a stub. "Populated correctly and never sent" is the
// exact shape of an unwired sensor, and it is invisible in a unit test — which is why
// this runs the real runCall against a real Hub over a real WebSocket and asserts on
// what the SERVER received.
//
// It waits for the SECOND report on purpose. The CPU sampler is a delta sensor over
// /proc/stat's cumulative counters, so its first reading has no baseline to diff
// against and legitimately reports nothing; asserting on report #1 would pin the
// absence rather than the presence.
func TestSensorsReachTheWire(t *testing.T) {
	obs := &captureObserver{}
	hub := signaling.NewHub(testLogger())
	hub.SetObserver(obs)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ws", hub.ServeWS)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	opts, err := parseArgs([]string{
		"-call", "-managed", "-name", "relay", "-server", srv.URL,
		"-room", "sensor-test", "-heartbeat", "20ms", "-upload-kbps", "1500",
	})
	if err != nil {
		t.Fatalf("parseArgs: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runCall(ctx, testLogger(), opts.cfg) }()

	deadline := time.Now().Add(20 * time.Second)
	var rep metrics.Report
	for time.Now().Before(deadline) {
		if obs.count(signaling.TypeMetrics) >= 2 {
			if err := json.Unmarshal(obs.last(signaling.TypeMetrics), &rep); err != nil {
				t.Fatalf("decode report: %v", err)
			}
			if rep.CPUPct > 0 {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}

	// A host that is genuinely 0.00% busy across a 3 s interval does not exist while
	// this test's own process is running, so a zero here means the sensor never ran.
	if rep.CPUPct <= 0 || rep.CPUPct > 100 {
		t.Errorf("CPUPct on the wire = %v; the host CPU sensor is not wired into runCall", rep.CPUPct)
	}
	// Loopback, so this is small — but a probe that never ran leaves it at exactly 0,
	// and a probe wired to a dead client reports the unreachable ceiling.
	if rep.RTTServerMs <= 0 {
		t.Errorf("RTTServerMs on the wire = %v; the control-link probe is not wired into runCall", rep.RTTServerMs)
	}
	if rep.RTTServerMs >= metrics.UnreachableRTTMs {
		t.Errorf("RTTServerMs on the wire = %v; the probe is reporting the arbiter unreachable over loopback", rep.RTTServerMs)
	}
	t.Logf("on the wire: cpu_pct=%.2f rtt_server_ms=%.4f", rep.CPUPct, rep.RTTServerMs)

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runCall returned %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runCall did not return after its context was cancelled")
	}
}
