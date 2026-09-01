package metrics

import (
	"math"
	"testing"
)

// procStat builds a synthetic /proc/stat body. Only the aggregate "cpu " line is
// load-bearing; the rest is present because the real file has it and a parser that
// only works on a one-line file would pass here and fail on every real host.
func procStat(agg string) string {
	return agg + "\n" +
		"cpu0 100 0 100 800 0 0 0 0 0 0\n" +
		"intr 12345 0 0\n" +
		"ctxt 987654\n" +
		"btime 1750000000\n" +
		"procs_running 2\n"
}

// TestParseCPUTimes covers the shapes of /proc/stat a real kernel can produce, and
// the shapes a broken read can produce.
//
// The mutation each case catches:
//   - "standard 10-field line": summing only the first four fields (a common
//     shortcut) leaves steal/guest out of total and inflates every later busy%.
//   - "idle includes iowait": counting iowait as busy reports a host blocked on disk
//     as CPU-saturated, which would demote a perfectly good coordinator.
//   - "short line": a kernel without steal/guest must still parse, not error.
//   - the three failure rows: each must be an error rather than a zero value, because
//     a silent 0 here means "100% CPU free" — the most optimistic possible lie from a
//     sensor that is actually broken.
func TestParseCPUTimes(t *testing.T) {
	tests := []struct {
		name      string
		in        string
		wantBusy  uint64
		wantTotal uint64
		wantErr   bool
	}{
		{
			name: "standard 10-field line",
			// user nice system idle iowait irq softirq steal guest guest_nice
			in:        procStat("cpu  100 20 50 700 30 5 10 15 0 0"),
			wantBusy:  100 + 20 + 50 + 5 + 10 + 15, // everything but idle+iowait
			wantTotal: 100 + 20 + 50 + 700 + 30 + 5 + 10 + 15,
		},
		{
			name:      "idle includes iowait",
			in:        procStat("cpu  0 0 0 0 500 0 0 0 0 0"),
			wantBusy:  0,
			wantTotal: 500,
		},
		{
			name:      "short line from an older kernel",
			in:        procStat("cpu  10 0 10 80"),
			wantBusy:  20,
			wantTotal: 100,
		},
		{
			name:    "no aggregate line",
			in:      "cpu0 1 2 3 4\nintr 5\n",
			wantErr: true,
		},
		{
			name:    "too few fields to be meaningful",
			in:      procStat("cpu  1 2"),
			wantErr: true,
		},
		{
			name:    "non-numeric field",
			in:      procStat("cpu  100 20 fifty 700"),
			wantErr: true,
		},
		{
			name:    "empty file",
			in:      "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseCPUTimes([]byte(tt.in))
			if tt.wantErr {
				if err == nil {
					t.Fatalf("want error, got %+v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if got.busy != tt.wantBusy || got.total != tt.wantTotal {
				t.Errorf("got busy=%d total=%d, want busy=%d total=%d",
					got.busy, got.total, tt.wantBusy, tt.wantTotal)
			}
		})
	}
}

// TestCPUSamplerNeedsTwoReads pins the property that makes this a DELTA sensor and
// not an absolute one: /proc/stat holds cumulative counters since boot, so a single
// read says only what the host has averaged over its whole uptime — which on a
// long-lived machine is a number that never moves and is never the current load.
//
// The mutation this catches: returning ok on the first sample. That would ship a
// boot-average as if it were a live reading, and it would look plausible.
func TestCPUSamplerNeedsTwoReads(t *testing.T) {
	var s CPUSampler
	if _, ok := s.sampleFrom([]byte(procStat("cpu  100 0 100 800 0 0 0 0 0 0"))); ok {
		t.Fatal("first sample must not report a value: there is no baseline to diff against")
	}
	got, ok := s.sampleFrom([]byte(procStat("cpu  200 0 200 1600 0 0 0 0 0 0")))
	if !ok {
		t.Fatal("second sample must report a value")
	}
	// busy delta 200, total delta 1000 => 20%.
	if math.Abs(got-20) > 1e-9 {
		t.Errorf("got %v%%, want 20%%", got)
	}
}

// TestCPUSamplerDeltas walks a sampler through a scripted sequence of /proc/stat
// bodies and asserts the percentage after each one.
//
// The mutations these catch:
//   - "fully busy"/"fully idle": a busy/total mix-up passes a 50% case and fails here.
//   - "no time passed": dividing by a zero total delta yields NaN, which propagates
//     into arbiter.Score's more() and is clamped to 0 — silently reporting a wedged
//     host as maximally CPU-free. It must report nothing instead.
//   - "counters went backwards": /proc/stat is monotonic, so this only happens on a
//     bad read or a container namespace switch. Treating it as a huge delta produces
//     a wild value; it must re-baseline and report nothing.
//   - "re-baselines after a gap": the sample after an unusable one must be measured
//     against the LAST read, not the last usable one, or the gap's activity is
//     attributed to the following interval.
func TestCPUSamplerDeltas(t *testing.T) {
	steps := []struct {
		name    string
		stat    string
		wantPct float64
		wantOK  bool
	}{
		{name: "baseline", stat: "cpu  0 0 0 0 0 0 0 0 0 0", wantOK: false},
		{name: "fully busy", stat: "cpu  100 0 0 0 0 0 0 0 0 0", wantPct: 100, wantOK: true},
		{name: "fully idle", stat: "cpu  100 0 0 100 0 0 0 0 0 0", wantPct: 0, wantOK: true},
		{name: "no time passed", stat: "cpu  100 0 0 100 0 0 0 0 0 0", wantOK: false},
		{name: "counters went backwards", stat: "cpu  1 0 0 1 0 0 0 0 0 0", wantOK: false},
		{name: "re-baselines after a gap", stat: "cpu  4 0 0 2 0 0 0 0 0 0", wantPct: 75, wantOK: true},
	}
	var s CPUSampler
	for _, st := range steps {
		t.Run(st.name, func(t *testing.T) {
			got, ok := s.sampleFrom([]byte(procStat(st.stat)))
			if ok != st.wantOK {
				t.Fatalf("ok = %v, want %v (got %v%%)", ok, st.wantOK, got)
			}
			if ok && math.Abs(got-st.wantPct) > 1e-9 {
				t.Errorf("got %v%%, want %v%%", got, st.wantPct)
			}
		})
	}
}

// TestCPUSamplerRejectsUnparseableWithoutLosingItsBaseline pins that a transient bad
// read does not corrupt the sampler. The mutation: storing the zero cpuTimes on a
// parse error, which makes the NEXT sample a delta against zero — i.e. the host's
// entire boot-average, reported as the current instant.
func TestCPUSamplerRejectsUnparseableWithoutLosingItsBaseline(t *testing.T) {
	var s CPUSampler
	s.sampleFrom([]byte(procStat("cpu  0 0 0 0 0 0 0 0 0 0")))
	if _, ok := s.sampleFrom([]byte("garbage")); ok {
		t.Fatal("an unparseable read must not report a value")
	}
	got, ok := s.sampleFrom([]byte(procStat("cpu  30 0 0 70 0 0 0 0 0 0")))
	if !ok {
		t.Fatal("the sample after a bad read must report a value")
	}
	if math.Abs(got-30) > 1e-9 {
		t.Errorf("got %v%%, want 30%% — the baseline was lost on the bad read", got)
	}
}

// TestCPUSamplerClampsToRange pins that the output is always a legal percentage.
// arbiter.Score divides CPUFreePct by 100 and feeds more(), which clamps — so an
// out-of-range value here would be silently absorbed there and never noticed. The
// clamp belongs at the sensor, where it is visible.
func TestCPUSamplerClampsToRange(t *testing.T) {
	var s CPUSampler
	s.sampleFrom([]byte(procStat("cpu  0 0 0 100 0 0 0 0 0 0")))
	// busy grows by more than total: impossible from a real kernel, but a namespace
	// change or a short read can fabricate it.
	got, ok := s.sampleFrom([]byte(procStat("cpu  500 0 0 0 0 0 0 0 0 0")))
	if !ok {
		t.Fatal("want a value")
	}
	if got < 0 || got > 100 {
		t.Errorf("got %v%%, which is not a percentage", got)
	}
}

// TestSampleCPUOnThisHost is the only test that touches the real /proc/stat. It
// asserts the shape rather than a value, because the value is whatever this machine
// is doing. On a non-Linux host the sampler must report nothing rather than error —
// a missing sensor is a supported state (metrics.Report's zero value), a lying one
// is not.
func TestSampleCPUOnThisHost(t *testing.T) {
	var s CPUSampler
	s.Sample() // prime; the first read never yields a value
	got, ok := s.Sample()
	if !ok {
		t.Skip("no usable /proc/stat on this host; the sampler correctly reported nothing")
	}
	if got < 0 || got > 100 {
		t.Errorf("host CPU busy = %v%%, which is not a percentage", got)
	}
	t.Logf("host CPU busy = %.1f%%", got)
}
