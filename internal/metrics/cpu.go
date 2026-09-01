package metrics

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"strconv"
)

// procStatPath is the kernel's aggregate CPU accounting file. A variable rather
// than a constant only so a test can point it at a fixture; production never
// reassigns it.
var procStatPath = "/proc/stat"

// minCPUFields is how many numbers the aggregate line must carry before a reading
// means anything: user, nice, system, idle. Everything past that (iowait, irq,
// softirq, steal, guest, guest_nice) is summed when present and absent on older
// kernels, so requiring all ten would refuse to work on a machine that can report
// perfectly well.
const minCPUFields = 4

// cpuTimes is one reading of the host's cumulative CPU accounting, in whatever
// unit the kernel counts in (USER_HZ jiffies). The unit never matters because
// nothing here reads an absolute value — only the ratio of two deltas — which is
// also why this needs no clock and sits outside the determinism gate's concerns.
type cpuTimes struct {
	// busy is every non-idle counter summed: time the CPU spent doing work.
	busy uint64
	// total is busy plus idle. Kept rather than derived so the ratio is a single
	// subtraction on each side and a bad read cannot make them disagree.
	total uint64
}

// CPUSampler turns /proc/stat's cumulative counters into a live busy percentage.
//
// It is a DELTA sensor and must be, because the file holds totals since boot: one
// read tells you what the host has averaged over its entire uptime, which on a
// machine that has been up for a week is a number that barely moves and is never
// the current load. So the first Sample primes the baseline and reports nothing,
// and every one after it measures the interval since the previous call.
//
// The caller owns the cadence. Nothing here schedules, sleeps, or reads a clock —
// the interval is however long it has been since the last call, which is exactly
// what the ratio of the deltas already expresses. That keeps the sampler a pure
// function of two file contents and lets it be tested without time at all.
//
// FAILS SILENT, DELIBERATELY, AND ONLY IN THE SAFE DIRECTION. Every path that
// cannot produce an honest number returns ok=false rather than a value: no
// /proc/stat (any non-Linux host), an unparseable read, no elapsed time, or
// counters that went backwards. metrics.Report's zero CPUPct then stands, which
// arbiter.Score reads as a fully idle host. That is the optimistic direction and it
// is the pre-existing behaviour of every build before this sensor, so a machine
// without the sensor is scored exactly as it was before rather than being newly
// penalised for the absence. A sensor that guessed would be worse than one that
// declines to speak.
type CPUSampler struct {
	prev cpuTimes
	// primed records that prev holds a real reading. A bool rather than testing
	// prev against its zero value, because a host that has genuinely accumulated
	// zero jiffies is indistinguishable from an unprimed sampler otherwise — rare,
	// but it would silently report a boot-average as a live reading.
	primed bool
}

// Sample reads /proc/stat and returns the host's busy percentage over the interval
// since the previous call. ok is false when no honest number is available; see the
// type comment for the four cases.
func (s *CPUSampler) Sample() (float64, bool) {
	raw, err := os.ReadFile(procStatPath)
	if err != nil {
		return 0, false
	}
	return s.sampleFrom(raw)
}

// sampleFrom is Sample's testable core: the same logic against a supplied file
// body, so every branch — including the ones a real kernel will not produce on
// demand — is reachable from a table.
func (s *CPUSampler) sampleFrom(raw []byte) (float64, bool) {
	cur, err := parseCPUTimes(raw)
	if err != nil {
		// The baseline is deliberately KEPT. Overwriting it with the zero value
		// would make the next call a delta against zero — the host's whole
		// boot-average, reported as the last few seconds.
		return 0, false
	}
	prev, primed := s.prev, s.primed
	s.prev, s.primed = cur, true
	if !primed {
		return 0, false
	}
	// Compare before subtracting: these are unsigned, so a backwards counter
	// (a bad read, or a container's CPU accounting namespace changing under us)
	// would underflow into an enormous delta rather than a negative one.
	if cur.total < prev.total || cur.busy < prev.busy {
		return 0, false
	}
	dTotal := cur.total - prev.total
	if dTotal == 0 {
		// No jiffies elapsed: two reads inside one tick, or a wedged clock. The
		// ratio is 0/0, and returning it would put a NaN on the wire. Score's
		// more() clamps NaN to the worst case, so it would not corrupt an
		// election — but it would report a wedged host as maximally busy on the
		// dashboard, which is a different lie.
		return 0, false
	}
	pct := float64(cur.busy-prev.busy) / float64(dTotal) * 100
	// Clamp at the sensor rather than relying on Score's own clamp: an
	// out-of-range value absorbed downstream is a sensor fault nobody ever sees.
	if pct < 0 {
		return 0, true
	}
	if pct > 100 {
		return 100, true
	}
	return pct, true
}

// parseCPUTimes extracts the aggregate "cpu " line from a /proc/stat body.
//
// The aggregate line is the one whose first field is exactly "cpu"; the per-core
// "cpu0", "cpu1" … lines must not match it, which is why this compares the field
// rather than the line prefix. Getting that wrong reports one core's load as the
// whole machine's.
func parseCPUTimes(raw []byte) (cpuTimes, error) {
	for _, line := range bytes.Split(raw, []byte("\n")) {
		fields := bytes.Fields(line)
		if len(fields) == 0 || string(fields[0]) != "cpu" {
			continue
		}
		nums := fields[1:]
		if len(nums) < minCPUFields {
			return cpuTimes{}, fmt.Errorf("aggregate cpu line has %d fields, want at least %d", len(nums), minCPUFields)
		}
		var t cpuTimes
		for i, f := range nums {
			v, err := strconv.ParseUint(string(f), 10, 64)
			if err != nil {
				return cpuTimes{}, fmt.Errorf("cpu field %d (%q): %w", i, f, err)
			}
			t.total += v
			// Fields 3 and 4 are idle and iowait. iowait counts as IDLE, not busy:
			// a host blocked on disk has CPU to spare, and scoring it as saturated
			// would demote a coordinator that is perfectly able to compute a tree.
			if i != 3 && i != 4 {
				t.busy += v
			}
		}
		return t, nil
	}
	return cpuTimes{}, errors.New("no aggregate cpu line in /proc/stat")
}
