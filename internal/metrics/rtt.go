package metrics

import (
	"context"
	"log/slog"
	"math"
	"sync/atomic"
	"time"

	"github.com/SammyUrfen/conclave/internal/clock"
)

// UnreachableRTTMs is what a peer reports for RTTServerMs when its control-link
// probe does not come back.
//
// THE ZERO VALUE IS THE TRAP THIS CONSTANT EXISTS TO AVOID. Report.RTTServerMs is a
// float64 whose zero means both "never measured" and "a perfect 0 ms link", and
// arbiter.Score reads 0 ms as FULL MARKS on its largest term (weight 0.35). A probe
// that failed instantly — a closed socket returns without waiting — has an elapsed
// time of ~0, so recording the measurement would score a peer whose connection to the
// arbiter has just died ABOVE a healthy peer on a 20 ms link. The sensor must fail
// toward unusable.
//
// 1000 ms rather than +Inf or math.MaxFloat64 because it has to survive being
// marshalled to JSON (both of those do not) and has to read as a plausible quantity in
// a log line and on the dashboard. It only needs to exceed the two thresholds that act
// on it, with room to spare:
//
//   - arbiter.MaxUsefulRTTMs (300) — past this Score's RTT term is saturated at zero.
//   - coordinator.DegradedRTTMs (400) — past this the degradation dwell arms.
//
// Neither can be named here: arbiter and coordinator both import metrics, so importing
// either back is a cycle. The numbers are restated in this package's own test, and
// cmd/server — which sees all three packages — carries the test that the restatement
// is still true. Splitting it that way is deliberate: the local test catches somebody
// lowering this constant, the cmd/server test catches somebody raising a threshold
// past it, and one test alone catches only its own half.
const UnreachableRTTMs = 1000.0

// RTTProbe measures a peer's round trip to the arbiter over the control link and
// keeps the latest reading for the Reporter to fold into its next Report.
//
// It is a SEPARATE GOROUTINE from the Reporter rather than a call inside the sample
// function, and that is the whole design. Reporter.report calls sample() and then
// send() in sequence, so a probe that blocked inside sample would delay the telemetry
// frame by however long the link is slow — which is exactly the moment the frame
// matters most. Probing on its own cadence and handing back a value that is at worst
// one interval stale keeps the report path non-blocking.
//
// The probe function owns its own deadline. This type deliberately does not take a
// timeout: a context deadline is real time no matter what clock is injected, so
// owning one here would put an untestable wall-clock dependency inside a package the
// determinism gate holds to the injected clock. The caller builds the bounded context
// (cmd/peer does) and this type only measures and remembers.
type RTTProbe struct {
	log      *slog.Logger
	interval time.Duration
	clk      clock.Clock
	probe    func(context.Context) error

	// lastBits is the most recent reading, as math.Float64bits, or the sentinel
	// noReading. Atomic rather than mutex-guarded because it is written by exactly
	// one goroutine (Run's) and read by another (the Reporter's sample function),
	// with no invariant spanning two fields — the degenerate case a mutex would only
	// make more expensive to state.
	lastBits atomic.Uint64
}

// noReading is the lastBits sentinel for "this probe has not produced a value".
// math.Float64bits of a quiet NaN cannot collide with any real measurement, and
// unlike a second atomic bool it cannot be read out of step with the value it guards.
var noReading = math.Float64bits(math.NaN())

// NewRTTProbe builds a probe. A zero or negative interval falls back to
// DefaultInterval and a nil clk to clock.System(), matching every other seam here.
//
// probe performs one round trip and returns nil only if it completed. It must respect
// its context: an unbounded probe would wedge Run's goroutine and freeze the reading
// at its last value for the life of the process.
func NewRTTProbe(log *slog.Logger, interval time.Duration, clk clock.Clock, probe func(context.Context) error) *RTTProbe {
	if interval <= 0 {
		interval = DefaultInterval
	}
	if clk == nil {
		clk = clock.System()
	}
	p := &RTTProbe{
		log:      log.With(slog.String("component", "rtt-probe")),
		interval: interval,
		clk:      clk,
		probe:    probe,
	}
	p.lastBits.Store(noReading)
	return p
}

// LastMs returns the most recent round trip in milliseconds. ok is false until the
// first probe has completed, and the caller MUST leave Report.RTTServerMs unset in
// that case — writing 0 would be indistinguishable from a perfect link.
func (p *RTTProbe) LastMs() (float64, bool) {
	bits := p.lastBits.Load()
	if bits == noReading {
		return 0, false
	}
	return math.Float64frombits(bits), true
}

// Run probes once immediately — so a peer's first Report already carries a
// measurement, rather than leaving the field unset through the whole window in which
// a bootstrap election runs — then every interval until ctx is cancelled.
func (p *RTTProbe) Run(ctx context.Context) {
	p.probeOnce(ctx)

	ticker := p.clk.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C():
			p.probeOnce(ctx)
		}
	}
}

// probeOnce performs one round trip and records it. A failure records the
// unreachable ceiling rather than the elapsed time or nothing at all: see
// UnreachableRTTMs for why both alternatives are optimistic in the wrong direction.
func (p *RTTProbe) probeOnce(ctx context.Context) {
	start := p.clk.Now()
	err := p.probe(ctx)
	if err != nil {
		p.lastBits.Store(math.Float64bits(UnreachableRTTMs))
		p.log.Debug("control-link probe failed", slog.Any("error", err))
		return
	}
	ms := float64(p.clk.Now().Sub(start)) / float64(time.Millisecond)
	if ms < 0 {
		// A clock that went backwards. Report nothing new rather than a negative
		// latency, which less() in Score would clamp to a perfect 1.0.
		return
	}
	p.lastBits.Store(math.Float64bits(ms))
}
