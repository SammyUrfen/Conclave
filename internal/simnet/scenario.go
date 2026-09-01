package simnet

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"time"

	"github.com/SammyUrfen/conclave/internal/overlay"
)

// SettleTimeout bounds Settle in REAL time. It is a deadlock guard, and the only
// wall-clock reference anywhere in the harness — it never affects a trace, only
// whether a wedged test hangs forever or fails. Five seconds is far beyond any
// legitimate in-memory control-loop round trip (they complete in microseconds), so
// exceeding it means a deadlock, not slowness.
//
// Scenario.SettleTimeout overrides it per scenario; the method and this constant
// coexist because a method name lives in its type's namespace, not the package's.
const SettleTimeout = 5 * time.Second

// DefaultStart is the virtual instant a Scenario begins at when ScenarioConfig.Start
// is left zero. A LITERAL, never the wall clock: a scenario seeded from real time
// would produce a different trace on every run, which is the one thing this harness
// exists to prevent. The date itself carries no meaning beyond being far from the
// zero time, so a duration printed relative to it reads sensibly.
var DefaultStart = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// Scenario is a deterministic, replayable script over a Network and a VirtualClock:
// a list of steps at virtual offsets, a set of quiescence barriers, and a trace.
// The same seed and the same script always produce the same trace.
//
// It is NOT safe for concurrent use, and deliberately so. Every method runs on the
// driving goroutine — including the barrier the clock calls back into mid-Advance —
// so a scenario step may touch the Network and drive the loop under test without a
// lock. The concurrency lives on the other side of the barrier, in the control loop.
type Scenario struct {
	net   *Network
	clk   *VirtualClock
	rng   *rand.Rand
	start time.Time
	cons  overlay.Constraints

	steps         []scenarioStep
	observers     []func(at time.Duration)
	barriers      []scenarioBarrier
	order         []string
	settleTimeout time.Duration

	err error
}

// ScenarioConfig parameterises a run.
type ScenarioConfig struct {
	// Seed drives the scenario's single *rand.Rand — the only source of randomness
	// in the harness, which is what makes a failure reproducible from one number.
	Seed int64
	// Start is virtual t0. Zero means DefaultStart.
	Start time.Time
	// Constraints are the build parameters a scenario's control loop uses. Root,
	// Epoch, and Rev may be left zero; nextConstraints fills them per rebuild.
	Constraints overlay.Constraints
}

type scenarioStep struct {
	at  time.Duration
	seq int // registration order: the tiebreak for steps at the same instant
	f   func()
}

type scenarioBarrier struct {
	name string
	fn   func(context.Context) error
}

// NewScenario builds a Scenario over a fresh Network and VirtualClock, and wires the
// clock's advance barrier to Settle — so a timer fired part-way through an Advance is
// fully reacted to before the next deadline is chosen. Without that wiring, an
// Advance spanning several deadlines would order them by the Go scheduler rather than
// by the virtual timeline (see VirtualClock.SetBarrier).
func NewScenario(cfg ScenarioConfig) *Scenario {
	start := cfg.Start
	if start.IsZero() {
		start = DefaultStart
	}
	s := &Scenario{
		net:           New(),
		clk:           NewVirtualClock(start),
		rng:           rand.New(rand.NewSource(cfg.Seed)), //nolint:gosec // determinism, not secrecy
		start:         start,
		cons:          cfg.Constraints,
		settleTimeout: SettleTimeout,
	}
	s.clk.SetBarrier(func() {
		if err := s.Settle(); err != nil {
			s.Fail(err)
		}
	})
	return s
}

// Net returns the fleet under test.
func (s *Scenario) Net() *Network { return s.net }

// Clock returns the virtual clock, for wiring into the control loop under test.
func (s *Scenario) Clock() *VirtualClock { return s.clk }

// Rand returns the scenario's seeded source. Every random choice a scenario makes
// must come from here; a second source would make the seed a half-truth.
func (s *Scenario) Rand() *rand.Rand { return s.rng }

// Constraints returns the build parameters from the config, so the loop under test
// and the scenario's assertions cannot drift apart on what tree was asked for.
func (s *Scenario) Constraints() overlay.Constraints { return s.cons }

// At schedules f to run at virtual time t0+d. Steps at the same instant run in
// registration order. f runs on the Scenario's own goroutine, so it may touch the
// Network and drive the loop under test without a lock.
func (s *Scenario) At(d time.Duration, f func()) *Scenario {
	s.steps = append(s.steps, scenarioStep{at: d, seq: len(s.steps), f: f})
	return s
}

// Observe registers a callback invoked after every settle — which is after every
// step and after every mid-Advance timer fire — with the virtual offset of that
// moment. It is how a test records a trace to assert on afterwards.
func (s *Scenario) Observe(f func(at time.Duration)) *Scenario {
	s.observers = append(s.observers, f)
	return s
}

// AddBarrier registers a quiescence barrier — in practice a control loop's Sync,
// bound to a ctx. A scenario driving more than one loop registers one per loop, and
// they are settled in registration order.
//
// A barrier is only sound if the loop it round-trips through DRAINS ITS FIRED TIMERS
// before acking. A fire and a sync arriving at one parked select are resolved by
// Go's uniform-random choice, so a loop that acks without draining can report
// quiescent while the reaction the barrier exists to wait for has not happened.
func (s *Scenario) AddBarrier(name string, fn func(context.Context) error) *Scenario {
	s.barriers = append(s.barriers, scenarioBarrier{name: name, fn: fn})
	return s
}

// SettleTimeout overrides the real-time deadlock guard for this scenario. It exists
// so the guard itself is testable in milliseconds instead of five seconds; a real
// scenario should never need it.
func (s *Scenario) SettleTimeout(d time.Duration) *Scenario {
	s.settleTimeout = d
	return s
}

// ReportOrder fixes the order in which the fleet's telemetry reports are delivered
// for the next round. Names not listed report after the listed ones, in the
// Network's insertion order; names that are not members are ignored.
//
// It exists for the report-order properties: on the SETTLED path (everyone heard
// from inside the first-build window) no permutation of arrival order may change the
// converged tree, which is the direct encoding of the root-flap defect where the
// recompute was a pure function of which frame arrived first. On the STRAGGLER path
// the converged tree legitimately does depend on order, and this is what lets a test
// characterise that rather than be surprised by it.
func (s *Scenario) ReportOrder(names ...string) *Scenario {
	s.order = append([]string(nil), names...)
	return s
}

// Reporters resolves ReportOrder against the current fleet and returns the delivery
// order a scenario step should iterate. With no ReportOrder set it is simply the
// Network's insertion order — never a map range, which Go randomises.
func (s *Scenario) Reporters() []string {
	seen := make(map[string]bool, len(s.net.order))
	out := make([]string, 0, len(s.net.order))
	for _, name := range s.order {
		if seen[name] {
			continue
		}
		if _, member := s.net.nodes[name]; !member {
			continue
		}
		seen[name] = true
		out = append(out, name)
	}
	for _, name := range s.net.order {
		if !seen[name] {
			seen[name] = true
			out = append(out, name)
		}
	}
	return out
}

// Fail records err as the scenario's outcome and stops the run at the end of the
// current step.
//
// It exists because At takes a func() with no return: a step is a piece of a story,
// not a function call, and threading an error return through every one of them would
// make the common (infallible) case noisy. Only the FIRST failure is kept — a
// scenario that keeps running after one produces cascading noise that buries it.
func (s *Scenario) Fail(err error) {
	if s.err == nil && err != nil {
		s.err = err
	}
}

// Settle blocks until every registered barrier reports quiescent, or until the
// scenario's SettleTimeout of REAL time elapses. Scenario.Run calls it after each
// step, and the clock calls it after each timer fire.
func (s *Scenario) Settle() error {
	for _, b := range s.barriers {
		// context.WithTimeout rather than a timer: the guard must run on REAL time
		// (a virtual clock nobody is advancing would never expire it) without any
		// of the wall-clock calls the determinism gate bans.
		ctx, cancel := context.WithTimeout(context.Background(), s.settleTimeout)
		err := b.fn(ctx)
		cancel()
		if err != nil {
			return fmt.Errorf("simnet: barrier %q did not settle within %v: %w", b.name, s.settleTimeout, err)
		}
	}
	at := s.clk.Now().Sub(s.start)
	for _, f := range s.observers {
		f(at)
	}
	return nil
}

// Run executes every scheduled step in virtual-time order, advancing the clock
// between them and settling after each. It returns the first error a step reported
// through Fail, or the error from a Settle that timed out.
//
// Note simnet does NOT import "testing": the harness stays a plain library so it can
// also back a CLI replay tool, and so a scenario is never coupled to a *testing.T's
// lifetime. Tests call Run and assert on the returned error and the recorded trace.
func (s *Scenario) Run() error {
	// Stable sort: steps registered at the same instant keep registration order,
	// which is the documented tiebreak and the thing that makes a script readable.
	sort.SliceStable(s.steps, func(i, j int) bool { return s.steps[i].at < s.steps[j].at })

	var cur time.Duration
	for _, st := range s.steps {
		if st.at > cur {
			// Advance settles between fires via the barrier wired in NewScenario.
			s.clk.Advance(st.at - cur)
			cur = st.at
		}
		if s.err != nil {
			return s.err
		}
		st.f()
		if err := s.Settle(); err != nil {
			s.Fail(err)
		}
		if s.err != nil {
			return s.err
		}
	}
	return s.err
}
