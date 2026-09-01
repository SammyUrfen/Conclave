package metrics

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/SammyUrfen/conclave/internal/clock"
	"github.com/SammyUrfen/conclave/internal/overlay"
)

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// TestReporterEmitsImmediatelyThenTicks pins the periodic-task shape: a report goes
// out at once (so the coordinator learns this node without an interval of silence),
// then keeps ticking, and cancelling the context stops it.
func TestReporterEmitsImmediatelyThenTicks(t *testing.T) {
	var mu sync.Mutex
	var got []Report
	send := func(r Report) error {
		mu.Lock()
		got = append(got, r)
		mu.Unlock()
		return nil
	}
	sample := func() Report { return Report{Name: "n1", UploadKbps: 4000, NAT: overlay.NATDirect} }

	r := NewReporter(discardLogger(), 15*time.Millisecond, clock.System(), sample, send)
	ctx, cancel := context.WithCancel(context.Background())
	go r.Run(ctx)

	count := func() int { mu.Lock(); defer mu.Unlock(); return len(got) }
	waitFor(t, 200*time.Millisecond, "first (immediate) report", func() bool { return count() >= 1 })
	waitFor(t, 500*time.Millisecond, "several ticks", func() bool { return count() >= 3 })

	cancel()
	time.Sleep(40 * time.Millisecond)
	stable := count()
	time.Sleep(60 * time.Millisecond)
	if count() != stable {
		t.Errorf("reporter kept sending after cancel: %d then %d", stable, count())
	}

	mu.Lock()
	defer mu.Unlock()
	if got[0].Name != "n1" || got[0].UploadKbps != 4000 {
		t.Errorf("reported %+v, want the sampled telemetry", got[0])
	}
}

// TestReporterSurvivesSendError proves telemetry is best-effort: a send that always
// fails must not stop the reporter — a dropped report self-corrects next tick, and a
// flaky control channel can never fault the peer's media loop.
func TestReporterSurvivesSendError(t *testing.T) {
	var calls int
	var mu sync.Mutex
	send := func(Report) error {
		mu.Lock()
		calls++
		mu.Unlock()
		return errors.New("channel down")
	}
	r := NewReporter(discardLogger(), 10*time.Millisecond, clock.System(), func() Report { return Report{Name: "n"} }, send)
	ctx, cancel := context.WithCancel(context.Background())
	go r.Run(ctx)

	got := func() int { mu.Lock(); defer mu.Unlock(); return calls }
	waitFor(t, 500*time.Millisecond, "keeps retrying despite errors", func() bool { return got() >= 3 })
	cancel()
}

// TestReporterDefaultsInterval checks a non-positive interval falls back rather than
// spinning (a zero ticker panics).
func TestReporterDefaultsInterval(t *testing.T) {
	r := NewReporter(discardLogger(), 0, clock.System(), func() Report { return Report{} }, func(Report) error { return nil })
	if r.interval != DefaultInterval {
		t.Errorf("interval = %v, want default %v", r.interval, DefaultInterval)
	}
}

func waitFor(t *testing.T, d time.Duration, what string, pred func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if pred() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}
