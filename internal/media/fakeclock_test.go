package media

import (
	"sync"
	"testing"
	"time"

	"github.com/SammyUrfen/conclave/internal/clock"
)

// fakeClock is a hand-driven clock.Clock for the deterministic media tests. It
// exists because the media package must not sleep in a unit test: every retry and
// every re-parent deadline is armed off the injected clock, so a test drives them
// explicitly with advance() instead of waiting out real milliseconds.
//
// It is deliberately much simpler than simnet's virtual clock (which media may not
// import): no ordering discriminator, no event queue — just "which timers are due".
type fakeClock struct {
	mu      sync.Mutex
	now     time.Time
	timers  []*fakeTimer
	created int // total timers ever created, including retired ones
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Unix(1700000000, 0)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) NewTimer(d time.Duration) clock.Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeTimer{c: make(chan time.Time, 1), due: c.now.Add(d), live: true}
	c.timers = append(c.timers, t)
	c.created++
	return t
}

func (c *fakeClock) NewTicker(time.Duration) clock.Ticker {
	panic("fakeClock.NewTicker: the media layer arms no tickers")
}

func (c *fakeClock) After(d time.Duration) <-chan time.Time { return c.NewTimer(d).C() }

// advance moves the clock forward and fires every timer that becomes due.
func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	now := c.now
	due := make([]*fakeTimer, 0, len(c.timers))
	kept := c.timers[:0]
	for _, t := range c.timers {
		if !t.due.After(now) {
			due = append(due, t)
			continue
		}
		kept = append(kept, t)
	}
	c.timers = kept
	c.mu.Unlock()
	for _, t := range due {
		t.fire(now)
	}
}

// createdCount reports how many timers have ever been armed off this clock.
func (c *fakeClock) createdCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.created
}

// waitCreated blocks (bounded) until at least n timers have been armed. Timers are
// armed from pion's operations goroutine, so a test cannot assume they exist the
// instant it returns from AddTrack.
func (c *fakeClock) waitCreated(t *testing.T, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if c.createdCount() >= n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("only %d timers armed, want at least %d", c.createdCount(), n)
}

type fakeTimer struct {
	mu   sync.Mutex
	c    chan time.Time
	due  time.Time
	live bool
}

func (t *fakeTimer) C() <-chan time.Time { return t.c }

func (t *fakeTimer) Stop() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	was := t.live
	t.live = false
	return was
}

func (t *fakeTimer) Reset(time.Duration) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	was := t.live
	t.live = true
	return was
}

func (t *fakeTimer) fire(now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.live {
		return
	}
	t.live = false
	select {
	case t.c <- now:
	default:
	}
}
