package media

import (
	"context"
	"testing"
	"time"
)

// TestSpawnTrackedIsRefusedOnceRunHasReturned pins the shutdown half of the
// Router's WaitGroup contract: r.wg is Added to from goroutines the Router does NOT
// own — pion's operations queue runs Session.onNegotiationNeeded, whose failure path
// calls s.spawn (== r.spawnTracked) — and pc.Close() does not join that queue. So a
// late spawn can arrive while Run's terminal r.wg.Wait() is already blocked, which
// is sync.WaitGroup misuse: an Add that takes the counter 0→1 concurrently with a
// Wait. The race detector catches it through WaitGroup's own &wg.sema annotation
// (Add's race.Read against Wait's race.Write), and sync itself may panic with
// "WaitGroup misuse: Add called concurrently with Wait".
//
// The assertion is deterministic without a sleep: r.wg.Wait() below JOINS anything
// spawnTracked accepted, so on the pre-fix code fn has certainly run by the time
// Wait returns and the channel is closed. On the fixed code the spawn is refused,
// Wait returns immediately, and the channel is still open.
//
// Two mutations it catches:
//   - Drop the spawnClosed check in spawnTracked: fn runs after Run returned.
//   - Drop stopSpawning from Run's teardown: the gate never closes, same failure.
func TestSpawnTrackedIsRefusedOnceRunHasReturned(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, _, _ := dialPair(t, ctx, "spawn-gate", "c", "d")
	r := NewRouter(discardLog(), client, RouterConfig{SelfName: "c", Managed: true})

	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- r.Run(runCtx) }()
	stop()
	<-done // Run has returned, so its r.wg.Wait() has already completed.

	ran := make(chan struct{})
	r.spawnTracked(func() { close(ran) })
	r.wg.Wait()
	select {
	case <-ran:
		t.Fatal("spawnTracked started a goroutine after Run returned: nothing will ever " +
			"join it, and the Add that tracked it races Run's terminal wg.Wait()")
	default:
	}
}
