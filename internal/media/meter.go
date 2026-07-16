package media

import (
	"context"
	"log/slog"
	"math"
	"sync/atomic"
	"time"

	"github.com/pion/webrtc/v4"
	pmedia "github.com/pion/webrtc/v4/pkg/media"
)

// uploadSampleInterval is how often the meter reports the send rate. One second
// makes the logged number read directly as "kbit this second"; short enough to
// watch it climb as peers join, long enough that the log isn't a firehose.
const uploadSampleInterval = time.Second

// uploadMeter is the Phase 2 instrument: it counts every byte handed to an
// outbound track across all peer sessions and periodically logs the aggregate
// send rate. Its whole purpose is to make the mesh's O(N) upload cost *visible* —
// each remote peer gets its own independently-encoded copy of our video, so the
// rate this reports climbs roughly linearly with the number of peers. That linear
// climb is the entire motivation for the elected-SFU the later phases build.
//
// The counter is a single atomic int64, not a mutex-guarded field, and that choice
// is deliberate — the roadmap asks to name the "mutex vs channel vs atomic for
// shared state" trade-off:
//   - Every media pump (one goroutine per peer) calls addBytes on its hot path,
//     once per frame — dozens of times a second, times N peers. The sampler reads
//     the total once a second.
//   - There is no invariant spanning several fields here — it is one running total.
//     That is the textbook case for sync/atomic: a lock-free add is both simpler
//     and cheaper than taking a sync.Mutex on every frame.
//   - A channel would be the worst fit: it would funnel every frame's byte count
//     through one collector goroutine, manufacturing exactly the cross-goroutine
//     serialization we have no reason to want. Channels are for handing off
//     ownership or signaling; a monotonic counter is neither.
type uploadMeter struct {
	bytes atomic.Int64 // total bytes written to outbound tracks since start
	peers atomic.Int64 // live outbound media pumps (≈ N-1 in a full mesh)
}

// addBytes records that n bytes were written toward some peer. Called on the media
// pump hot path, so it must stay lock-free.
func (m *uploadMeter) addBytes(n int) { m.bytes.Add(int64(n)) }

// addPeer adjusts the live outbound-pump gauge (+1 when a pump starts, -1 when it
// exits), so the logged per-peer rate divides by the right N.
func (m *uploadMeter) addPeer(delta int) { m.peers.Add(int64(delta)) }

// livePeers reports the current outbound-pump count. Used by Router.Stats.
func (m *uploadMeter) livePeers() int { return int(m.peers.Load()) }

// run samples the byte counter every interval and logs the send rate until ctx is
// cancelled. It runs on its own goroutine (joined by the Router's WaitGroup), so
// cancelling ctx is what stops it.
func (m *uploadMeter) run(ctx context.Context, log *slog.Logger, interval time.Duration) {
	log = log.With(slog.String("component", "upload-meter"))
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var last int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			total := m.bytes.Load()
			delta := total - last
			last = total
			peers := m.peers.Load()
			if peers == 0 && delta == 0 {
				continue // idle: don't spam the log with zeros
			}
			// bytes/interval → bits/sec → kbit/sec.
			kbitPerSec := float64(delta*8) / interval.Seconds() / 1000
			perPeer := 0.0
			if peers > 0 {
				perPeer = kbitPerSec / float64(peers)
			}
			log.Info("upload",
				slog.Int64("peers", peers),
				slog.Float64("kbit_per_sec", round1(kbitPerSec)),
				slog.Float64("kbit_per_sec_per_peer", round1(perPeer)),
			)
		}
	}
}

// round1 trims a rate to one decimal so the logs stay readable.
func round1(f float64) float64 { return math.Round(f*10) / 10 }

// meteredTrack wraps a real outbound track and tallies every sample's byte count
// into the shared meter before forwarding it. It satisfies sampleWriter, so a
// media source writes to it exactly as it would to the raw track — the accounting
// is invisible to both sides.
type meteredTrack struct {
	track *webrtc.TrackLocalStaticSample
	meter *uploadMeter
}

func (t meteredTrack) WriteSample(s pmedia.Sample) error {
	t.meter.addBytes(len(s.Data))
	return t.track.WriteSample(s)
}
