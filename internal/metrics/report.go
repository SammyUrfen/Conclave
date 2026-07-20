package metrics

import (
	"context"
	"log/slog"
	"time"

	"github.com/SammyUrfen/conclave/internal/overlay"
)

// DefaultInterval is how often a peer re-reports its telemetry. A few seconds is
// the right order of magnitude: frequent enough that the coordinator learns a new
// peer's real capacity within one cycle of it joining, infrequent enough that the
// report stream is a trickle, not a load. Membership changes (join/leave) drive
// re-optimisation directly and immediately; this interval only refreshes the
// underlying numbers, so it need not be tight.
const DefaultInterval = 3 * time.Second

// Report is the telemetry one peer sends the coordinator — the payload of a
// TypeMetrics frame. It carries only what BuildTree consumes plus room to grow:
//
//   - UploadKbps and NAT are the load-bearing inputs today; together they decide
//     whether a node can be a relay and how many children it may serve.
//   - RTTServerMs/LossPct/CPUPct are declared now and populated later (Phase 5/7);
//     wiring the fields through the plane up front means adding a real sensor never
//     touches the protocol.
//
// It intentionally does NOT carry the peer's id: the server stamps the sender id
// on the frame, and trusting a self-reported id would let a peer file telemetry as
// someone else. The coordinator pairs this body with that server-known id.
type Report struct {
	// Name is the peer's stable label — how it appears in the computed tree.
	Name string `json:"name"`
	// UploadKbps is the upload budget this node offers for forwarding others'
	// media. Phase 4 declares it via a flag (an honest stand-in for the bandwidth
	// probe a real deployment would run); it is the primary tree-shaping input.
	UploadKbps int `json:"upload_kbps"`
	// NAT is the node's reachability class; NATRelayed (TURN-bound) forces it to a
	// leaf. Declared via a flag in Phase 4; real detection is Phase 7's TURN work.
	NAT overlay.NATType `json:"nat,omitempty"`
	// RTTServerMs is the node's round-trip to the server, a cheap proxy for its
	// network quality. Unpopulated in Phase 4 (BuildTree tolerates missing RTT);
	// Phase 7 measures it. Kept for forward compatibility of the wire format.
	RTTServerMs float64 `json:"rtt_server_ms,omitempty"`
	// LossPct and CPUPct are headroom signals Phase 5's hysteresis will act on.
	LossPct float64 `json:"loss_pct,omitempty"`
	CPUPct  float64 `json:"cpu_pct,omitempty"`
}

// Reporter is the producer side of the metrics plane: on its own goroutine it
// samples the local node's telemetry every interval and ships it to the
// coordinator. It is deliberately decoupled from both signaling and media — it
// holds a sample function (what to report) and a send function (how to ship it),
// injected by the peer's main, so this package imports neither. That is what lets
// it be unit-tested with a fake sink and no sockets.
type Reporter struct {
	log      *slog.Logger
	interval time.Duration
	sample   func() Report
	send     func(Report) error
}

// NewReporter builds a Reporter. sample is called each tick to snapshot the
// current telemetry (so live values can be folded in later without changing this
// type); send ships one report and may fail transiently — telemetry is
// best-effort and a dropped report must never fault the peer, so send errors are
// logged, not returned. A zero or negative interval falls back to DefaultInterval.
func NewReporter(log *slog.Logger, interval time.Duration, sample func() Report, send func(Report) error) *Reporter {
	if interval <= 0 {
		interval = DefaultInterval
	}
	return &Reporter{
		log:      log.With(slog.String("component", "metrics-reporter")),
		interval: interval,
		sample:   sample,
		send:     send,
	}
}

// Run reports once immediately — so the coordinator learns this node's real
// capacity right after it joins, rather than a cycle later — then every interval
// until ctx is cancelled. The immediate-then-ticker shape is the standard Go
// periodic-task idiom: a select over the ticker and ctx.Done, with the first
// emission hoisted out of the loop so there is no initial interval of silence.
func (r *Reporter) Run(ctx context.Context) {
	r.report()

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.report()
		}
	}
}

// report samples once and ships it, swallowing (but logging) a transient send
// failure — a lost telemetry frame is self-correcting on the next tick, and must
// never propagate as a peer-level error.
func (r *Reporter) report() {
	rep := r.sample()
	if err := r.send(rep); err != nil {
		r.log.Debug("metrics report dropped", slog.Any("error", err))
		return
	}
	r.log.Debug("reported telemetry",
		slog.String("name", rep.Name), slog.Int("upload_kbps", rep.UploadKbps), slog.String("nat", string(rep.NAT)))
}
