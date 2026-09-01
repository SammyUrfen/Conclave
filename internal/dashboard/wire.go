package dashboard

// The JSON bodies of §9. They are UNEXPORTED: the contract this package publishes is
// the wire schema, not a set of Go types, and exporting them would invite a consumer
// to depend on the Go shape of something whose only real definition is §9.
//
// Every slice field is built with make(..., 0, n) rather than left nil, because
// encoding/json writes a nil slice as `null` and the frontend does Array.isArray /
// .map on these without a null guard. An empty collection is `[]`.
//
// There are no maps here, and that is structural rather than incidental: Go randomises
// map iteration, the frontend replays these bodies, and a map would make two renderings
// of one history differ. The one exception is errorDetail.Details, where encoding/json
// sorts the keys and the object is a bag of diagnostics rather than an ordered
// collection.

// listBody is GET /api/meets (§9.3). It is the REALIZED view — what peers actually
// did, reconstructed from heartbeats — and says so on every row.
type listBody struct {
	APIVersion int `json:"api_version"`
	// DemoEnabled tells the frontend whether the gated §9.6 surface exists, WITHOUT
	// it having to POST a destructive verb speculatively to find out. §9 defines no
	// capability signal; this is the addition, and it rides this endpoint because
	// GET /api/meets is the one call the dashboard already makes on connect, so the
	// answer arrives with the first useful response and costs no extra round trip
	// and no extra route.
	DemoEnabled bool           `json:"demo_enabled"`
	Meets       []listRow      `json:"meets"`
	Ended       []endedListRow `json:"ended"`
}

// listRow is one live meet. Rev is deliberately ABSENT (§9.4b): rev is a coordinator
// counter, not an observable, and a heartbeat-derived rev would be a fabricated number
// wearing an authoritative name that the fence depends on meaning one thing.
type listRow struct {
	ID              string   `json:"id"`
	CreatedAtUnixMs int64    `json:"created_at_unix_ms"`
	Members         int      `json:"members"`
	Epoch           uint64   `json:"epoch"`
	Coordinator     string   `json:"coordinator"`
	CoordinatorID   string   `json:"coordinator_id"`
	ArbiterIsCoord  bool     `json:"arbiter_is_coordinator"`
	Relays          []string `json:"relays"`
	Depth           int      `json:"depth"`
	// Provenance is the literal "realized" on every list row, so a client cannot
	// mistake these numbers for the coordinator's intent (§9.4b).
	Provenance string `json:"provenance"`
}

// endedListRow is one tombstone (§6.9). It carries counts, never telemetry.
type endedListRow struct {
	ID              string `json:"id"`
	CreatedAtUnixMs int64  `json:"created_at_unix_ms"`
	EndedAtUnixMs   int64  `json:"ended_at_unix_ms"`
	PeakMembers     int    `json:"peak_members"`
	FinalEpoch      uint64 `json:"final_epoch"`
	Elections       int    `json:"elections"`
}

// createBody is the 201 answer to POST /api/meets.
type createBody struct {
	APIVersion      int      `json:"api_version"`
	ID              string   `json:"id"`
	CreatedAtUnixMs int64    `json:"created_at_unix_ms"`
	Join            joinInfo `json:"join"`
}

// joinInfo is the RENDEZVOUS: the browser hands a participant the command or link that
// puts them in the meet's subnetwork. The server builds it from its own -addr and
// -public-url so it is correct for a hosted deployment without the frontend guessing.
type joinInfo struct {
	WSURL       string `json:"ws_url"`
	PeerCommand string `json:"peer_command"`
}

// meetBody is GET /api/meets/{id} and the WS `snapshot` frame's data — the same object
// in both places, so a client needs one render path rather than two that differ only by
// a race.
//
// This is the INTENDED view: Root/Edges/Backups/Rev come from the coordinator's last
// published tree. Each node's Parent/Children are REALIZED (from its last heartbeat),
// which is what makes Convergence/Diverged computable at all.
type meetBody struct {
	APIVersion     int          `json:"api_version"`
	ID             string       `json:"id"`
	Epoch          uint64       `json:"epoch"`
	Rev            uint64       `json:"rev"`
	Coordinator    string       `json:"coordinator"`
	CoordinatorID  string       `json:"coordinator_id"`
	ArbiterIsCoord bool         `json:"arbiter_is_coordinator"`
	Root           string       `json:"root"`
	AtUnixMs       int64        `json:"at_unix_ms"`
	Nodes          []nodeBody   `json:"nodes"`
	Edges          []edgeBody   `json:"edges"`
	Backups        []backupBody `json:"backups"`
	// StaleRejected is the visible proof of the fence: coordinator.RoomSnapshot's
	// meet-wide total of the refusals its members have reported.
	//
	// It is READ FROM THE SNAPSHOT, never accumulated here. The number can legitimately
	// DECREASE — a peer's fence resets on TypeJoined, so its heartbeat counter resets
	// on rejoin exactly like Seq — and an in-process accumulator could not represent
	// that: it would pin the display at a high-water mark belonging to a fence that no
	// longer exists.
	StaleRejected uint64 `json:"stale_rejected"`
	// Convergence and Diverged expose the gap between intended and realized (§9.4b).
	// A non-empty Diverged means convergence lag, a failed apply, or a fenced-out
	// peer — the three faults hardest to see any other way, and each of them
	// invisible if the UI simply renders whichever number it fetched last.
	//
	// Convergence is a THREE-VALUE ENUM and not the boolean it used to be, because the
	// comparison has three outcomes and one of them is "there was nothing to compare".
	// With no published tree the old boolean reported true — len(Diverged) == 0 fell
	// out of the formula — and "converged" beside an empty subnet reads as HEALTHY, so
	// a meet that had failed to build showed a green signal. Naming the third state is
	// the same correction as renaming `fitness` to `fitness_lower_bound`: when a value
	// cannot be computed, say that, rather than emitting whatever the formula produces.
	//
	// Rejected: a NULLABLE boolean. JSON null is falsy in JavaScript, so every naive
	// `if (converged)` and `!converged` would silently render "nothing to compare" as
	// DIVERGED — the same confusion, moved. Rejected: keeping `converged` and adding a
	// second `comparable` flag. Two booleans encode four states for three meanings, and
	// the constructible-but-illegal combination is precisely the misleading one. A
	// string enum has neither problem, and §9.4a already tells a UI how to survive an
	// enum value it does not know: render it verbatim in a neutral style.
	Convergence string   `json:"convergence"`
	Diverged    []string `json:"diverged"`
	// Provenance is the literal "intended" (§9.4b).
	Provenance string `json:"provenance"`
}

// nodeBody is one member. Units are frozen by §9.4a: *_kbps is kbit/s integer, *_ms is
// float milliseconds, LossPct/CPUPct are 0-100 to one decimal, FitnessLowerBound is
// 0-1 to three, and Depth is hops from the root with -1 meaning "not in the tree"
// (which the UI must render as "unattached", never as a number).
type nodeBody struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Roles       []string `json:"roles"`
	Health      string   `json:"health"`
	Parent      string   `json:"parent"`
	Backup      string   `json:"backup"`
	Children    []string `json:"children"`
	Depth       int      `json:"depth"`
	UploadKbps  int      `json:"upload_kbps"`
	NAT         string   `json:"nat"`
	RTTServerMs float64  `json:"rtt_server_ms"`
	LossPct     float64  `json:"loss_pct"`
	CPUPct      float64  `json:"cpu_pct"`
	// FitnessLowerBound is named for what it IS (§15.14), not for what a reader would
	// like it to be. arbiter.Fitness.UptimeSec is unreachable from a
	// coordinator.MemberSnapshot — there is no join time on it — so the uptime term is
	// 0 and this figure is short of the arbiter's own score by up to the 0.15 uptime
	// weight. The key carries the caveat because a number quietly 0.15 too low is
	// INVISIBLY wrong; a footnote in a spec does not travel with the value onto a
	// screen. The UI must label it ">=" and must not present it as the arbiter's score.
	FitnessLowerBound float64 `json:"fitness_lower_bound"`
	LastBeatSeq       uint64  `json:"last_beat_seq"`
	LastBeatUnixMs    int64   `json:"last_beat_unix_ms"`
}

// edgeBody is one intended parent->child link. The slice preserves Topology.Edges'
// TOPOLOGICAL order (§8.1) and must never be sorted: that order is the invariant a
// rebuild depends on.
type edgeBody struct {
	Parent string `json:"parent"`
	Child  string `json:"child"`
}

// backupBody is one precomputed secondary parent, drawn as a dashed edge.
type backupBody struct {
	Node   string `json:"node"`
	Parent string `json:"parent"`
}

// demoAckBody is the 202 answer to a demo action.
type demoAckBody struct {
	APIVersion int    `json:"api_version"`
	OK         bool   `json:"ok"`
	Action     string `json:"action"`
	Target     string `json:"target"`
}

// envelope is the §9.4 stream frame. Seq, Epoch and Rev are uint64 and are marshalled
// as BARE JSON NUMBERS: web/js/format.js quotes those digits with a regex BEFORE
// JSON.parse runs and revives them as BigInt, so emitting them as strings — or in
// exponent form — would silently break the only path that survives values past 2^53.
type envelope struct {
	APIVersion int    `json:"api_version"`
	Seq        uint64 `json:"seq"`
	AtUnixMs   int64  `json:"at_unix_ms"`
	MeetID     string `json:"meet_id"`
	Epoch      uint64 `json:"epoch"`
	Rev        uint64 `json:"rev"`
	Kind       string `json:"kind"`
	// Data is never null: §9.4 says every frame has a data object, and the client
	// destructures it without a guard.
	Data any `json:"data"`
}

// The frozen §9.4 kind enum.
const (
	kindSnapshot       = "snapshot"
	kindMemberJoined   = "member_joined"
	kindMemberLeft     = "member_left"
	kindHealthChanged  = "health_changed"
	kindTopology       = "topology"
	kindReparent       = "reparent"
	kindFailover       = "failover"
	kindElection       = "election"
	kindAnnounceRepair = "announce_repair"
	kindStaleRejected  = "stale_rejected"
	kindSettling       = "settling"
	kindUnbuildable    = "unbuildable"
	kindDemo           = "demo"
	kindPong           = "pong"
)

// The per-kind `data` shapes (§9.4).

type memberData struct {
	// ID is the SERVER-STAMPED peer id (coordinator.Event.NodeID), never the
	// peer-supplied name. It is what lets a membership delta be joined against
	// nodes[] on the same authoritative key the snapshot uses; keying on Name would
	// key on the one field a peer controls.
	ID   string `json:"id"`
	Name string `json:"name"`
}

type healthData struct {
	Name string `json:"name"`
	// Health and PrevHealth are the transition, not a sample, and BOTH come straight
	// from the coordinator — the only party that performed the transition. The
	// dashboard deliberately remembers nothing here: in-process memory is wrong across
	// a restart and wrong for a fresh subscriber, whose very first frame would report
	// "" for a node whose history it simply had not witnessed.
	//
	// PrevHealth == Health is a VALID and expected shape, not a bug to normalise away:
	// sustained degradation and its recovery ride this event with the two equal,
	// because the node's LIVENESS did not change — a fired dwell is a quality verdict.
	// That equality is how a consumer separates the two cases without parsing Reason,
	// which the coordinator promises never to make parseable, so it is passed through
	// untouched.
	Health     string `json:"health"`
	PrevHealth string `json:"prev_health"`
}

type topologyData struct {
	Root    string       `json:"root"`
	Edges   []edgeBody   `json:"edges"`
	Backups []backupBody `json:"backups"`
	Depth   int          `json:"depth"`
	Relays  []string     `json:"relays"`
	Outcome string       `json:"outcome"`
	Reason  string       `json:"reason"`
}

// reparentData carries a peer's self-promotion to its precomputed backup.
//
// There is deliberately no `self_promoted` discriminator (REMOVED in §15.14).
// coordinator.EventReparent is emitted ONLY from the self-promotion path — a move the
// coordinator decided arrives as `failover` plus `topology` — so the field could only
// ever hold one value, and a field that can only hold one value states something false
// about the type: that the other value is reachable. If a coordinator-decided reparent
// ever needs its own event, the answer is a new KIND, not a flag.
type reparentData struct {
	Name   string `json:"name"`
	From   string `json:"from"`
	To     string `json:"to"`
	Reason string `json:"reason"`
}

type failoverData struct {
	Name    string   `json:"name"`
	Orphans []string `json:"orphans"`
	Reroot  bool     `json:"reroot"`
	Reason  string   `json:"reason"`
}

type electionData struct {
	Epoch uint64 `json:"epoch"`
	// Coordinator is "" when Reason is "vacated" (§6.8): the epoch still advances,
	// because the epoch is a fencing token and the thing being fenced is the OLD
	// coordinator.
	Coordinator string `json:"coordinator"`
	Prev        string `json:"prev"`
	Reason      string `json:"reason"`
}

type repairData struct {
	Name      string `json:"name"`
	PeerEpoch uint64 `json:"peer_epoch"`
	MeetEpoch uint64 `json:"meet_epoch"`
	Resolved  bool   `json:"resolved"`
}

// staleData explains one fence refusal.
//
// Epoch and Rev are the PEER's fence, NOT the meet's — coordinator.publish exempts this
// kind from its usual meet-stamping precisely so a peer that has adopted nothing shows
// as epoch 0, which is the most diagnostic case there is. They are the numbers that
// explain the refusal, and they belong here rather than on the envelope, whose epoch/rev
// describe the meet.
type staleData struct {
	Name  string `json:"name"`
	Epoch uint64 `json:"epoch"`
	Rev   uint64 `json:"rev"`
	// Total is that peer's cumulative refusals THIS SESSION (Event.Count). The event
	// fires on an INCREASE, never per heartbeat, so consecutive frames report a
	// climbing total rather than a stream of identical ones.
	Total  uint64 `json:"total"`
	Reason string `json:"reason"`
}

type settlingData struct {
	Waiting []string `json:"waiting"`
	Reason  string   `json:"reason"`
}

type unbuildableData struct {
	Reason string `json:"reason"`
}

type demoData struct {
	Action string `json:"action"`
	Target string `json:"target"`
	// ByRemoteAddr is ADVISORY ONLY. There is no auth here, so this is just the
	// requester's RemoteAddr as the server saw it: trivially spoofable behind a
	// proxy, and present so a demo operator can tell their own clicks from a
	// colleague's on a shared screen. It is NOT an identity and the UI must not
	// present it as one (§9.4a).
	ByRemoteAddr string `json:"by_remote_addr"`
}

// emptyData is the `data` of a frame that has none (pong). It marshals to {}.
type emptyData struct{}
