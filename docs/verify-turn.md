# Verifying the TURN path and the measured NAT class

> **Status: proposed, not yet run.** Everything below is the command sequence believed
> to prove *"media flows over TURN when the direct path is blocked, and the peer says
> so"*. It has not been executed; when it is, its output belongs in `DESIGN.md` §9.5
> and this banner comes off. The automated suite proves the **classification logic**
> (`internal/media/linkstats_test.go`) and the **relay itself allocates**
> (`ALLOCATED relay address: 127.0.0.1:49176` from a manual `pion/turn` client against
> `bin/turn`); what is unproven is the two of them working together over a real ICE
> negotiation.

## What is being proved, and what is not

**The claim.** With the direct UDP path between two peers dropped and nothing else
blocked, ICE nominates a **relay** candidate pair, media still decodes at the far end,
and both peers' telemetry reports `nat: "turn"` — measured, not declared.

**Not claimed.** This is not NAT-type discovery. It does not simulate symmetric NAT,
CGNAT, or any particular mapping behaviour; it produces the *observable consequence* of
one — no direct path — and checks that conclave classifies it. That is the same thing
the sensor claims and no more (see `internal/media/linkstats.go`, `relayedPath`).

## The constraints this recipe is shaped by

Four facts about this machine drive every awkward line below.

1. **`tc` is not installed.** `nft` can DROP but cannot DELAY, so the fault injected is
   a hard blackhole of the direct path, not degradation. Fine here: ICE responds to
   "no path" and that is exactly the condition under test.
2. **pion gathers no loopback candidates.** A network namespace with only `lo` produces
   an ICE agent with nothing to offer, and the call fails for a reason that has nothing
   to do with TURN. `ip link add dummy0 type dummy` plus an address on it is what makes
   ICE find anything at all.
3. **`unshare -rn` gives root INSIDE the namespace** (user + network namespace), so no
   host root is needed for `ip` or `nft`, and no rule can escape onto the real machine.
4. **`cmd/peer -media-ports lo-hi` and `cmd/turn -relay-ports lo-hi` pin the UDP ports.**
   Without both, there is nothing for a firewall rule to name.

### The rule must be PAIR-SPECIFIC — this is the part that is easy to get wrong

The obvious rule — "drop everything from peer A's port range" — kills A's traffic to the
TURN server too. A then has no path at all, the call fails, and the run proves nothing
except that dropping all of a peer's UDP breaks it.

So the rule must match **both** `sport` and `dport`, describing a *pair* of endpoints
rather than an endpoint, and leave A↔TURN and TURN↔B untouched.

**But naming only the two peers' MEDIA ranges is not enough, and that omission voids the
run.** Blocking host↔host leaves `A_host ↔ B_relay` alive — a valid pair, and one that
ICE prefers over `A_relay ↔ B_relay`. RFC 8445 §5.1.2.1 gives a host candidate priority
`126·2²⁴ = 2 113 929 216` and a relay candidate `0·2²⁴ + 65535·2⁸ + 255 = 16 777 215`;
§6.1.2.3 makes a pair worth `2³²·MIN(G,D) + 2·MAX(G,D) + (G>D ? 1 : 0)`:

| Pair | MIN | MAX | priority |
|---|---|---|---|
| `A_host ↔ B_relay` | 16 777 215 (the relay end) | 2 113 929 216 | `2³²·1.678e7 + 4.23e9` |
| `A_relay ↔ B_relay` | 16 777 215 | 16 777 215 | `2³²·1.678e7 + 3.36e7` |

The `MIN` term ties — both pairs take their minimum from the *same* relay candidate — so
`2·MAX` decides, and host↔relay wins by ~4.2e9. (If the two relay candidates' local
preferences differ, relay↔relay's `MIN` can only be *lower*, never higher, so the
conclusion does not depend on the tie.) ICE would nominate host↔relay, only ONE end
would hold a relay-typed LOCAL candidate, and `nat: "turn"` for BOTH peers — the stated
expectation of Phase B, and the precondition of Phase C — would be unreachable.

**The mixed pairs must be dropped too.** Dropping `sport <peer range> → dport <relay
range>` is safe in both directions, and safe for exactly one reason: **a peer never sends
to a relay PORT when using its own allocation.** That traffic goes to the server's
`:3478` as a Send indication or ChannelData, and the server delivers relayed data back to
its client FROM `:3478`. The relay ports appear as a *source* on the wire (the server
emitting an allocation's traffic outward) and as a *destination* only when someone is
addressing the far end's allocation directly — which is precisely the pair being removed.

Every flow the run produces, with nothing omitted:

| # | Flow | sport | dport | Matched? |
|---|---|---|---|---|
| 1 | A → B direct (`A_host ↔ B_host`) | 47000-47019 | 47100-47119 | **dropped** |
| 2 | B → A direct | 47100-47119 | 47000-47019 | **dropped** |
| 3 | A → B's allocation (`A_host ↔ B_relay`) | 47000-47019 | 49160-49200 | **dropped** |
| 4 | B → A's allocation (`B_host ↔ A_relay`) | 47100-47119 | 49160-49200 | **dropped** |
| 5 | A → TURN (its own allocation) | 47000-47019 | 3478 | passes |
| 6 | B → TURN | 47100-47119 | 3478 | passes |
| 7 | TURN → A (relayed data to its client) | 3478 | 47000-47019 | passes |
| 8 | TURN → B | 3478 | 47100-47119 | passes |
| 9 | A's allocation → B's allocation — **the path under test** | 49160-49200 | 49160-49200 | passes |
| 10 | A's allocation → B host (`A_relay ↔ B_host`) | 49160-49200 | 47100-47119 | passes out… |

Flow 10 is the one that looks like a hole and is not. Rows 3 and 4 are the *return legs*
of the two mixed pairs: A's check reaches B over flow 10, B's response is flow 4 and is
dropped, so the pair never succeeds from either end. Killing the return leg is enough,
and it is what keeps the rule set to four lines instead of six.

## Address and port plan

| | address | ports |
|---|---|---|
| arbiter | `10.99.0.1:9000` | TCP |
| TURN relay | `10.99.0.1:3478` | relay allocations `49160-49200` |
| peer `alpha` (sender) | `10.99.0.1` | media `47000-47019` |
| peer `bravo` (receiver) | `10.99.0.1` | media `47100-47119` |

## The sequence

```bash
make build     # ./bin/{server,peer,turn}

# ONE namespace holds everything: no host root, and no rule can leak onto the machine.
unshare -rn bash
```

Everything from here runs **inside** that shell.

```bash
# --- 1. an interface ICE will actually gather from --------------------------
ip link set lo up
ip link add dummy0 type dummy
ip addr add 10.99.0.1/24 dev dummy0
ip link set dummy0 up
ip addr show dummy0          # expect: inet 10.99.0.1/24

# --- 2. the control plane ---------------------------------------------------
./bin/server -addr 10.99.0.1:9000 -coordinate -elect -log-format json > /tmp/arbiter.log 2>&1 &
MEET=$(curl -sS -X POST http://10.99.0.1:9000/api/meets | python3 -c 'import sys,json; print(json.load(sys.stdin)["id"])')
echo "meet=$MEET"

# --- 3. the relay -----------------------------------------------------------
./bin/turn -addr 10.99.0.1:3478 -public-ip 10.99.0.1 \
           -users conclave=hunter2 -relay-ports 49160-49200 \
           -log-format json > /tmp/turn.log 2>&1 &
# expect in /tmp/turn.log: "turn relay listening" ... relay_ports=49160-49200
```

### Phase A — the control run: TURN configured, direct path open

Both peers are given TURN and both *gather* a relay candidate. The claim is that they
do not USE it, and are therefore classified `direct` — the "an unused gathered relay
candidate does not classify the peer" case, in the wild.

```bash
TURNARGS="-turn turn:10.99.0.1:3478 -turn-user conclave -turn-pass hunter2"

./bin/peer -call -managed -server http://10.99.0.1:9000 -room $MEET -name alpha \
  $TURNARGS -media-ports 47000-47019 -send -media testdata/sample.ivf \
  -log-format json > /tmp/alpha.log 2>&1 &
./bin/peer -call -managed -server http://10.99.0.1:9000 -room $MEET -name bravo \
  $TURNARGS -media-ports 47100-47119 -record /tmp/phaseA.ivf \
  -log-format json > /tmp/bravo.log 2>&1 &

sleep 20
curl -sS http://10.99.0.1:9000/api/meets/$MEET | python3 -m json.tool | grep -E '"name"|"nat"'
# EXPECT: nat "direct" for both peers.

kill %3 %4; sleep 2
ffprobe -v error -count_frames -show_entries stream=nb_read_frames /tmp/phaseA.ivf
# EXPECT: a few hundred decodable VP8 frames — the direct-path baseline.
```

### Phase B — the treatment: the direct pair is dropped, TURN is not

The rule goes in **before** the peers start. ICE nominates once and does not
re-nominate on its own, so a peer that has already settled on a direct pair keeps
reporting `direct` even after the path dies — that is a real property of the sensor, and
it is stated in the limitations below rather than hidden by test ordering.

```bash
nft -f - <<'EOF'
table inet conclave {
  chain out {
    type filter hook output priority 0; policy accept;
    # A -> B direct: both ends named, so A -> TURN (dport 3478) and
    # TURN-relay -> B (sport 49160-49200) are untouched.
    udp sport 47000-47019 udp dport 47100-47119 counter drop
    # B -> A direct.
    udp sport 47100-47119 udp dport 47000-47019 counter drop
    # A -> B's relay allocation, and B -> A's. Without these two ICE nominates
    # host<->relay, which OUTRANKS relay<->relay (see the priority table above), and
    # only one end ends up relay-typed. Safe because a peer addresses its OWN
    # allocation at :3478, never at a relay port.
    udp sport 47000-47019 udp dport 49160-49200 counter drop
    udp sport 47100-47119 udp dport 49160-49200 counter drop
  }
}
EOF
nft list table inet conclave      # confirm all four rules, counters at 0

./bin/peer -call -managed -server http://10.99.0.1:9000 -room $MEET -name alpha \
  $TURNARGS -media-ports 47000-47019 -send -media testdata/sample.ivf \
  -log-format json > /tmp/alphaB.log 2>&1 &
./bin/peer -call -managed -server http://10.99.0.1:9000 -room $MEET -name bravo \
  $TURNARGS -media-ports 47100-47119 -record /tmp/phaseB.ivf \
  -log-format json > /tmp/bravoB.log 2>&1 &

sleep 25
```

**The four pieces of evidence, in order of how much they prove:**

```bash
# 1. The rule actually fired. A zero counter means the direct path was never even
#    attempted, and the whole run is void — it would prove TURN works when nothing
#    was blocked.
nft list table inet conclave | grep counter
# EXPECT: non-zero packets on the two host<->host rules AND on at least one of the
# host<->relay rules. A zero on rows 3/4 means ICE never tried the pair that would
# have out-prioritised the relayed one, which is worth knowing but is not a failure.

# 2. The classification flipped. This is the feature.
curl -sS http://10.99.0.1:9000/api/meets/$MEET | python3 -m json.tool | grep -E '"name"|"nat"'
# EXPECT: nat "turn" for both peers, where Phase A said "direct".

# 3. The relay carried it, rather than the peers finding some other path.
grep -c allocation /tmp/turn.log    # EXPECT: allocations created
ss -unap | grep -E ':(4916[0-9]|491[7-9][0-9]|4920[0-9])'   # EXPECT: bound relay ports

# 4. Media survived the detour. Same oracle as every other phase.
kill %3 %4; sleep 2
ffprobe -v error -count_frames -show_entries stream=nb_read_frames /tmp/phaseB.ivf
# EXPECT: comparable to Phase A. A large drop is a finding, not a pass.
```

### Phase C — the consequence, if a third peer is added

`nat: "turn"` is not decoration: `overlay.BuildTree` forces such a node to a **leaf**
and `arbiter.Fitness` disqualifies it as coordinator. With a third, unblocked peer in
the meet, the tree should place the two TURN-bound peers as leaves under it, and the
coordinator role should never land on either.

```bash
./bin/peer -call -managed -server http://10.99.0.1:9000 -room $MEET -name charlie \
  -media-ports 47200-47219 -upload-kbps 9000 -log-format json > /tmp/charlie.log 2>&1 &
sleep 20
curl -sS http://10.99.0.1:9000/api/meets/$MEET | python3 -m json.tool
# EXPECT: alpha and bravo have no children; the coordinator is charlie or the arbiter.
```

### Teardown

```bash
nft delete table inet conclave
kill %1 %2 %3 %4 %5 2>/dev/null
exit           # leaves the namespace; dummy0 and every rule vanish with it
```

## What this run would still not prove

- **Nothing here is a real NAT.** The direct path is dropped by a firewall on one host,
  not translated by a middlebox. The *consequence* is identical from ICE's point of
  view, which is why the classification is behavioural and named that way — but no
  claim about symmetric-NAT behaviour follows from it.
- **Still single-host** (`DESIGN.md` §8.7). The namespace is isolation, not distance:
  no real RTT, no real loss, no congestion control under contention.
- **The class does not flip back without a renegotiation.** A peer that nominated a
  direct pair and then lost it keeps reporting `direct` until ICE re-nominates. Phase B
  installs the rule before the peers start for exactly this reason. Whether a
  disconnected-then-restarted ICE session re-classifies is not covered by this recipe
  and is not claimed.
- **`UploadKbps` remains declared.** Phase 7 closes **one** of the two declared fields.
  A real bandwidth probe means saturating the uplink to measure it, in a system whose
  entire thesis is that the uplink is the scarce resource; it was not attempted.
