# Testing

> Living doc — kept in sync with the code. Reflects the tree at **Phase 6 complete**.
> The strategy narrative also lives in [`DESIGN.md`](./DESIGN.md) §6; this file is the
> operational version — what to run, how the harness works, and what the coverage
> honestly does and does not prove.

**The shape of the problem.** Two thirds of the Go in this repository is test code:
**22,751 test lines against 17,505 non-test lines**, 372 test functions and 297 `t.Run`
subtests across 69 files. That ratio is not diligence for its own sake. A system whose
defining behaviours are *temporal* (hysteresis, dwells, timeouts), *concurrent* (one
control loop, many peer goroutines, pion's own dispatch goroutines) and *distributed* (a
fence, a handover, an ambiguity window) has almost no behaviour a straightforward unit
test can reach. Four techniques do the reaching, and the rest of this document is those
four.

| Technique | What it makes testable | Where |
|---|---|---|
| **The injected clock** (`internal/clock` + `simnet.VirtualClock`) | Every dwell, timeout and cadence, in microseconds of virtual time | all four control-plane packages |
| **Independent oracles** (`overlay.Validate`, `overlay.ValidateLocalRepair`) | "Is this tree legal?" and "was this change minimal?" without re-deriving the expected tree | asserted from `overlay`, `coordinator` and `simnet` |
| **Deterministic simulation** (`internal/simnet`) | Whole churn and handover scenarios as a pure function of a seed and an event *sequence* | `simnet`, and `coordinator`'s external tests |
| **Mutation discipline** | Tests that are green *for the right reason* | a convention, recorded in comments |

---

## Running the tests

| Command | What it does |
|---|---|
| `make check` | `fmt` + `vet` + `check-determinism` + `go test -race ./...`. **This is the gate — the project's definition of green.** |
| `make test` | `go test -race ./...` |
| `make check-determinism` | Fails the build if a control-plane package reaches for the wall clock. Silent on success. |
| `make cover` | Coverage profile + a per-function report |
| `go test ./internal/overlay` | One package, fast inner loop |
| `go test -run 'TestValidateCatches/depth' ./internal/overlay` | One subtest (`/` separates test from subtest; spaces become `_`) |
| `go test -v ./internal/simnet` | Names every test and subtest as it runs |

CI (`.github/workflows/ci.yml`) runs **the same four commands, spelled out** rather than
delegated to `make check`, so a red job names which one failed in its own step title — and
so that if CI and `make check` ever disagree about what green means, the definition of done
has quietly moved. It adds `-count=1` to defeat the test cache and a final `make build`.

### Why the gate specifies `-race`

The control planes are single-goroutine *by design*, and the whole safety argument rests on
that being true — so a data race in this codebase is a **design violation**, not merely a bug.
That is why `-race` is in the gate. It needs a C toolchain (it links ThreadSanitizer through
cgo); see [`setup.md`](./setup.md).

**The gate *specifies* `-race`; the suite does not *need* it to pass.** Both invocations are
fully green, 13/13 packages each:

```
go test -count=1 ./...          13 ok   media 16.2s
go test -race -count=1 ./...    13 ok   media 81.9s
```

That distinction is worth keeping straight, because for a while it was not true — and the
reason it was not true is the single most useful testing lesson this project produced.

> ### `-race` is not a neutral observer
>
> A pion library-behaviour pin used to **fail without `-race` and pass with it** — 5 failures
> out of 5 plain, green every time instrumented. The tempting responses are both wrong:
> widen the window until the flake stops, or declare the package race-only.
>
> Re-measured properly, the determining variable was **not the invocation**. It was whether
> the `PeerConnection` had reached `connected`. Removed while `connected`, pion fires
> negotiation-needed the same millisecond; removed while `connecting`, it fires the instant
> the pc connects, or never if it does not. Nobody had been varying that.
>
> `-race` is a **~5–6× time dilation** on this package (16 s → 82 s here; ~14 s → ~88 s on the
> reference run). Under it, DTLS on loopback takes *seconds*, so every quiescence window
> expired while the pc was still `connecting` — and **`-race` had been acting as an
> unrecognised proxy for a state variable**, one that is the *inverse* of production, where a
> relay removes a departed source's track from sessions that have carried media for minutes.
>
> **The lesson, in the form to carry into the next test you write:**
>
> > **A test that disagrees across invocations is not flaky. It is reporting that the
> > invocation is an input** — and an input you did not choose is a variable you are not
> > controlling.
>
> Two corollaries:
>
> - **When a test behaves oddly, the first question is "what is it actually measuring?", not "how do I make it green."** Widening the window here would have preserved a false belief *and* kept the correct code — the worst of both, because nothing fails and the next person deletes the machinery on the strength of a comment that was never true.
> - **Any test whose subject is a race between your goroutine and a library's dispatch goroutine is measuring a different system under `-race`.** If a timing window is load-bearing, make the *state* explicit and wait for it, rather than letting a duration stand in for it.
>
> The fix was to pin the state, not the timing: the replacement test waits for `connected`
> **first**, which is exactly what makes it invocation-independent. `DESIGN.md` §7.2 tells
> the whole story, including what it meant for the machinery built on the false claim.

This is also, incidentally, the class of test the determinism rules exist to keep *out* of the
control plane: `internal/media` is deliberately outside `check-determinism` because it runs
real pion. A control-plane package with a timing window would have the same failure mode with
none of the visibility.

### The wall-clock profile is the argument for the architecture

Under `-race` (the reference run; this machine measures within a few seconds of it):

```
media 81.9s     ← real pion, real ICE, real DTLS on loopback
cmd/peer 7.3s   arbiter 2.2s   simnet 1.9s   dashboard 1.7s
coordinator 1.4s  signaling 1.2s  overlay 1.2s  metrics 1.2s
clock 1.1s   cmd/server 1.1s   policy 1.0s   logging 1.0s
```

Without `-race`, everything deterministic collapses to hundredths of a second and `media`
still takes **16.2 s** — 96 % of the whole suite:

```
media 16.174s   cmd/peer 0.726s   dashboard 0.270s   arbiter 0.242s
signaling 0.171s  metrics 0.165s  clock 0.125s  overlay 0.091s
coordinator 0.088s  simnet 0.310s  policy 0.008s  cmd/server 0.012s  logging 0.004s
```

**That gap is the entire reason the control plane was kept pion-free.** Every deterministic
package runs on a virtual clock and is essentially free; the one package that must touch a
real network dominates either way. It is also why the ~5× dilation `-race` adds to `media`
matters so much more there than anywhere else — see the box above.

---

## 1. The clock seam and the virtual clock

### `internal/clock` — the seam

`clock.Clock` is `Now()`, `NewTimer(d) Timer`, `NewTicker(d) Ticker`, `After(d)`. Every
component that measures or waits — `coordinator`, `arbiter`, `signaling`, `metrics`,
`media`, `dashboard` — takes one on its config struct and defaults to `clock.System()` when
nil. **Treat it like `*slog.Logger`: an ambient capability injected at construction, not a
service seam.**

Four alternatives were rejected, and they are the common ones:

| Rejected | Why |
|---|---|
| A package-level `var now = time.Now` swapped in tests | A global. Two parallel tests swapping it race, and `-race` will not necessarily catch it. |
| Carrying the clock on `context.Context` | `context.Value` for a *dependency* is a documented Go anti-pattern — untyped, invisible to the compiler, and it makes every function that might need time take a ctx it otherwise would not. |
| Passing a clock to every call site | Churns dozens of signatures for a value that is fixed for the object's lifetime. |
| A consumer-defined clock interface per package | Breaks on `NewTimer`'s **return type**: Go compares *named* types in return position, so `coordinator.Timer` does not satisfy an interface demanding `simnet.Timer` even if the two declarations are character-identical. One leaf package solves it once. (`Timer`/`Ticker` expose their channel through a `C()` **method** rather than `time.Timer`'s `C` **field**, because an interface can declare methods but not fields — which is why "just use `time.Timer`" is not available.) |

### `simnet.VirtualClock` — replayable, not merely fake

`NewVirtualClock(start)` satisfies `clock.Clock`; time moves only when `Advance`/`AdvanceTo`
is called. `DefaultStart` is a literal date, never `time.Now()`. Three rules make it
*replayable*:

- **Total order `(deadline, seq)`.** A monotonic sequence number is stamped at `NewTimer`/`NewTicker` and **re-stamped on every `Reset` and every ticker re-arm**, so two timers due at the same virtual instant fire in most-recently-armed order — never in map order, which Go randomises. (There is deliberately no heap: the armed set is a map, and ranging it is legal *only* because the range selects a unique minimum under a total order rather than building an order out of the iteration.)
- **One deadline per iteration.** `AdvanceTo` picks the single earliest due timer, sets `now` to *that deadline* rather than the target, fires it, runs the barrier, and re-examines the armed set. So a timer re-armed by an earlier handler — which is exactly what a dwell reset is — takes its position from virtual time, not from goroutine scheduling.
- **Cap-1 channels, non-blocking send.** A test that has not drained a fired timer cannot deadlock `Advance`; it simply misses the tick, exactly like a real `time.Ticker`'s consumer.

Guard rails follow one stated principle — *"a hanging test is strictly worse than a failing
one: it yields no stack, no seed, and no clue"*: `maxFiresPerAdvance = 100_000` panics
(a ten-minute virtual meet at a 1 Hz heartbeat is 600 fires, so the bound is generous);
`Advance` panics on a negative duration and `AdvanceTo` on a rewind; `NewTicker(<=0)` panics,
matching `time.NewTicker`. `Pending()` exists so a scenario can assert no control loop leaked
a deadline.

### `Settle` — the barrier that makes assertions safe

Firing a timer is not enough: the goroutine that *receives* it has not necessarily finished
reacting when `Advance` returns, so a naive assertion races. The fix exploits a property the
codebase already has — **the coordinator is a single-goroutine event loop.** Round-tripping
a no-op event through it proves every previously enqueued event has been fully processed,
because the channel is FIFO with exactly one consumer.

`VirtualClock.SetBarrier(fn)` is invoked *between* consecutive firings; `NewScenario` wires
it to `Scenario.Settle`, which calls every registered barrier under a `SettleTimeout` (5 s)
context deadline. In the real-coordinator scenarios the registered barrier **is
`coordinator.Sync`**.

The subtlety is the load-bearing part, and `AddBarrier` documents it:

> A barrier is only sound if the loop it round-trips through **DRAINS ITS FIRED TIMERS
> BEFORE ACKING**. A fire and a sync arriving at one parked `select` are resolved by Go's
> uniform-random choice, so a loop that acks without draining can report quiescent while
> the reaction the barrier exists to wait for has not happened.

This is not hypothetical — it is a permanently-flaky-test generator, and the flake gets
blamed on the harness. It is also why the coordinator multiplexes *every* deadline onto
**one** timer: with a single wake channel, "a deadline is due" and "the wake channel holds a
value" are the same statement, so draining one drains all, and the barrier is *provably*
complete rather than complete-in-practice.

### Why the harness itself does not trip the determinism gate

`SettleTimeout = 5 * time.Second` is a **constant expression, not a call**, and `Settle` uses
`context.WithTimeout(context.Background(), …)` rather than `time.After`. That is deliberate:
the deadlock guard must run on *real* time (a virtual clock nobody is advancing would never
expire it) without using any of the wall-clock entry points the gate bans.
`internal/coordinator/harness_test.go` uses the identical shape (`const guard = 10 * time.Second`
+ `context.WithTimeout` around `Sync`) and says so.

---

## 2. The independent oracles

**The tests never re-derive the expected tree.** That would just re-implement the heuristic
and prove nothing — a mirror, not an oracle. The rule instead:

> **Construct with one function, verify with an independent one.**

### `overlay.Validate(t, nodes, c)` — is this tree legal?

Re-derives everything from the tree itself and checks ten independent properties: `Epoch ≥ 1`
and `Rev ≥ 1` (an unstamped tree is unpublishable), `Root` agrees with `Edges` *and* with the
constraints, is-a-tree (single parent; exactly one parentless node), connectivity by BFS with
`len(reached) == len(nodes)` (which rules out both cycles and shared children), depth ≤
`MaxDepth`, topological edge order, degree ≤ capacity-derived-from-upload, TURN-bound nodes
have no children, closed world, and the full backup legality set — including
`B ∉ Subtree(ParentOf(node))` ("the backup must survive the failure it insures against"), no
impaired backup ("insurance written against a degraded relay is not insurance"), the
*promotion* depth bound (a promotion sinks the promoted node's whole subtree, so bounding the
backup's own depth is not enough), and the fan-in cap.

Every failure has its own message, all prefixed `validate:`, and each table case in
`validate_test.go` names the substring it expects **so a case cannot pass by tripping a
different check**.

### `overlay.ValidateLocalRepair(prev, next, nodes, c, churn)` — was this change minimal?

A **second, separate** oracle over *transitions*. "Was this change minimal?" is a property of
a *pair* of trees, and folding it into `Validate` would force `Validate` to take a `prev` it
does not otherwise want.

A parent change `P → Q` is **justified** iff any of four rules holds: `P` is in `Churn.Gone`
or absent from `next`; `u` is in `Churn.Promoted` — in the **strict form**, `Q` must *equal*
`prev.BackupOf(u)` whenever that backup is still present and eligible; `P` became ineligible
in `next` (TURN-bound, out of capacity, past `MaxDepth`, impaired); or the rank-1 rule
`rtt(u,Q) + StickinessMs < rtt(u,P)`.

Two preconditions are shouted in the source and are worth repeating because getting them
wrong produced a *numerically wrong* test bound that survived two contract revisions:

- **`prev` MUST be the last *published* tree — the tree peers were actually running — not the coordinator's patched working copy.** An oracle fed the patched copy asserts against the very belief it exists to check; and, decisively, **the published tree is the only artifact that still carries the `Backups` assignment the promotion must be checked against.** Against the patched copy, "did the promoted node land on the backup it was actually assigned?" is not merely weaker — it is impossible, because the patch overwrote the evidence.
- The oracle takes `nodes` and `Constraints` because minimality is defined relative to the builder's preference ordering, and rank 2 of that ordering is RTT-aware. An oracle that cannot see RTT cannot evaluate rank 2 — that is not a weaker oracle, it is one answering a narrower question than it claims to.

**And the limit it states about itself:** it checks a **necessary** condition, not a
sufficient one. It asserts every move was *permitted* by the ordering; it does not assert the
result was optimal, and it deliberately does **not** assert the converse (that a node which
could have improved did move). Greedy makes no such promise — an eligible closer parent may
have been filled by an earlier node — so asserting it would fail on correct output.

Both oracles are asserted from `overlay`'s own tests, from `coordinator`'s tests against the
real loop, and from `simnet`'s churn scenarios **after every rebuild**.
`TestChurnBoundedEdgeDelta` runs seeds `{1, 7, 42, 1337, 20260901}` × 200 steps with both
green at every step.

---

## 3. Deterministic simulation — and what it deliberately does not assert

`internal/simnet` is a media-free harness that drives the **real** control plane over a
virtual clock: the real `overlay.BuildTree`, the real `coordinator.Coordinator`, no sockets,
no ICE, no codecs. It is the FoundationDB / TigerBeetle deterministic-simulation idea in
miniature — *scoped to a learning project*, which is the honest framing.

It states **two non-negotiable rules**: a scenario replays identically from a seed (enforced
by the clock's firing order, the `Network`'s insertion-order iteration, `BuildTree`'s purity,
and a single seeded `*rand.Rand`); and nothing in it reads the wall clock. Notably it does
**not** import `testing` — it stays a plain library so the same harness could back a CLI
replay tool.

`Scenario` is a builder: `At(d, f)`, `Observe(f)`, `AddBarrier(name, fn)`, `ReportOrder(…)`,
`Settle()`, `Run()`. The `Network` exposes failure injection as **separate verbs rather than
one parameterised `Fault()`** — `Kill`, `Leave`, `Partition`, `Heal`, `Isolate`, `Rejoin`,
`Degrade`, `Restore` — because the distinctions between a graceful leave, a vanished machine,
a live-but-unreachable process and a merely degraded one are exactly what Phases 5 and 6 are
about.

> **What the harness deliberately does NOT assert**, in its own words: that the same fleet
> always converges to the same tree — *and it cannot*, because stickiness makes `BuildTree`
> a function of history, which is precisely the price paid for minimal-disruption rebuilds.
> The two properties cannot both hold.

An early contract asserted exactly that false property. A reviewer produced a counterexample
and it is correct; the false equality was replaced by three properties that are true and
worth the same amount:

1. **Every produced tree is legal and every transition is minimal** — both oracles hold at every step of every scenario, over *every* history rather than a chosen one.
2. **The edge-set delta between consecutive trees is bounded by the churn that caused the rebuild.** This is the property a user actually *feels*, because each changed edge is one stream interruption.
3. **Determinism given the same event SEQUENCE** — not the same event *set*. Same seed and the same *ordered* script ⇒ byte-identical trace, over ≥50 repeats.

The bounds table for (2) is worth having on hand, because two of its rows are the honest ones:

| Churn | Bound on nodes whose parent changed |
|---|---|
| one join, no re-root | exactly 1 (the joiner) |
| one leaf departs | 0 |
| one relay `X` departs, no re-root | ≤ `len(childrenOf_prev(X))` |
| one self-promotion | exactly 1, and strictly `next.ParentOf(u) == published.BackupOf(u)` |
| one node becomes `Impaired` | ≤ `len(childrenOf_prev(node))` |
| an RTT improvement past `StickinessMs` | ≤ 1 per improving node |
| the relaxed retry ran | **unbounded** — assert `Outcome == OutcomeRelaxed` was published |
| the root departs | **unbounded** — assert `Reroot == true` was published |

The last two rows are where local repair does not apply at all, and the test asserts the
coordinator **said so** rather than that it avoided them. *A design that cannot always be
local must at minimum always be legible about when it was not.*

### Property and permutation tests

- **`TestStragglerPathIsPathDependent`** enumerates all **120** first-report permutations of a counterexample fleet, asserts each tree is `Validate`-clean and holds every node, asserts the root is identical across all 120, **and asserts at least two permutations differ**. It logs `converged to 9 distinct valid trees over the same fleet`. That last assertion is a regression check on the *trade*: if the divergence ever disappears, the stability trade-off has changed and the contract must be revisited.
- **`TestReportOrderInvarianceSettledPath`** asserts the opposite for the *settled* path — byte-identical trees across all 120 permutations — and fails loudly if the permutation count is not 120, so the enumeration can never silently shrink.
- **`TestPickRootNeverRootsAGuess`** replays a real 3-peer incident: over every arrival permutation, whatever `PickRoot` returns must `BuildTree` cleanly.
- **`TestChurnReplayIsDeterministic`** runs two seeds × 120 steps and compares marshalled traces byte-for-byte across replays.
- **Zero fuzz targets and zero benchmarks** exist in the repository. Recorded rather than hidden.

### Real coordinator vs. model — which is which, and why the model survives

`simnet` drives the **real** `internal/coordinator` in `control_test.go`, and keeps a
`modelCoordinator` (test-only, in `harness_test.go`) for fast property sweeps. The split is
documented in the model's own doc comment and is not arbitrary:

| Driven by | Scenarios | Why |
|---|---|---|
| **The real loop** (`control_test.go`) | the first-build settle; report-order invariance over the shipped code; the join-order characterization; the epoch/yield fencing and handover scenarios; peer fence-refusal counting | *"If an assertion is about control-plane BEHAVIOUR, it belongs there."* |
| **The model** (`scenario_test.go`, `churn_test.go`) | hundreds of seeded churn steps; the 120-permutation straggler sweeps | Those run the graph layer thousands of times. Standing up a full control loop with two goroutines and a barrier round-trip behind each one buys no coverage the real-loop scenarios already give, and **costs the sweeps their breadth.** |

The real-loop wiring is worth knowing: `coordinator.Sender` is satisfied by `simnet.Capture`
(records pushes) and `coordinator.Publisher` by `simnet.Recorder` (records events) —
scenarios assert against the **published events** rather than internal state, *because the
contract requires the coordinator to be legible about what it did, not merely correct*.
`ReportOf` deliberately drops `Node.RTT` (pairwise latency is not measured in production, so
feeding it to the real loop would test an input the loop cannot receive) and `Node.Impaired`
(impairment is the coordinator's own conclusion after a dwell, not something a peer declares).

The model earns its continued existence with a **differential test**,
`TestModelAgreesWithTheRealCoordinator`, which drives both through the same script over all
120 permutations of a 5-node fleet and compares the marshalled edges. Its comment records the
payoff: *"That differential immediately earned itself: it caught that the harness was
conflating JOIN arrival with REPORT arrival."*

**Two limits, both stated in the source rather than discovered later:**

- A **known, deliberate asymmetry**: the model learns the whole roster at `t0`; the real coordinator learns it one `PeerJoined` at a time and rebuilds on each. The two therefore agree on the *settled* path and are **not expected to agree on an incremental join sequence** — `TestJoinOrderIsPathDependent` covers that case against the real loop, where it belongs.
- **A sweep that runs only against the model cannot discriminate a mutation in the shipped loop.** That is the residual cost of keeping it, and it is why every behavioural assertion moved to the real loop.

---

## 4. The mutation-testing discipline — the most valuable finding in the build

**There is no mutation-testing tool.** No `gremlins`, no `go-mutesting`, no Makefile target,
nothing in CI. It is a **review practice**, and its findings are recorded as test comments
and, in one case, as a whole file.

The question adversarial reviewers were asked was not "is this code correct" but:

> **Which line can I delete and keep the suite green?**

The answer, repeatedly, was: quite a few. Eight green-tests-measuring-nothing were found by
the review fleet, plus a ninth by its own author.
`internal/coordinator/guards_test.go` exists *solely* because of that audit and opens:

> Guards a mutation audit found undefended: each of these deletes cleanly from the
> implementation without any other test noticing.

It holds eight such tests. Three cases are worth walking through, because each fails
differently.

**Case A — a test that could never fire.** `TestStaleRejectedShimIsStillNeeded` is a
self-removing scaffold: it must fail the moment `metrics.Heartbeat` grows a `stale_rejected`
field, so the temporary shim reading the raw key can be deleted. It marshalled a
`metrics.Heartbeat{Name:"a", Seq:1}` and checked for the key — but *"the shipped field is
tagged `omitempty`, so a zero value omits the key and the test concluded the field did not
exist — a test passing for the wrong reason, which is the exact class of bug the scaffold was
built to prevent."* The fix asks the **type**, not an instance: reflect over the struct's JSON
tags.

**Case B — a discriminator that discriminated only one of two mutations.** A comment claimed
one test was "the discriminator" for the Sync-drains-first rule. The audit found it true for
only one mutation:

> **TWO MUTATIONS, TWO TESTS.** *Deleting* the drain is caught by
> `TestSyncDrainsFiredDeadlinesBeforeAcking`. **Reordering** it — acking before draining — is
> invisible to that test, because both statements complete before `handle()` returns.

The fix added `TestSyncDrainsBeforeItAcks`, which parks the publisher on its first event to
freeze the loop *inside* the reaction and then asserts the ack is **not yet closed** — "the
ordering question asked as a state question, which is the only way to ask it without a race."
The same file records that the naive end-to-end version does **not** discriminate: on a
multi-core machine the `Run` goroutine is almost always already awake, so a broken loop
passes anyway and *"the test reads as coverage while proving nothing."*

**Case C — a comment justifying code with a dependency that does not exist.** A sort in the
coordinator's node projection was justified with *"the projection order feeds `PickRoot`'s
tie-break and `BuildTree`'s…"*. The sweep proved that false: `PickRoot` computes a maximum
under a total order, so `BuildTree`'s output is invariant to input slice order — *given unique
names*. The comment was rewritten to the three real reasons and a new test,
`TestDuplicateNameResolutionIsDeterministic`, runs the fleet **20 independent times**, because
"a projection that ranged the map would have to win a coin flip every time to survive this."
The finding is not that the code was wrong; it is that **the stated reason was wrong, which is
how the next person deletes it.**

The habit shows up in three smaller forms you should copy:

- **A `Discrimination:` heading** on a test comment, naming the mutation the test would catch. ~40 files carry one.
- **Inline vacuity guards** — `t.Fatal("no backups assigned at all; the assertion above is vacuous")`; `"ValidateLocalRepair(nil prev) must fail loud rather than pass vacuously"`; `"both branches parented u to %q; the fixture does not discriminate the coupling"`.
- **Naming what does *not* discriminate**, so nobody re-derives it: `// Note what does NOT discriminate: r.rp is non-nil either way…`.

**The generalisable lesson, and the one worth taking into any codebase:**

> *A passing test is evidence only if you know which mutation it would have caught. Write the
> mutation down next to the assertion.*

---

## 5. Library-behaviour pins

Several of this system's designs depend on facts about pion that are **not in its
documentation**. Those facts are pinned by tests that assert the *library's* behaviour, not
ours, so a pion upgrade that changes them fails a test instead of shipping a silent
regression. Most live in `internal/media/pionbehavior_test.go`.

- **`TestPionRenegotiatesARemovalOnceConnected`** — on a **connected** `PeerConnection`, pion v4.2.16 *does* fire `OnNegotiationNeeded` for a `pc.RemoveTrack`, immediately and reliably, and the offer it asks for really does carry `a=recvonly`. It uses **raw pion** deliberately, bypassing `Session.RemoveTrack`, because the subject is what the library does unaided. **This replaced an earlier pin, `TestPionRemoveTrackDoesNotRenegotiate`, that asserted the opposite** — see §7.2 of [`DESIGN.md`](./DESIGN.md) for how a claim verified against the vendored source still came out false. Two things about its *shape* are the transferable part:
  - It **waits for `connected` first**, and that is not politeness — it is what makes the test invocation-independent. The state it depends on is now waited for explicitly instead of being implied by a duration.
  - It **waits for quiet** before the removal rather than asserting it, because pion may legitimately fire one more round on the return to `stable`, and a hard check there would blame that round on the removal. *"Nothing can be attributed to the removal until negotiation has gone quiet."*
- **`TestSessionRemoveTrackOffersExactlyOnceWhenConnected`** — the invariant the corrected finding puts at risk. On a connected session, **two** independent sources now want an offer for one removal: pion's own trigger and our `pendingLocalChange` nudge. Exactly one may go out; two offers for one removal is precisely the double-offer the serializer exists to prevent, reintroduced through its own fix. Measured 12/12 with a delta of exactly one offer, six runs each way. Whichever trigger arrives first takes `negotiating`; the other returns at the guard.
- **`TestRemovalNudgeSurvivesTheAnswer`** and **`TestSessionRemoveTrack`** (in `negotiation_test.go`) cover the *complementary* regime — a removal landing while the pc is **not stable**, which is what happens during a topology apply. There pion's `negotiationNeededOp` aborts on its signaling-state check and the notification is dropped with nothing to re-raise it, so the nudge is the only mechanism. **Deleting `pendingLocalChange` fails these deterministically.** That is the conditional claim that was true all along, under a stated reason that was not.
- **`TestNegotiationRetryResendsTheCommittedOffer`** — pion forbids `SetLocal(offer)` from `have-local-offer`, so the only legal recovery is re-sending the already-applied description. Discriminated by comparing the SDP **`o=` origin line** byte-for-byte (it carries the session version, which `CreateOffer` increments) rather than the whole body, because bodies legitimately differ as ICE candidates accumulate.
- **`TestNegotiationAnswerTimeoutResends`** — an offer lands and no answer ever comes; without the re-send that edge deadlocks forever.

One more is an **implementer obligation** rather than a pin:
`TestNegotiationSerializer`'s first subtest, whose comment says it *"must be run before
trusting the claim that pion v4 re-fires `OnNegotiationNeeded` on the return to `stable`. If
it did not, a manual re-run flag would be required after all."* A named obligation is a
lighter instrument than a pin and does the same job — it says which belief a subtest is
holding up, so the belief cannot quietly outlive its evidence.

> **One behaviour, two regimes, three tests, and none of them depend on how the suite was
> invoked.** That last clause is the acceptance bar for a library pin here.

These are necessarily wall-clock tests (`waitFor` polls; `stableFor` asserts a condition keeps
holding for a window), which is exactly why `internal/media` is **not** in
`CONTROL_PLANE_DIRS`. And it is why a pin whose subject is a race between our goroutine and
the library's dispatch goroutine must anchor on an observable **state**, not on a duration:
a duration is a proxy, and §7.2 is what happens when the proxy turns out to track something
else.

---

## 6. The determinism gate

`make check-determinism` greps four packages — `internal/overlay`, `internal/simnet`,
`internal/coordinator`, `internal/arbiter`, **including their `_test.go` files** — for every
entry point into wall-clock time:

```
time\.(Now|Since|Until|Sleep|After|AfterFunc|Tick|NewTimer|NewTicker)\(
```

and exits 1 on any hit. It is currently **silent: zero matches, no exceptions, no `//nolint`,
no skip list.** The Makefile's own comment explains why the list is exhaustive rather than
representative:

> `WALL_CLOCK_CALLS` is every entry point into package time that reads or waits on real time.
> The list is deliberately exhaustive: a guard that catches `time.Now` but misses
> `time.NewTicker` is worse than no guard, because it reads as coverage.

It is a **build gate and not a review note** for a specific reason: reaching for the wall clock
in a control-plane package breaks replayability *silently*, and the failure mode is invisible
in a diff.

Repo-wide, the only non-test wall-clock calls outside `internal/clock` are three in
`internal/media`, which is deliberately outside the gate because it runs real pion.

---

## 7. Specialized harnesses — four, each with a different clock

Each package's harness makes a different assertion about *its own* design, which is why they
are not one shared fake.

| Harness | Package | Clock | The assertion it encodes |
|---|---|---|---|
| `simnet/harness_test.go` | `simnet` | `simnet.VirtualClock` | `modelCoordinator` keeps `published`/`working`/`next` distinct, because conflating them was a critical defect in the contract and the model must not conflate them either |
| `coordinator/harness_test.go` | `coordinator_test` (**external**) | the real `simnet.VirtualClock` | Lives in the external package for one structural reason: an internal white-box file importing `simnet` would be an import cycle. Every assertion is made after a `Sync`, so it observes a quiesced loop |
| `arbiter/harness_test.go` | `arbiter_test` (external) | a `fakeClock` whose `NewTimer`/`NewTicker`/`After` **panic** | The arbiter is specified to be purely event-driven, so arming a timer is a design regression — panicking turns that into a failing test instead of a review note |
| `dashboard/harness_test.go` | `dashboard` (internal) | `fixedClock` returning dead timers | The dashboard's temporal behaviour is caching intervals, not deadlines |
| `media/fakeclock_test.go` | `media` | a minimal `fakeClock`; `NewTicker` panics | *"the media layer arms no tickers."* Deliberately much simpler than simnet's — no ordering discriminator, no event queue — because **`media` may not import `simnet`** |

`coordinator/harness_test.go` also **deliberately duplicates** `simnet.Capture`/`Recorder` as
local fakes, with the reason spelled out: the simnet versions are documented as never blocking
and never failing, and two tests need the opposite — one needs a `Sender` that *wedges*
(`TestSenderStallCannotStallTheLoop`), another needs one that wedges *and announces its entry*
(`TestAmbiguityWindowStalePushIsNeitherSentNorAccepted`). "One local pair that can do
everything beats three that each do part."

`internal/dashboard/zz_seamcheck_test.go` is a different animal entirely: **no runtime test at
all**, just four `var _ Iface = (*Impl)(nil)` declarations. It is a *compile-time* seam check —
it fails to **build** the moment a producer stops satisfying a consumer-defined interface,
which is the failure `cmd/server` would otherwise hit at wiring time. It lives in a `_test.go`
file so it links into no binary and adds no production import edge.

---

## Test inventory

69 test files, **372 top-level `func Test`**, 297 `t.Run` subtests, **0 benchmarks, 0 fuzz
targets, 0 examples**.

| Package | Test files | `func Test` | `t.Run` | Src / test LOC |
|---|---:|---:|---:|---:|
| `internal/coordinator` | 10 | 87 | 7 | 2,544 / 3,714 |
| `internal/dashboard` | 8 | 52 | 28 | 2,319 / 2,889 |
| `internal/overlay` | 8 | 41 | 21 | 1,678 / 2,892 |
| `internal/simnet` | 7 | 41 | 26 | 1,240 / 2,700 |
| `internal/media` | 13 | 38 | 24 | 3,973 / 3,514 |
| `internal/arbiter` | 6 | 31 | 82 | 1,690 / 2,421 |
| `cmd/server` | 3 | 23 | 25 | 1,252 / 1,139 |
| `internal/signaling` | 4 | 18 | 29 | 1,113 / 1,208 |
| `cmd/peer` | 1 | 17 | 17 | 870 / 1,157 |
| `internal/metrics` | 3 | 12 | 21 | 380 / 606 |
| `internal/policy` | 4 | 9 | 10 | 267 / 346 |
| `internal/clock` | 1 | 2 | 6 | 106 / 117 |
| `internal/logging` | 1 | 1 | 1 | 73 / 48 |

Read the **ratios**, not the totals. `overlay` — the package holding the algorithm and its two
oracles — carries **1.7× more test than source**; `simnet` carries **2.2×**; `coordinator`
**1.5×**. `media`, the one package that cannot be tested deterministically, is the only place
the ratio drops below 1:1.

---

## The idiom: table-driven tests + `t.Run` subtests

Still the default shape for anything with a fixed set of cases, and still worth learning from
`internal/logging/logging_test.go` or `internal/overlay/validate_test.go`:

```go
tests := []struct {
    name    string
    in      string
    want    slog.Level
    wantErr bool
}{
    {name: "warning alias", in: "warning", want: slog.LevelWarn},
    {name: "garbage is an error", in: "loud", wantErr: true},
}
for _, tt := range tests {
    t.Run(tt.name, func(t *testing.T) { /* … */ })
}
```

| Element | What it buys |
|---|---|
| Slice of anonymous structs | The test *is* a data table. Adding a case is one line, not a copy-pasted function. |
| `name` field | A human label per row; `-v` prints it and failures name it. |
| `t.Run(tt.name, …)` | Each row is an isolated subtest (`TestParseLevel/warning_alias`). One row failing doesn't stop the others, and you can re-run just that row with `-run`. |
| `want` / `wantErr` next to the input | A case reads as one self-contained fact. |

`t.Fatalf` stops the subtest immediately (use it when continuing is pointless — a returned
error makes `got` meaningless); `t.Errorf` records and continues (use it for the final value
comparison, so one run reports every mismatch). `t.Fatal*` must be called from the test's own
goroutine, never a spawned one.

**And the addition this project makes to the idiom:** name the mutation. A table case whose
`name` says what would break if the code were wrong is worth more than three that only say
what they feed in.

---

## What the coverage does *not* prove

Stated plainly, because a testing doc that oversells is worse than one that is out of date.

- **Phases 5 and 6 are verified by the automated suite, not by a live run.** Deterministic simulation of the control plane plus real-pion integration tests of the media plane — but the live end-to-end verification (relay kill → backup promotion; coordinator kill → election and migration; a non-zero `stale_rejected` count proving the fence did visible work) is **pending** (`DESIGN.md` §9.5). Any claim about *live* failover or *live* migration must carry that qualification.
- **Every live run so far has been single-host.** All demos ran multiple processes on one machine over loopback: no cross-machine result, no real NAT traversal, no real packet loss, no real congestion control. The "≈6 Mbit/s at 5 peers" figure is arithmetic on a measured per-stream bitrate, not an observed collapse.
- **The degradation, impairment and voluntary-election paths are exercised only in `simnet`**, where the inputs are injected — because `CPUPct`, `LossPct` and `RTTServerMs` are never populated in production (`DESIGN.md` §8.1). The logic is tested; the sensors do not exist.
- **PLI *response* is unproven.** File and synthetic sources have no live encoder, so the keyframe-request *plumbing* (including the upstream SSRC translation) is proven, but a source actually producing a keyframe on demand awaits a browser sender.
- **A model-only sweep cannot discriminate a mutation in the shipped coordinator loop** (§3).
- **Two ordering seams are argued but not pinned by a test.** `Publisher.Publish` is called on the coordinator's `Run` goroutine and must not block, but unlike `Sender` there is no dedicated goroutine and no bounded-queue-with-drop in front of it. And `pendingLocalChange` is cleared inside `negotiate`, after `SetLocalDescription` and before `sendDescription` — sound, but moving the clear earlier would lose a removal arriving in that window and **every existing test would still pass**.
- **`internal/media` is outside the determinism gate** and holds three wall-clock calls; its tests dominate the suite either way (~82 s of ~104 s under `-race`, ~16 s of ~17 s without). That is the price of testing against a real WebRTC stack, and it is why every other package was kept off one.
- **Coverage percentage is not a target.** `make cover` is a floor-of-confidence signal, not a number to game. 100% line coverage of `ParseLevel` still would not prove the error *message* is right.
