# Partition Testing & Chaos Results (Phase 6)

This is the evidence file for the claim that the cluster **self-heals across network
partitions without losing acknowledged writes or accepting split-brain writes**. All scenarios
live in `test/partition_test.go` and run in every `go test ./test/` invocation (race detector on;
each scenario verified over repeated runs).

## The harness

`test/cluster.go` gained a **directional link matrix**: `blocked[from][to]` makes the in-memory
transport drop every RPC from → to while both processes keep running — a network failure, not a
crash, which is the distinction that matters: a crashed leader is silent, a *partitioned* leader
is alive, convinced it leads, and answering clients. API:

- `BlockLink(from, to)` / `UnblockLink` — one direction of one link (asymmetric failures);
- `Partition(side...)` — full bidirectional split between a node set and its complement;
- `Heal()` — restore every link;
- unchanged from earlier phases: `Stop`/`Restart` (crash), `Disconnect` (whole-node isolation).

No production code changed in this phase. Partitions are a transport phenomenon, and the raft
core never knew the difference — which is itself a design validation.

## Scenario 1 — symmetric 3/2 split, leader in the majority
`TestSymmetricPartitionMinorityStarves`

Two followers are cut off. Sampled every 10ms across six election-timeout periods:

- **Observed**: both minority nodes became candidates repeatedly (term inflation), and **neither
  ever reported the Leader state** — 2 votes < quorum 3, always.
- A `Put` sent directly to a minority node returned a redirect/timeout, never an ack.
- The majority side served 3 writes with no interruption (the leader never lost quorum, so no
  election even happened there).
- **On heal**: the rejoining candidates' inflated terms deposed the leader once (expected — no
  PreVote, flagged since Phase 1), the election restriction denied their stale logs, a
  full-logged node won, and both stragglers converged to the full write history. The
  never-acked minority write did not exist anywhere.

## Scenario 2 — the partition isolates the current leader (split brain proper)
`TestPartitionIsolatingLeaderLosesNoAckedWrites`

The leader plus one follower on one side, three nodes on the other.

- A write sent to the **stale leader** was accepted into its log but could never commit
  (2 < 3); the client got a **timeout, not an ack**. This is the moment a broken implementation
  acks and loses data.
- **Observed**: the majority elected a new leader at a strictly higher term within one election
  timeout and acked writes throughout.
- **On heal**: the deposed leader saw the higher term, stepped down to follower, and its
  uncommitted "lost" entry was truncated in favor of the majority's history (the Phase 4
  truncation-of-committed panic guard stayed silent — it was uncommitted, as designed). Final
  audit of all five state machines: the majority's writes everywhere, the split-brain write
  **nowhere**.

## Scenario 3 — asymmetric partition (one-way link failure)
`TestAsymmetricPartitionConverges`

`BlockLink(leader, follower)`: the leader's heartbeats to one follower vanish, but the
follower's own RPCs still deliver. This manufactures the **disruptive server** pattern: the
starved follower times out, campaigns at ever-higher terms, and (lacking PreVote) each campaign
can depose a working leader.

- **Observed**: brief leadership churn, then the cluster settled on an arrangement where every
  node was served (leadership landed on a node whose links all work — the starved node itself,
  or a third node). All writes issued during the disruption were eventually acked and were
  present on **all five nodes** at the end. Convergence well inside the 15s bound (sub-second in
  typical runs).
- This test is the honest cost of skipping PreVote (§9.6 of the thesis): liveness wobbles,
  safety never.

## Scenario 4 — rapid partition/heal cycling under compaction, with a split-brain monitor
`TestRapidPartitionHealCycling`

Ten cycles of rotating 2-node partitions (the cut regularly catches the current leader),
~180ms per cycle, writes during and after every cut, **snapshotting enabled** (threshold 5) so
log compaction and InstallSnapshot happen mid-chaos. A background monitor samples every node
every 10ms and records `term → leader` claims; two different nodes claiming the **same term** is
the formal definition of split brain and fails the test.

- **Observed** (representative run): `cycled 10 partitions; 20 acked writes verified on all
  nodes; 8 terms had (unique) leaders` — eight leadership changes forced, zero same-term double
  leaders across ~thousands of samples, all 20 acked writes on all 5 nodes, stores byte-identical.

## Invariants these scenarios pin down

1. **A minority never elects a leader and never acks a write** (scenario 1) — quorum arithmetic.
2. **An acked write is never lost, a never-acked write never resurrects** (scenarios 2, 4) —
   the Phase 3 safety rules under real partitions.
3. **At most one leader per term, ever** (scenario 4's monitor) — Election Safety, observed
   continuously rather than asserted at endpoints.
4. **Heals converge without operator action** (all) — stale nodes reconcile via term adoption,
   log truncation of uncommitted junk, and (with compaction on) InstallSnapshot.

## Honest caveats

- Partitions here are **in-process** (dropped function calls), not iptables. Real networks add
  latency, reordering, and half-open TCP — the gRPC layer sees none of that in these tests. The
  kill -9 process tests (Phase 4) cover the process-reality axis; combining both (real processes
  + packet filters) would be the next fidelity step.
- Liveness under asymmetric failure is wobbly by design (no PreVote); these tests bound it
  loosely rather than asserting tight election counts.
- The monitor samples at 10ms; a sub-sample same-term double-leader would slip past it. The
  deterministic Figure 8 replay (Phase 3) covers the adversarial interleavings sampling can't.
