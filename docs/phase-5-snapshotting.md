# Phase 5 — Log Compaction (Snapshotting)

Without this phase the WAL grows forever and a long-dead follower can need entries nobody keeps.
The fix (§7): periodically serialize the state machine, persist it as a snapshot, and throw away
the log prefix it covers; followers too far behind to catch up entry-by-entry get the snapshot
itself via the InstallSnapshot RPC.

## What was built

- **Threshold-triggered snapshotting** (`raft/replication.go`, `maybeSnapshotLocked`): after the
  log accumulates `SnapshotThreshold` applied entries past the last snapshot, the applier
  serializes the state machine at `lastApplied`, persists it (`snapshot.json`, atomic-replace like
  hard state), and compacts.
- **Real space reclamation** (`storage/wal.go`, `Compact`): the WAL is rewritten to contain only
  the suffix beyond the snapshot (build temp → fsync → rename → swap the open handle). Verified
  live: after compaction `wal.log` is bytes-small again.
- **InstallSnapshot** end to end: leader side (`sendSnapshot` when a peer's `nextIndex` falls at or
  below the compaction boundary), follower side (`HandleInstallSnapshot`, Figure 13), wire
  conversions (the proto existed since Phase 0), and state-machine `Snapshot()/Restore()` on the
  KV store (JSON of the map).
- **Recovery understands snapshots**: `NewNode` loads snapshot + WAL suffix; `commitIndex`/
  `lastApplied` start at the snapshot boundary (its contents are committed by construction), and
  `Start` restores the state machine before anything runs.

## The design decisions that matter

1. **The floating sentinel.** Since Phase 0, slot 0 of the log held a `{0,0}` sentinel so the log
   was 1-indexed. Now the sentinel carries the snapshot's `(lastIncludedIndex, lastIncludedTerm)`,
   and entry *i* lives at slice position `i - log[0].Index`. Every Figure 2 formula survives
   compaction untouched because index→position translation was already centralized (`termAt`,
   `entriesFrom`, truncation) — the Phase 2 comment "Phase 5 changes this mapping in one place"
   cashed in. Two things fall out for free:
   - The §7 requirement to retain the last included (index, term) for the consistency check *is*
     the sentinel: `prevLogIndex == boundary` hits `termAt(boundary)` → the snapshot's term.
   - A fully-compacted (empty) log reports the snapshot position as its last index/term — exactly
     what elections must advertise.

2. **Only the applier touches the state machine.** Restores (from InstallSnapshot) and captures
   (threshold snapshots) both run on the applier goroutine. `HandleInstallSnapshot` persists the
   snapshot and rewrites the log, but *parks* the state-machine restore (`pendingSnapshot`) for
   the applier — otherwise a restore could interleave with an in-flight `Apply` batch and the
   state machine would see time travel. This also makes `Snapshot()` trivially consistent: nothing
   applies while it serializes, so it captures exactly `lastApplied`.

3. **Ordering: snapshot first, then compact the WAL — never the reverse.** Crash between the two
   and recovery finds the new snapshot plus a WAL still holding covered entries: it skips them
   (tested). The reverse order could leave a compacted WAL with *no* snapshot covering the gap —
   lost committed state. Same rule on both the threshold path and the install path.

4. **Snapshots that don't move us forward are ignored** (`LastIncludedIndex <= commitIndex`).
   Guarding on commitIndex (not just the log base) protects the applier from rewinding the state
   machine below what clients may already have read, and makes duplicate deliveries (the leader
   re-sends until a reply lands) harmless.

5. **Figure 13 step 6 (retain-or-discard)**: if the follower's log extends past the snapshot and
   *matches its term at the boundary*, the suffix is real and survives; otherwise the whole log is
   junk from a dead leader and is discarded. Both paths tested.

6. **AppendEntries below the boundary**: a prevLogIndex inside our snapshot passes the consistency
   check by construction — snapshotted state is committed, and no legitimate leader conflicts with
   committed history (Leader Completeness, the Phase 3 rules doing load-bearing work again). The
   append walk skips entries the snapshot covers; the conflict-hint scan is clamped so it never
   descends past the boundary.

## Deviations from the paper (flagged)

- **Single-chunk InstallSnapshot.** §7 chunks snapshots with `offset`/`done` for multi-GB state;
  ours is a KV map measured in KB, so every snapshot ships as one RPC (`offset=0, done=true`).
  The proto fields exist, so chunking is an additive change later.
- **Entry-count threshold** rather than the paper's size-based suggestion — simpler to reason
  about and to force in tests (`-snapshot-threshold` on raftd, default 1000).
- **Leaders reload the snapshot from disk per send** instead of caching it — the rare path stays
  simple, at the cost of a file read per lagging-follower round.
- **JSON snapshot encoding**, consistent with every other durable artifact in this project.

## Test scenarios

| Requirement | Test |
|---|---|
| Offline follower catches up via InstallSnapshot, not stuck | `test.TestOfflineFollowerCatchesUpViaInstallSnapshot` (asserts ≥1 InstallSnapshot actually delivered, state byte-identical, then proves the rejoined node can carry a quorum) |
| Snapshot + compaction at threshold | `raft.TestApplierSnapshotsAndCompactsAtThreshold`, live check on real binaries (wal.log → 0 bytes) |
| Restart recovers via snapshot + WAL suffix | `test.TestRestartRecoversFromSnapshotAndWAL` (full-cluster restart), `raft.TestNewNodeRecoversFromSnapshotPlusWAL` (incl. crash-between-save-and-compact leftovers) |
| Receiver rules: stale leader, already-covered, retained suffix | `raft.TestInstallSnapshot*` |
| Leader switches to snapshot when nextIndex ≤ boundary | `raft.TestLeaderSendsSnapshotWhenPeerNeedsCompactedEntries` |
| Consistency check at/below the boundary | boundary heartbeat assertion in `TestApplierSnapshotsAndCompactsAtThreshold` |
| No acked write lost under churn + aggressive compaction | `test.TestAckedWritesSurviveChurnWithSnapshots` (threshold 3, repeated leader kills) |
| Storage: snapshot round-trip, WAL rewrite correctness | `storage.TestSnapshot*`, `storage.TestWALCompact*` |

## Not handled / future work

- Snapshot chunking + flow control for large state (see deviation above).
- A client whose write was pending when its leader got snapshotted past never gets a reply and
  times out (unchanged Phase 2 behavior; sessions/dedup remain future work).
- The applier holds the WAL rewrite inside a node-mutex window; huge logs would stall RPCs during
  compaction. Fine at learning scale, noted for honesty.
