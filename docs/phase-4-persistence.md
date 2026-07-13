# Phase 4 — Persistence & Crash Recovery

## What was built

- **A real write-ahead log** (`storage/wal.go`): the replicated log now lives in `wal.log`, an
  append-only stream of framed records, fsync'd before any acknowledgment. `FileStore` now
  implements both `raft.StateStore` (hardstate.json, from Phase 1) and the new `raft.LogStore`.
- **Recovery path** (`raft/node.go`, `NewNode`): on startup a node replays the WAL to rebuild its
  in-memory log (validated contiguous from index 1), alongside the recovered term/vote.
  `commitIndex`/`lastApplied` deliberately start at 0 — Figure 2 marks them volatile; the next
  leader's traffic re-establishes them and the applier replays the committed prefix into the KV
  store, rebuilding it from the log alone.
- **Durability at every mutation point**: leader `Propose`, follower append, and follower conflict
  truncation all hit disk (fsync included) *before* the in-memory state — and therefore before any
  reply, quorum count, or client ack can observe them.
- **Real kill -9 testing** (`test/proc_test.go`): five actual `raftd` processes, SIGKILL'd singly
  and in simultaneous pairs during live writes, restarted onto the same data dirs.

## WAL design (the interview-worthy parts)

**Record framing**: `[4B length | 4B CRC32(payload) | JSON payload]`. The framing exists for
exactly one failure mode: a crash mid-append leaves a torn record at the tail. On load, the first
record that is short, has a garbage length, or fails its CRC marks the end of the log; the file is
truncated there and appends continue from that clean boundary. Discarding the tail is *safe*, not
just convenient, because of fsync-before-ack: a record not fully on disk was by definition never
acknowledged to anyone — dropping it breaks no promise. `TestWALTornTailAtEveryOffset` proves this
by chopping the file at **every byte offset** inside the final record.

**Truncation is a record, not a file rewrite.** The Raft log mutates in only two ways — append at
the tail, drop a suffix on conflict — so the WAL has two record types, and replay converges to the
same log. Writing a "truncate from N" record keeps every disk operation an O(1) tail append, the
only pattern that's cheap to make durable. Dead bytes accumulate until Phase 5's compaction.

**Why fsync per batch is non-negotiable**: when a follower replies "success", the leader counts it
toward the entry's quorum. If that follower could lose the entry in a kill -9, the entry might be
"committed" while existing on fewer live nodes than a majority — at which point the election
restriction can no longer guarantee the next leader has it. The fsync is what makes the majority
arithmetic mean something. Same logic for the leader's own append (it counts itself) and for
truncations (a resurrected zombie suffix would diverge from the log the cluster agreed on).

**Two files, two shapes**: hard state is two overwrite-in-place fields → atomic replace
(temp + fsync + rename); the log is append-shaped → WAL. There is no ordering dependency between
the two files: each is updated before the RPC reply that depends on it, and any crash between the
two writes leaves a state some legal execution could have produced anyway.

## Recovery semantics worth being able to explain

- **The state machine is not persisted** (until Phase 5's snapshots). A restarted node's KV map is
  rebuilt by replaying the log from index 1 — correctness comes from apply-order determinism, the
  same property that makes replication work. Symmetric consequence: `lastApplied` must start at 0.
- **A restarted node does not know what was committed.** It has the entries but not
  `commitIndex`; it learns it from the next leader heartbeat. After a *full-cluster* restart this
  matters: nothing applies until the new leader commits its first own-term entry (the Phase 3
  no-op gap, now load-bearing — `TestFullClusterRestartRecoversData` documents it live).
- **Stopped instances are inert**: handlers and reply paths check `stopped` so a goroutine that
  outlives `Stop()` can never write to files a restarted incarnation now owns (an in-process
  hazard; real processes get this from the OS).

## Test scenarios

| Requirement | Test |
|---|---|
| kill -9 each node (and pairs, 2 simultaneous) during writes; no lost acked writes | `test.TestKill9EveryNodeAndPairsDuringWrites` — 15 SIGKILLs of real processes |
| Recovery rebuilds correct state/term/log from disk | `test.TestFullClusterRestartRecoversData` (all 5 down at once — no live replica to copy from), `raft.TestNewNodeRecoversLog` |
| Torn/corrupt WAL tails | `storage.TestWALTornTailAtEveryOffset`, `TestWALCorruptTailDropped` |
| Truncate + re-append replay | `storage.TestWALTruncateReplay`, `TestWALAppendAfterReload` |
| Durable before reply/ack (ordering) | `raft.TestFollowerPersistsEntriesBeforeReply`, `TestFollowerPersistsTruncationBeforeReply`, `TestLeaderPersistsOwnEntryOnPropose` |
| Corrupt/holey recovered logs refused | `storage.TestWALRejectsNonContiguousLog`, `raft.TestNewNodeRejectsNonContiguousRecoveredLog` |

The proc test is skipped under `go test -short` (it builds a binary and churns processes);
everything else runs everywhere. The in-process harness now also uses the real WAL, so every
existing restart test exercises disk recovery too.

## Deviations & known gaps (flagged)

- **JSON record payloads** inside binary framing — debuggable (`xxd` + `jq`) over compact;
  consistent with the KV command encoding choice. Production would use protobuf/binary.
- **Mid-file corruption (disk rot) is indistinguishable from a torn tail**: both stop the replay.
  Under the crash-only failure model this phase targets (kill -9, power cut) that's correct;
  surviving byte rot needs per-record recovery decisions or checksummed segments — out of scope.
- **No lock file** on the data directory: two processes sharing one node's dir would corrupt it.
  The config already makes IDs unique; a flock is listed as future work.
- **The WAL only grows** — truncation records and dead entries are never reclaimed until Phase 5
  (log compaction), which is precisely the next phase's job.
- **fsyncs happen under the node mutex**: simple and correct, caps throughput at disk sync
  latency. Group commit/pipelining is a known optimization, explicitly out of scope per the plan.
