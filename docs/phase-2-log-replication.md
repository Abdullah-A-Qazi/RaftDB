# Phase 2 — Log Replication & the KV State Machine

## What was built

- **Propose path** (`raft/node.go`): the leader appends client commands to its log and kicks the
  replication loop; `Propose` returns the `(index, term)` pair whose commitment is the only proof
  the command survived.
- **Replication loop** (`raft/replication.go`): the Phase 1 heartbeat loop became the replication
  loop — every round sends each peer exactly the entries it is missing (an empty round *is* a
  heartbeat). Success advances `matchIndex`/`nextIndex` and possibly `commitIndex`; rejection backs
  `nextIndex` off using conflict hints and retries immediately.
- **Full AppendEntries receiver** (`raft/handlers.go`): the §5.3 consistency check, conflict-hint
  generation, safe truncation of conflicting suffixes, appending, and
  `commitIndex = min(leaderCommit, last new entry)`.
- **Applier** (`raft/replication.go`): one goroutine per node feeds committed entries to the
  state machine in strictly ascending index order, exactly once per node lifetime.
- **KV layer** (`kv/`): JSON-encoded `put`/`delete` commands, the in-memory `Store` (the actual
  replicated state machine), and the client-facing gRPC `Server` with leader redirection
  (`rpc/kv.proto`, served on each node's `client_addr` by `raftd`).

## How a write actually flows (interview version)

1. Client sends `Put(k,v)` to any node; a non-leader answers with a redirect to the leader.
2. Leader encodes the command, appends it to its own log at index *i*, term *t*, and returns
   `(i, t)` to the KV server, which parks the request on a waiter for index *i*.
3. The replication loop sends each follower the entries it's missing. Followers run the
   consistency check, append, and acknowledge.
4. When a majority's `matchIndex` reaches *i*, the leader sets `commitIndex = i`.
5. The applier feeds entry *i* to the KV store; the waiter for *i* fires — **only if the applied
   entry still carries term *t***, otherwise the client gets an error (its entry was replaced by
   another leader's) — and only then is the client acknowledged.
6. Followers learn `commitIndex` on the next AppendEntries and apply independently.

The client ack therefore certifies: on a majority of nodes, ordered, applied on the leader.

## Key design decisions

1. **Heartbeats and replication are one code path.** A heartbeat is a replication round with no
   entries. This means the consistency-check/backoff machinery runs continuously instead of only
   under write load — bugs surface in seconds, not in rare interleavings.

2. **The (index, term) waiter contract.** "My entry committed" cannot be inferred from "index *i*
   was applied" — a different leader's entry can occupy *i* after a leadership change. Waiters
   store the proposal term and compare at apply time (`kv/server.go: fireWaiter`); a term mismatch
   is reported as `ErrNotLeader`, never a silent false OK.

3. **Truncate only on a real term conflict** (`HandleAppendEntries`). Duplicated or reordered
   AppendEntries covering an old prefix must not chop newer entries off the log — a naive
   "truncate at prevLogIndex+1, then append" deletes *committed* entries under RPC re-delivery.
   The walk skips entries we already have and truncates only where terms genuinely disagree
   (`TestStaleDuplicateAppendDoesNotTruncate`).

4. **`matchIndex` computed from the request, capped monotonic.** Replies arrive out of order; a
   late ack for a short prefix must not drag `matchIndex` backwards (`TestMatchIndexNeverRegresses`).
   And it's computed from `args.PrevLogIndex + len(args.Entries)` — never from the current log
   length, which may have grown since the request was built.

5. **Conflict hints (fast backtracking, §5.3).** The follower reports the conflicting term and its
   first index of that term; the leader skips whole terms per round trip instead of probing one
   index at a time. This is why a follower restarted with an empty (in-memory) log catches up in
   two round trips rather than one per entry. `nextIndexAfterRejection` is a pure function with a
   table test.

6. **`commitIndex = min(leaderCommit, last new entry)` on followers.** The leader's commitIndex
   may point past what *this particular request* verified; trusting it blindly would mark
   unverified entries committed (`TestFollowerCommitClampedToLastNewEntry`).

7. **Reads are leader-local — NOT linearizable (flagged deviation).** `Get` returns the leader's
   applied state without touching the log. A leader deposed behind a partition it can't see can
   serve a stale value until it learns the new term. Fixes (ReadIndex §6.4 of the thesis, or
   leases) are future work per the project plan; writes are unaffected.

8. **No client request deduplication (flagged limitation).** A client whose Put times out cannot
   know whether it committed ("limbo" — see `TestWriteNotAckedWithoutQuorum`, where the un-acked
   write commits after the partition heals). Retrying a Put is idempotent in effect for this KV
   (same key/value), but the general fix is client sessions + serial numbers (thesis §6.3);
   deliberately out of scope.

## THE known unsafety (until Phase 3)

`advanceCommitIndexLocked` implements Figure 2's majority rule **without** the
`log[N].term == currentTerm` clause — the plan assigns that rule, the election restriction, and
the Figure 8 walkthrough to Phase 3. Until then there exist crash-and-partition interleavings
(exactly Figure 8) where an entry counted as committed can be overwritten by a later leader. The
insertion points are marked with loud comments in `advanceCommitIndexLocked` and
`HandleRequestVote`. None of the Phase 2 tests can trigger it (they don't contrive the required
double leader change), which is precisely why Phase 3 builds that harness.

## Test scenarios (all in `go test -race ./...`)

| Requirement | Test |
|---|---|
| Write visible on leader after commit (ack ⇒ applied) | `test.TestWriteVisibleOnLeaderAfterCommit` |
| Restarted follower catches up (incl. overwrites/deletes) | `test.TestFollowerRestartCatchesUp` |
| Same apply order on every node + identical final maps | `test.TestSameApplyOrderOnAllNodes` |
| No ack until majority; kill minority mid-write → write survives | `test.TestWriteSucceedsWithMinorityDown`, `test.TestWriteNotAckedWithoutQuorum`, `raft.TestLeaderCommitsOnlyWithMajority` |
| Leader redirection | `test.TestFollowerRedirectsToLeader`, `kv.TestFollowerRedirectsWrites/Reads` |
| Truncation safety, conflict hints, commit clamping | `raft.TestStaleDuplicateAppendDoesNotTruncate`, `raft.TestFollowerRejects*WithHints`, `raft.TestFollowerCommitClampedToLastNewEntry`, `raft.TestNextIndexAfterRejection` |
| Waiter term check (no false OK to clients) | `kv.TestWaiterRejectsSupersededEntry` |

Verified end-to-end over real gRPC: 3 `raftd` processes, a client hitting a follower got
redirected to the leader, Put/Get/Delete round-tripped through consensus.

## Not handled yet

- Figure 8 commit rule + election restriction — **Phase 3** (next).
- Log persistence: the log is in-memory; a restarting node recovers it from the leader, not from
  disk. Real WAL + recovery in **Phase 4**.
- Log compaction/snapshots — **Phase 5**; the log grows without bound until then.
- Linearizable reads, client dedup — future work (see deviations above).
