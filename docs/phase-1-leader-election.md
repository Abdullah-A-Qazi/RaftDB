# Phase 1 — Leader Election

## What was built

- **Node states and transitions** (`raft/node.go`, `raft/election.go`): Follower → Candidate on
  election timeout; Candidate → Leader on quorum of votes; anyone → Follower on seeing a higher
  term. One long-lived ticker goroutine per node watches the election timer; leaders additionally
  run a heartbeat goroutine per term.
- **Randomized election timeouts** (default 150–300ms, per §5.6) and **heartbeats** (empty
  AppendEntries every 50ms by default).
- **RPC handlers** (`raft/handlers.go`): `HandleRequestVote` and `HandleAppendEntries` implementing
  Figure 2's term rules ("Rules for Servers") — heartbeat-only for AppendEntries in this phase.
- **Durable hard state** (`storage/storage.go`): `currentTerm` and `votedFor` are written to disk
  (write-temp → fsync → rename → fsync-dir) *before* any RPC reply leaves the node, and recovered
  in `NewNode` on restart.
- **gRPC wiring** (`rpc/server.go`, `rpc/transport.go`): the proto service from Phase 0 is now
  served and consumed; `cmd/raftd` runs a real node.
- **Test harness** (`test/cluster.go`): an in-process cluster over an in-memory transport with
  per-node crash/disconnect/restart — the seed of Phase 6's partition harness.

## Key design decisions (and the *why*s worth being able to explain)

1. **Persist-before-reply is the load-bearing wall.** Figure 2's fine print says persistent state
   is "updated on stable storage before responding to RPCs". The failure it prevents: vote for A,
   crash, restart with `votedFor` erased, vote for B → term has two votes from one node → two
   leaders possible. Same for `currentTerm`: forgetting it lets a deposed leader's heartbeats be
   accepted as current. Every mutation path calls `persistLocked()` before the lock is released.
   A store failure **panics** — Raft assumes fail-stop nodes, and a node that can't remember its
   promises must stop making them.

2. **Same-term step-down must NOT clear `votedFor`.** When a candidate learns a leader exists for
   its own term (AppendEntries at equal term), it becomes a follower — but it voted for itself
   this term. `becomeFollowerLocked` clears the vote *only when the term increases*. Clearing it
   on same-term step-down is a classic bug that re-enables double voting.
   Test: `TestCandidateStepsDownSameTermKeepsVote`.

3. **Stale-reply guards ("term confusion").** A vote reply can arrive after the candidate has
   moved on (newer election, stepped down, already won). Every reply handler re-checks
   `state == Candidate && currentTerm == termWhenSent` under the lock before counting; win fires
   on `votes == quorum` (not `>=`) so late votes can't re-trigger it.
   Test: `TestStaleVoteRepliesAreIgnored`.

4. **Election timer reset points are deliberate.** Reset on: accepting an AppendEntries from the
   current leader, granting a vote, starting an election, and adopting a *higher* term. NOT reset
   on receiving a vote request that gets denied — a candidate that can't win must not be able to
   suppress the voter's own candidacy (this matters more once Phase 3 makes denials common).

5. **Randomized timeouts are correctness-adjacent, not cosmetic.** With identical timeouts, a
   split vote repeats forever (all candidates re-time-out together). Each reset draws a fresh
   uniform duration from [min, max], so after a split one node usually fires first and wins before
   the others wake. `TestSplitVoteResolves` forces ≥3 simultaneous candidates and requires
   convergence.

6. **RPCs are never sent while holding the node's mutex.** Handlers lock the receiver; if node A
   called node B's handler while holding A's lock (as the in-memory test transport does directly),
   two nodes calling each other would deadlock. Pattern everywhere: snapshot args under lock →
   unlock → RPC in a goroutine → re-lock to process the reply. This also keeps a slow peer from
   stalling our state machine over real gRPC.

7. **Atomic-rename file store instead of a WAL** (deviation from the plan's "simple file-based WAL
   is fine"). Hard state is two overwrite-in-place fields, not an append stream: temp file → fsync
   → rename → fsync dir gives crash atomicity (a reader sees the whole old state or the whole new
   state, never a torn file). The log, which *is* append-shaped, gets a real WAL in Phase 4.
   Corrupt state files are a refused startup, not a silent fresh start.

8. **Storage interface moved into `package raft`** (refactor of the Phase 0 sketch): raft calls the
   store, so raft owns the interface (`raft.StateStore`, `raft.HardState`); `storage` implements
   it. Keeps the import arrow one-directional.

## Deviations from the paper (flagged)

- **Election restriction (§5.4.1) deferred to Phase 3** per the project plan. Figure 2 includes
  the "candidate's log at least as up-to-date" check in RequestVote; the marked TODO sits in
  `HandleRequestVote`. Harmless in Phases 1–2 only because all logs are empty/identical.
- **Log consistency check in AppendEntries deferred to Phase 2** (heartbeats are accepted on term
  alone for now); the insertion point is marked in `HandleAppendEntries`.
- **Timer reset on higher-term adoption** (see decision 4): Figure 2's literal reset triggers are
  only leader-contact and vote-grant. Resetting when we *enter a new term* is a mild, widely-used
  addition that prevents an ex-leader/candidate stampede the instant they step down.
- **No PreVote (§9.6 of Ongaro's thesis).** A node isolated from the cluster keeps timing out and
  inflating its term; on rejoin it forces a disruptive re-election (the cluster converges anyway —
  `TestRejoinedNodeFollowsNewLeader` exercises the benign path). PreVote is the fix; listed as
  future work.
- **No per-RPC retries within an election.** The paper says candidates retry RPCs; here a failed
  round just waits for the next randomized timeout, which retries with a fresh term. Simpler, and
  the liveness cost is bounded by one timeout.

## What is NOT handled yet

- No log replication or client commands (Phase 2) — heartbeats carry no entries and followers
  skip the consistency check.
- No election restriction (Phase 3) — with real logs this would be unsafe; it is safe now only
  because logs cannot diverge yet.
- Log is in-memory only (Phase 4 WAL); only term/vote survive crashes.
- Whole-node disconnect only in the harness; per-link partitions arrive in Phase 6.

## Test scenarios (all in `go test -race ./...`)

| Requirement | Test |
|---|---|
| Single leader in healthy 5-node cluster, stays stable | `test.TestHealthyClusterElectsSingleLeader` |
| Leader crash → new election within bounded time | `test.TestLeaderCrashTriggersNewElection` |
| Simultaneous candidates resolve, no deadlock | `test.TestSplitVoteResolves` (forces ≥3 concurrent candidates) + `raft.TestElectionRetriesWithNewTermsWhenVotesDenied` |
| Node down during election follows new leader on rejoin | `test.TestRejoinedNodeFollowsNewLeader` |
| Vote/term survive restart (persist-before-reply) | `test.TestVoteSurvivesRestart`, `test.TestTermSurvivesRestart`, `raft.TestHardStatePersistedByHandlerReturn` |
| One vote per term; idempotent regrant | `raft.TestRequestVoteOneVotePerTerm` |
| Stale leaders/candidates rejected & stepped down | `raft.TestAppendEntriesRejectsStaleLeader`, `raft.TestCandidateStepsDownOnHigherTermVoteReply`, `raft.TestLeaderStepsDownOnHigherTermHeartbeatReply` |

## Try it live

```sh
go build -o raftd ./cmd/raftd
./raftd -config cluster.json -id node1 &
./raftd -config cluster.json -id node2 &
./raftd -config cluster.json -id node3 &   # quorum of the 5-node config
```

Watch the logs: one node's timer fires first, it collects two votes, logs `won election`, and the
others settle as followers. Kill the leader (`kill %n`) and a new election appears within ~300ms.
Durable state lands in `data/<node-id>/hardstate.json`.
