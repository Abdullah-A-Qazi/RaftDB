# Phase 0 — Project Scaffold

## What was built

- **Go module** `github.com/Abdullah-A-Qazi/RaftDB` (matches the GitHub remote).
- **Directory layout**:

  | Directory | Contents |
  |---|---|
  | `raft/` | Core Raft algorithm. Phase 0: `RaftNode` struct mirroring Figure 2, node states, log helpers. |
  | `kv/` | Key-value state machine (placeholder; implemented in Phase 2). |
  | `rpc/` | `raft.proto` definitions; generated stubs in `rpc/raftpb/`. |
  | `storage/` | Persistence contract (`Store` interface); implementations in Phases 1/4/5. |
  | `config/` | Static cluster config loading + validation. *(Added beyond the original list — cluster membership is needed by both `raft` and `cmd`, so it gets its own dependency-free package.)* |
  | `cmd/raftd/` | Server daemon entrypoint. Phase 0: loads config, constructs a node, prints status. |
  | `test/` | Multi-node integration/chaos harness (placeholder; built out from Phase 2 on). |
  | `docs/` | These phase write-ups. |

- **Cluster config format**: JSON (`cluster.json`), a static list of `{id, raft_addr, client_addr}`.
  Validation rejects empty/duplicate IDs and reused addresses. `QuorumSize()` derives majority
  from the member count — every node must run with an identical config, since disagreement about
  membership means disagreement about what a majority is.
- **Proto definitions** (`rpc/raft.proto`): `RequestVote`, `AppendEntries`, `InstallSnapshot`
  request/response messages and the `Raft` gRPC service, per Figure 2. Regenerate with `make proto`
  (plugins installed via `make tools`).
- **`RaftNode` struct** (`raft/node.go`) with every Figure 2 field — persistent (`currentTerm`,
  `votedFor`, `log[]`), volatile (`commitIndex`, `lastApplied`), leader-volatile (`nextIndex[]`,
  `matchIndex[]`) — plus the Follower/Candidate/Leader role enum and a `Status()` snapshot method.

## Key design decisions

1. **Separate Raft and client addresses per node.** Peer RPCs and client traffic get different
   listeners so heartbeats never queue behind client load, and so the Phase 6 partition harness can
   cut inter-node links while still letting tests query each node.

2. **String node IDs instead of the paper's integer IDs.** Human-readable IDs (`node1`…) make logs
   and test failures legible. Nothing in Raft requires ordered IDs. `votedFor`'s "null" becomes the
   empty string (`raft.None`), which is safe because config validation forbids empty IDs.

3. **Sentinel log entry at slot 0.** The paper's log is 1-indexed and formulas like
   `prevLogIndex = nextIndex - 1` assume index 0 "exists" as an empty predecessor. Keeping a dummy
   `{Term: 0, Index: 0}` entry at slot 0 means every formula from Figure 2 transcribes directly with
   no off-by-one adjustments — the single most common source of bugs in from-scratch Rafts.

4. **Explicit `Index` field on log entries** (deviation from the paper, where index is positional).
   Costs a few bytes per entry; pays for itself in debuggability and becomes necessary in Phase 5
   when snapshotting truncates the log prefix and slice position ≠ log index.

5. **`conflict_index`/`conflict_term` fields declared in `AppendEntriesResponse` now** (deviation:
   Figure 2 has only `{term, success}`). This is the §5.3 fast log-backtracking optimization.
   Declaring the fields now avoids a wire-format change later; they stay unused until Phase 2/3.

6. **Domain `raft.LogEntry` type separate from the protobuf `raftpb.LogEntry`.** The algorithm
   shouldn't be coupled to the wire encoding; conversion happens at the RPC boundary. Slightly more
   code, much cleaner core.

7. **One coarse `sync.Mutex` over all node state.** Raft transitions touch several fields
   atomically (granting a vote reads `currentTerm` + log, writes `votedFor`). Fine-grained locking
   is where subtle races breed, and performance is explicitly not a goal yet.

8. **Terms and indexes are `uint64`, starting at 0.** A fresh cluster is at term 0 with last log
   index 0 (the sentinel), exactly what a first `RequestVote` should carry.

9. **JSON for config** — parseable with the standard library, no new dependency. The only
   third-party dependencies are `grpc` and `protobuf`, per the ground rules.

## What is stubbed / not handled yet

- **No persistence.** `currentTerm`/`votedFor` live only in memory; the `storage.Store` interface
  documents the contract (durable **before** responding to RPCs — otherwise a node could vote
  twice in one term after a crash) but has no implementation until Phase 1.
- **No server.** `raftd` loads config and prints status; no gRPC listener, no timers, no elections.
- **No behavior on `RaftNode`.** Fields only; `nextIndex`/`matchIndex` stay `nil` until a node wins
  an election in Phase 1.
- **Static membership only.** No joint consensus (§6); changing the cluster means restarting it.

## How to verify

```sh
make tools            # one-time: install protoc plugins (protoc itself via brew)
make proto            # regenerate stubs (already checked in)
make build test vet   # everything should pass
go run ./cmd/raftd -config cluster.json -id node1
```

Tests in this phase: config load/validation edge cases (duplicates, empties, malformed JSON),
peer/quorum derivation, initial `RaftNode` state per Figure 2, sentinel-log index/term helpers,
and a protobuf round-trip sanity check on the generated stubs.
