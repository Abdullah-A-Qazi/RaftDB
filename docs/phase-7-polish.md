# Phase 7 — Polish for Portfolio Use

## What was built

- **`raftctl`** (`cmd/raftctl`): the demo/ops CLI.
  - `put` / `get` / `delete` — go through the current leader, following redirects and rotating
    across nodes when one is down (same client discipline the tests use).
  - `status [-watch]` — a per-node cluster table (state, term, known leader, commit/applied,
    log span, key count), refreshed live with `-watch`. Each row is that node's *own* view;
    rows disagreeing during an election is information, not a bug.
- **Status RPC** on the KV service (`rpc/kv.proto`): every node answers about itself on its
  `client_addr` — the one production-code addition this phase (a read-only projection of
  `raft.Status()` plus the KV key count).
- **`scripts/run-cluster.sh`**: builds and launches every node from `cluster.json`, tears down on
  Ctrl-C. `SNAPSHOT_THRESHOLD=20 ./scripts/run-cluster.sh` makes compaction visible in demos.
- **Top-level `README.md`**: architecture diagram, quick start, a "how this works" section
  (elections/replication/safety/persistence/compaction in plain words), the tests worth reading,
  and the full known-limitations list with the paper sections that would fix each.

## Design notes

- **CLI over web dashboard.** The plan allowed "CLI and/or dashboard"; `status -watch` gives the
  same demo (kill a leader, watch the term bump and a DOWN row appear) without shipping a web
  stack. A dashboard would be a thin consumer of the same Status RPC if ever wanted.
- **Status is per-node truth, not cluster truth.** No aggregation server-side: the CLI queries
  all nodes and shows disagreement raw. Aggregating would hide exactly the states (elections,
  partitions, stale leaders) the tool exists to demonstrate.
- The demo loop that sells the whole project in ~30 seconds:
  ```
  ./scripts/run-cluster.sh          # terminal 1
  ./raftctl status -watch           # terminal 2
  ./raftctl put greeting hello      # terminal 3
  kill -9 <leader pid>              # watch term bump, DOWN row, new leader
  ./raftctl get greeting            # still there
  ```

## Test coverage added

- `kv.TestStatusReportsOwnView` — the Status projection.
- The CLI itself is a thin client over already-tested RPCs; it was exercised end-to-end against
  a live 5-node cluster (put/get/delete round-trip, leader kill → DOWN row + term 2 + compacted
  log spans in the table).
