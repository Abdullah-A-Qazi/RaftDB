# Phase 3 — Safety Rules (§5.4, the part that's usually implemented wrong)

Phases 1–2 built a system that *works*; this phase makes it *safe*. Two small rules, each a
handful of lines, each catastrophic to omit:

1. **Election restriction** (`raft/handlers.go`, `HandleRequestVote`): a voter denies any
   candidate whose log is less up-to-date than its own — compare last entry **terms** first,
   log length only as a tiebreaker.
2. **Current-term commit rule** (`raft/replication.go`, `advanceCommitIndexLocked`): a leader
   only *directly* commits entries from its **own** term; older entries commit indirectly, as a
   side effect.

Also added: a fail-stop assertion that no leader ever asks a follower to truncate a committed
entry. That situation is impossible while the two rules hold, so hitting it means a safety bug —
and silently erasing acknowledged writes is the one thing this system must never do quietly.

## Why the election restriction works (the two-majorities argument)

A committed entry is, by definition, on a **majority** of nodes. A winning candidate needs votes
from a **majority** of nodes. Any two majorities intersect in at least one node — so at least one
voter holds the committed entry. The restriction makes that voter deny any candidate whose log
lacks it. Therefore *no electable candidate can be missing a committed entry* (Leader
Completeness). One subtlety worth being able to defend: "up-to-date" is `(lastTerm, lastIndex)`
lexicographically, **not** log length — a longer log can be stuffed with a deposed leader's
uncommitted junk, while a shorter log ending in a higher term provably contains the committed
prefix. (`TestElectionRestrictionMatrix` — including the "100 junk entries lose to a higher term"
case.) A denial must not consume the voter's vote for that term, or a viable candidate arriving
second would be locked out (`TestDeniedVoteIsNotBurned`).

## Figure 8, step by step

The paper's Figure 8 is the interleaving that breaks the "obvious" commit rule
("majority-replicated ⇒ committed"). Mapping of the paper's states to our replay
(`raft/safety_test.go`, `buildFigure8`):

| Paper | What happens | In the test |
|---|---|---|
| (a) | S1 leads term 2, appends entry *e* = (idx 2, term 2); it reaches only S2 | `feed` to s1, s2 |
| (b) | S1 crashes. S5 wins term 3 with votes from S3, S4 (their logs end at (1,1), so the restriction *allows* this) and appends its own (idx 2, term 3) locally | asserted grants + `makeLeader(s5, 3)` + `Propose` |
| (c) | S5 crashes. S1 returns, wins term 4 (S2: identical log; S3: last term 2 > 1), and continues replicating *e* — now on {S1, S2, S3}, **a majority** | asserted grants + `feed(s3)` + acks |
| (d) | If S1 now counted *e* as committed and crashed: S5's log ends at (2, **term 3**), beating every voter's (2, term 2) — S5 wins term 5 and overwrites *e* everywhere. **A committed entry is erased.** | `TestFigure8OldEntryLegallyOverwritten` |
| (e) | Instead, if S1 first replicates an entry of its own term 4 to a majority, *that* commits — and seals everything beneath it | `TestFigure8CurrentTermEntrySealsThePrefix` |

The two branch tests pin down both sides of the rule:

- **Branch (d)** asserts `commitIndex == 0` despite *e* sitting on a majority, then verifies S5's
  term-5 candidacy is *granted* by S2/S3/S4 and its overwrite of S2's entry goes through cleanly.
  The lesson people miss: the rule does **not** prevent old-term entries from being overwritten —
  that rewrite is legal and necessary. It prevents them from being **called committed** (acked to
  a client) while still overwritable. No ack, no loss.
- **Branch (e)** asserts that committing one term-4 entry drags (idx 1) and (idx 2) in with it
  (`commitIndex == 3`), and that S5 is now **denied** by S2/S3 (their last term 4 > S5's 3), so it
  can never assemble a quorum again. Committed means sealed.

Why does a *current-term* majority suffice when an old-term majority didn't? Because of the
election restriction's term-first comparison: to win, a future candidate must show a last term ≥ 4
to those voters, and the only way to have a term-4 entry is to have gotten it from S1's log —
which contains everything below it (Log Matching). The two rules interlock; neither is safe alone.

Behavioral form: `test.TestAckedWritesSurviveLeaderChurn` runs five rounds of
write → kill leader → write → rejoin over real elections and asserts every acknowledged write is
present on every node (and nothing else is). The truncation assertion turns any regression into a
loud panic rather than a quietly wrong map.

## Deviations / known gaps (flagged)

- **No no-op entry on election win.** With the commit rule, a fresh leader cannot commit inherited
  entries until it commits something of its own term. The paper (§8) has leaders append an empty
  no-op immediately on winning to unblock this; we rely on the next client write instead. Pure
  liveness (never safety) — a quiet cluster just leaves inherited entries uncommitted a little
  longer. Deferred: it would shift every KV log index in existing tests and needs a "skip me"
  command type; revisit in Phase 7 polish if demos care.
- The election restriction protects the **log**, not leader-local reads — `Get`'s
  non-linearizability from Phase 2 is unchanged and still future work.

## What this phase deliberately did NOT change

No wire format changes (RequestVote already carried `last_log_index`/`last_log_term` since
Phase 0), no new goroutines, no storage changes. Safety in Raft is two guards on existing paths —
which is exactly why it's so easy to build a Raft that seems to work without them: nothing fails
until a Figure 8-shaped partition/crash sequence, which is why the deterministic replay test
exists.
