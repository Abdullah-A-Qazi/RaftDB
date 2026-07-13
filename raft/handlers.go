package raft

import "fmt"

// RPC receiver implementations, per Figure 2's "RequestVote RPC" and
// "AppendEntries RPC" boxes plus the "Rules for Servers" that apply to all
// RPCs. Called by the transport layer (gRPC server in package rpc, or the
// in-memory router in tests).

// HandleRequestVote decides whether to vote for a candidate (§5.2).
func (rn *RaftNode) HandleRequestVote(args RequestVoteArgs) RequestVoteReply {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	// A stopped instance is a dead process: it must neither answer nor
	// touch disk (a restarted incarnation may own the files now).
	if rn.stopped {
		return RequestVoteReply{Term: rn.currentTerm}
	}

	// Rules for Servers: any RPC with a term above ours moves us to that
	// term as a follower — and clears votedFor, because a vote binds to a
	// specific term and we have never voted in this new one.
	if args.Term > rn.currentTerm {
		rn.becomeFollowerLocked(args.Term)
	}

	reply := RequestVoteReply{Term: rn.currentTerm}

	// Stale candidate: it will see our term in the reply and step down.
	if args.Term < rn.currentTerm {
		return reply
	}

	// --- Election restriction (§5.4.1) ---
	// Deny the vote unless the candidate's log is at least as up-to-date
	// as ours: compare last entry terms first, lengths only as tiebreaker.
	//
	// This single check is what makes elections safe. A committed entry
	// lives on some majority; a candidate needs votes from some majority;
	// any two majorities share at least one node. That shared node has the
	// committed entry, and this check makes it refuse any candidate whose
	// log lacks it — so no leader can ever be elected that is missing a
	// committed entry (Leader Completeness, §5.4.3). Without it, a
	// stale-logged node could win and then "correct" everyone else's logs,
	// erasing acknowledged writes.
	//
	// "Up-to-date" is by (lastLogTerm, lastLogIndex), NOT log length
	// alone: a longer log can be full of uncommitted junk from a deposed
	// leader's term, while a shorter log ending in a higher term provably
	// contains everything committed (higher term = elected later = its
	// history subsumes the committed prefix).
	upToDate := args.LastLogTerm > rn.lastLogTerm() ||
		(args.LastLogTerm == rn.lastLogTerm() && args.LastLogIndex >= rn.lastLogIndex())
	if !upToDate {
		// votedFor stays untouched: we haven't voted, and a better
		// candidate in this same term must still be able to get our vote.
		// Our election timer is NOT reset either — a candidate that
		// cannot win must not suppress our own (viable) candidacy.
		return reply
	}

	// One vote per term: grant iff we haven't voted in this term, or we
	// already voted for this same candidate (making retries idempotent).
	if rn.votedFor == None || rn.votedFor == args.CandidateID {
		rn.votedFor = args.CandidateID
		// Durable BEFORE the reply leaves: if we crash after replying but
		// before persisting, we'd restart with votedFor empty and could
		// grant a second, conflicting vote in the same term.
		rn.persistLocked()
		// Granting a vote defers our own candidacy (Figure 2's timeout
		// rule) — we just endorsed someone; give them time to win.
		rn.resetElectionTimerLocked()
		reply.VoteGranted = true
		rn.logger.Info("granted vote", "candidate", args.CandidateID, "term", args.Term)
	}
	// Note: a denied RequestVote does NOT reset our election timer; see
	// resetElectionTimerLocked for why.
	return reply
}

// HandleAppendEntries processes a leader's heartbeat/replication request
// (§5.2, §5.3). Phase 1 handles only the term/leadership aspects; the log
// consistency check and entry appending arrive in Phase 2.
func (rn *RaftNode) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	// See HandleRequestVote: stopped means dead, and dead nodes don't
	// write to disk.
	if rn.stopped {
		return AppendEntriesReply{Term: rn.currentTerm}
	}

	if args.Term > rn.currentTerm {
		rn.becomeFollowerLocked(args.Term)
	}

	reply := AppendEntriesReply{Term: rn.currentTerm}

	// Stale leader (deposed before a partition healed): reject so it steps
	// down when it reads our term.
	if args.Term < rn.currentTerm {
		return reply
	}

	// Same term as ours and it is the leader's: at most one leader can
	// exist per term (it needed a majority of votes), so if we are a
	// candidate in this term, we lost — defer to it (§5.2). This must NOT
	// clear votedFor (becomeFollowerLocked only clears on term *increase*):
	// we voted for ourselves this term, and forgetting that could let us
	// vote again.
	if rn.state != Follower {
		rn.becomeFollowerLocked(args.Term)
	}
	rn.leaderID = args.LeaderID

	// Hearing from the live leader of our term is exactly what the election
	// timeout waits for.
	rn.resetElectionTimerLocked()

	// --- Log consistency check (§5.3) ---
	// We may only accept these entries if our log matches the leader's at
	// the position just before them. This inductive check is what upholds
	// the Log Matching Property: if two logs agree on (index, term) at one
	// position, they agree on everything before it.
	if args.PrevLogIndex > rn.lastLogIndex() {
		// Our log is too short to even contain the predecessor. Hint the
		// leader to jump straight to our end instead of probing backwards
		// one index per round trip.
		reply.ConflictTerm = 0
		reply.ConflictIndex = rn.lastLogIndex() + 1
		return reply
	}
	if have := rn.termAt(args.PrevLogIndex); have != args.PrevLogTerm {
		// We have an entry there, but from the wrong term. Hint: the term
		// we do have, and our first index of that term — the leader can
		// step over the whole run of bad-term entries at once.
		reply.ConflictTerm = have
		first := args.PrevLogIndex
		for first > 1 && rn.termAt(first-1) == have {
			first--
		}
		reply.ConflictIndex = first
		return reply
	}

	// --- Append (§5.3, Figure 2 receiver steps 3–4) ---
	// Walk the incoming entries; skip everything we already have. We must
	// NOT blindly truncate at PrevLogIndex+1: RPCs can arrive duplicated or
	// out of order, and a stale AppendEntries that only covers an old
	// prefix would otherwise chop off newer entries we already acknowledged
	// — including committed ones. Truncation is allowed only on a genuine
	// term conflict at some index.
	for i, e := range args.Entries {
		if e.Index <= rn.lastLogIndex() {
			if rn.termAt(e.Index) == e.Term {
				continue // already have this exact entry
			}
			// Conflict: our suffix from e.Index on was written by a
			// different (deposed) leader and can never commit — Raft
			// guarantees no committed entry ever conflicts with a current
			// leader's log, so everything we drop here was uncommitted.
			//
			// That guarantee rests on the two §5.4 safety rules (election
			// restriction + current-term commit); assert it, because the
			// only way to get here with a committed entry is a safety bug,
			// and silently erasing acknowledged writes is the one failure
			// mode this system must never shrug off.
			if e.Index <= rn.commitIndex {
				panic(fmt.Sprintf(
					"raft %s: leader %s (term %d) asks to truncate committed entry %d (commitIndex %d) — Leader Completeness violated",
					rn.id, args.LeaderID, args.Term, e.Index, rn.commitIndex))
			}
			rn.truncateLogLocked(e.Index)
		}
		// Durable (WAL + fsync) before the Success reply leaves: the
		// leader will count this follower toward the entries' quorum the
		// moment it reads the reply, and a counted copy must survive
		// kill -9.
		rn.appendLogLocked(args.Entries[i:]...)
		break
	}

	// --- Advance commitIndex (Figure 2 receiver step 5) ---
	// min(leaderCommit, last new entry): leaderCommit may point past the
	// entries this particular request verified; we may only trust our log
	// up to what we just matched against the leader, not beyond.
	if args.LeaderCommit > rn.commitIndex {
		lastNew := args.PrevLogIndex + uint64(len(args.Entries))
		newCommit := min(args.LeaderCommit, lastNew)
		if newCommit > rn.commitIndex {
			rn.commitIndex = newCommit
			rn.applyCond.Signal()
		}
	}

	reply.Success = true
	return reply
}
