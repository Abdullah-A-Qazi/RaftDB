package raft

// RPC receiver implementations, per Figure 2's "RequestVote RPC" and
// "AppendEntries RPC" boxes plus the "Rules for Servers" that apply to all
// RPCs. Called by the transport layer (gRPC server in package rpc, or the
// in-memory router in tests).

// HandleRequestVote decides whether to vote for a candidate (§5.2).
func (rn *RaftNode) HandleRequestVote(args RequestVoteArgs) RequestVoteReply {
	rn.mu.Lock()
	defer rn.mu.Unlock()

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

	// PHASE 3 will add the election restriction here (§5.4.1): deny the
	// vote unless the candidate's log (args.LastLogTerm/LastLogIndex) is at
	// least as up-to-date as ours. Deferred per the project plan; harmless
	// in Phases 1–2 only because logs are empty/uniform.

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

	// PHASE 2 will add here, in order: the log consistency check on
	// PrevLogIndex/PrevLogTerm (reply false + conflict hints on mismatch),
	// truncation of conflicting suffixes, appending of new entries, and
	// advancing commitIndex to min(LeaderCommit, last new index).
	reply.Success = true
	return reply
}
