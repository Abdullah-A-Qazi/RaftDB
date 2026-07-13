package raft

import (
	"context"
	"time"
)

// Log replication (§5.3), leader side. One goroutine per leadership term
// runs runReplication; each round sends every peer a tailored AppendEntries
// carrying the entries that peer is missing (an empty one — a heartbeat —
// when it is caught up). Replication and heartbeats are deliberately the
// same code path: a heartbeat is just replication with nothing to send, and
// unifying them means the consistency-check/back-off machinery is exercised
// on every tick, not only when writes happen.

// runReplication drives rounds until this node stops being leader of
// `term`. A round fires every HeartbeatInterval, or immediately when
// triggerCh is kicked (new proposal, or a rejection that moved nextIndex).
// The immediate first round announces the election win and resets follower
// timers before any of them can start a competing election.
func (rn *RaftNode) runReplication(term uint64) {
	defer rn.wg.Done()
	ticker := time.NewTicker(rn.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		if !rn.replicationRound(term) {
			return
		}
		select {
		case <-rn.stopCh:
			return
		case <-ticker.C:
		case <-rn.triggerCh:
		}
	}
}

// replicationRound sends one AppendEntries to every peer. Returns false
// when this node is no longer leader of `term` and the loop should exit.
func (rn *RaftNode) replicationRound(term uint64) bool {
	type outbound struct {
		peer string
		args AppendEntriesArgs
	}

	rn.mu.Lock()
	if rn.state != Leader || rn.currentTerm != term {
		rn.mu.Unlock()
		return false
	}
	msgs := make([]outbound, 0, len(rn.peers))
	for _, peer := range rn.peers {
		next := rn.nextIndex[peer]
		prevIndex := next - 1 // >= 0 always: nextIndex is never below 1
		msgs = append(msgs, outbound{peer: peer, args: AppendEntriesArgs{
			Term:         term,
			LeaderID:     rn.id,
			PrevLogIndex: prevIndex,
			// prevIndex can't exceed our last index: nextIndex starts at
			// last+1 and only moves down on rejection or up to match+1.
			PrevLogTerm:  rn.termAt(prevIndex),
			Entries:      rn.entriesFrom(next),
			LeaderCommit: rn.commitIndex,
		}})
	}
	rn.mu.Unlock()

	for _, m := range msgs {
		go func(peer string, args AppendEntriesArgs) {
			ctx, cancel := context.WithTimeout(context.Background(), rn.cfg.RPCTimeout)
			defer cancel()
			reply, err := rn.cfg.Transport.AppendEntries(ctx, peer, args)
			if err != nil {
				return // unreachable follower; next round retries
			}
			rn.handleAppendEntriesReply(peer, term, args, reply)
		}(m.peer, m.args)
	}
	return true
}

// handleAppendEntriesReply digests one follower's response.
func (rn *RaftNode) handleAppendEntriesReply(peer string, term uint64, args AppendEntriesArgs, reply AppendEntriesReply) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if reply.Term > rn.currentTerm {
		// A partition healed or we were deposed: someone out there has a
		// higher term, so we are not a legitimate leader anymore (§5.1).
		rn.becomeFollowerLocked(reply.Term)
		return
	}
	// Stale-reply guard: this response belongs to a request we sent as
	// leader of `term`. If we since stepped down and won again, our
	// nextIndex/matchIndex were reinitialized and this reply describes a
	// conversation from a previous life.
	if rn.state != Leader || rn.currentTerm != term {
		return
	}

	if reply.Success {
		// The follower now matches us up through the entries we sent.
		// Compute from the *request* (prevLogIndex + len(entries)), never
		// from our current log length — more entries may have been
		// appended since this request was built.
		matched := args.PrevLogIndex + uint64(len(args.Entries))
		// max(), because replies arrive out of order: a slow reply for an
		// old short prefix must not drag matchIndex backwards below what a
		// newer reply already confirmed.
		if matched > rn.matchIndex[peer] {
			rn.matchIndex[peer] = matched
			rn.nextIndex[peer] = matched + 1
			rn.advanceCommitIndexLocked()
		}
		return
	}

	// Rejected: the consistency check failed, so our guess of where the
	// follower's log matches ours (nextIndex) is too high. Back off using
	// the follower's conflict hints (§5.3's fast backtracking) and retry.
	next := nextIndexAfterRejection(rn.log, args.PrevLogIndex, reply)
	if next < rn.nextIndex[peer] { // never move forward on a rejection
		rn.nextIndex[peer] = next
	}
	// Retry immediately rather than waiting out the heartbeat interval —
	// a far-behind follower would otherwise take one interval per hop.
	select {
	case rn.triggerCh <- struct{}{}:
	default:
	}
}

// nextIndexAfterRejection computes where the leader should retry after a
// follower rejected (prevLogIndex, prevLogTerm). With no hints this is the
// paper's decrement-by-one; the hints let us skip whole terms per round
// trip instead:
//
//   - Follower's log is too short (ConflictTerm == 0): jump straight to its
//     end (ConflictIndex = follower's lastIndex+1).
//   - Follower has a conflicting term at prevLogIndex: if the leader also
//     has entries of ConflictTerm, retry just past its last one (the logs
//     can only diverge after that); if the leader has none, the whole term
//     is bogus on the follower — retry at the follower's first index for it
//     (ConflictIndex), wiping the term in one shot.
//
// Pure function (operates on the log slice, not the node) so it can be unit
// tested without a running node.
func nextIndexAfterRejection(log []LogEntry, prevLogIndex uint64, reply AppendEntriesReply) uint64 {
	if reply.ConflictTerm == 0 {
		if reply.ConflictIndex >= 1 {
			return reply.ConflictIndex
		}
		// Defensive: a rejection with no usable hints (shouldn't happen
		// with our follower) degrades to the paper's decrement-by-one.
		if prevLogIndex >= 1 {
			return prevLogIndex
		}
		return 1
	}
	// Scan for the leader's last entry of ConflictTerm. Linear, backwards:
	// fine at this scale, and terms cluster at the tail.
	for i := len(log) - 1; i >= 1; i-- {
		if log[i].Term == reply.ConflictTerm {
			return log[i].Index + 1
		}
		if log[i].Term < reply.ConflictTerm {
			break // terms only decrease going left; it's not here
		}
	}
	return reply.ConflictIndex
}

// advanceCommitIndexLocked implements the leader commit rule: find the
// highest N > commitIndex that is replicated on a majority (matchIndex for
// peers, our own log implicitly for ourselves) AND belongs to the current
// term, and commit through it.
//
// The current-term clause (§5.4.2) is the Figure 8 rule, and it is easy to
// get wrong because majority replication *feels* like enough. It isn't:
// an old-term entry on a majority can still be overwritten, because a
// candidate whose log ends in a *higher* term wins elections against
// majority-holders of the old entry (the election restriction compares
// terms before lengths). Committing it would acknowledge a write that a
// legal future leader then erases. An entry of the CURRENT term on a
// majority is different: any future winner must out-term that majority,
// which forces its log to already contain the entry.
//
// Old-term entries are therefore only ever committed *indirectly*: when a
// current-term entry above them commits, commitIndex jumps over them (Log
// Matching guarantees everything below a replicated entry matches too).
// Consequence, flagged as a liveness quirk in docs/phase-3: a fresh leader
// with inherited uncommitted entries cannot commit them until its first
// own-term entry arrives (the paper's fix — an empty no-op entry on
// election win — is future work; our KV traffic unblocks it naturally).
//
// Callers must hold rn.mu.
func (rn *RaftNode) advanceCommitIndexLocked() {
	if rn.state != Leader {
		return
	}
	for n := rn.lastLogIndex(); n > rn.commitIndex; n-- {
		if rn.termAt(n) != rn.currentTerm {
			// Terms only decrease toward the log's head, and no entry can
			// carry a term above ours (we'd have stepped down on seeing
			// it) — so everything at n and below is from older terms and
			// not directly committable. Stop.
			break
		}
		count := 1 // ourselves: our log always contains our own entry at n
		for _, peer := range rn.peers {
			if rn.matchIndex[peer] >= n {
				count++
			}
		}
		if count >= rn.quorum() {
			rn.commitIndex = n
			rn.applyCond.Signal()
			// Followers learn the new commitIndex on the next round; no
			// need to force one — commit latency for *clients* is decided
			// by the leader's own apply, which we just signaled.
			break
		}
	}
}

// runApplier is the single goroutine that feeds committed entries to the
// state machine, in order. Single, so that "exactly once, in log order" is
// enforced by construction rather than by locking discipline in the state
// machine.
func (rn *RaftNode) runApplier() {
	defer rn.wg.Done()
	rn.mu.Lock()
	defer rn.mu.Unlock()
	for {
		for rn.lastApplied >= rn.commitIndex && !rn.stopped {
			rn.applyCond.Wait()
		}
		if rn.stopped {
			return
		}
		// Snapshot the batch and release the lock while applying: Apply
		// runs user code (the KV store, waiter callbacks) that must not be
		// able to deadlock raft, and RPC handlers must not stall behind it.
		batch := rn.entriesFrom(rn.lastApplied + 1)
		batch = batch[:rn.commitIndex-rn.lastApplied] // only committed ones
		rn.mu.Unlock()
		for _, e := range batch {
			if rn.stateMachine != nil {
				rn.stateMachine.Apply(e)
			}
		}
		rn.mu.Lock()
		// Safe against races because only this goroutine writes
		// lastApplied — commitIndex may have moved on, the loop re-checks.
		rn.lastApplied += uint64(len(batch))
	}
}
