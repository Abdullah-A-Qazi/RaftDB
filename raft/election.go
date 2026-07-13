package raft

import (
	"context"
	"time"
)

// tickInterval is how often the election ticker wakes to check the timer.
// It only needs to be comfortably finer than the election timeout; 10ms
// against a >=100ms timeout adds at most ~10% jitter to when an election
// actually fires, which the randomized timeout already dwarfs.
const tickInterval = 10 * time.Millisecond

// runElectionTicker is the node's one long-lived goroutine: it watches the
// election timer and starts an election when a non-leader has heard nothing
// for a full (randomized) timeout. Leaders don't run election timers — they
// only step down when they see a higher term.
func (rn *RaftNode) runElectionTicker() {
	defer rn.wg.Done()
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-rn.stopCh:
			return
		case <-ticker.C:
		}
		rn.mu.Lock()
		if rn.state != Leader && time.Since(rn.electionResetAt) >= rn.electionTimeout {
			rn.startElectionLocked()
		}
		rn.mu.Unlock()
	}
}

// startElectionLocked converts this node to a candidate and solicits votes
// (§5.2): increment currentTerm, vote for self, reset the election timer,
// send RequestVote to every peer in parallel.
//
// Callers must hold rn.mu.
func (rn *RaftNode) startElectionLocked() {
	rn.state = Candidate
	rn.currentTerm++
	rn.votedFor = rn.id
	rn.leaderID = None
	// The self-vote must be durable before any RequestVote goes out: peers
	// will count it (via our candidacy), so forgetting it in a crash could
	// let a restarted us vote for someone else in this same term.
	rn.persistLocked()
	// Fresh randomized timeout: if this election splits, we and the other
	// candidates retry after *different* waits, which is what breaks the
	// split-vote livelock (§5.2).
	rn.resetElectionTimerLocked()

	term := rn.currentTerm
	args := RequestVoteArgs{
		Term:         term,
		CandidateID:  rn.id,
		LastLogIndex: rn.lastLogIndex(),
		LastLogTerm:  rn.lastLogTerm(),
	}
	rn.logger.Info("starting election", "term", term)

	// votes is shared by the reply goroutines below and guarded by rn.mu.
	// It starts at 1: our own (persisted) vote.
	votes := 1
	if votes >= rn.quorum() {
		// Single-node cluster: the self-vote already is a majority, and
		// with no peers no reply would ever arrive to trigger the win.
		rn.becomeLeaderLocked()
		return
	}

	for _, peer := range rn.peers {
		go func(peer string) {
			ctx, cancel := context.WithTimeout(context.Background(), rn.cfg.RPCTimeout)
			defer cancel()
			reply, err := rn.cfg.Transport.RequestVote(ctx, peer, args)
			if err != nil {
				// Unreachable peer simply contributes no vote. No
				// per-RPC retry: if this round fails to reach quorum
				// the election timer fires again and a new election
				// (new term) retries everything.
				return
			}

			rn.mu.Lock()
			defer rn.mu.Unlock()
			// Fire-and-forget reply goroutines can outlive Stop; a
			// stopped instance must not mutate state or disk.
			if rn.stopped {
				return
			}
			if reply.Term > rn.currentTerm {
				// Someone is ahead of us; our candidacy is void (§5.1).
				rn.becomeFollowerLocked(reply.Term)
				return
			}
			// Guard against stale replies ("term confusion"): between
			// sending and receiving we may have started a newer election,
			// stepped down, or already won. A vote granted for term N must
			// only ever count toward the election for term N.
			if rn.state != Candidate || rn.currentTerm != term {
				return
			}
			if reply.VoteGranted {
				votes++
				if votes == rn.quorum() {
					// == rather than >= so late-arriving votes don't
					// re-trigger the win path.
					rn.becomeLeaderLocked()
				}
			}
		}(peer)
	}
}

// becomeLeaderLocked transitions a candidate that just reached quorum into
// the leader role and starts the heartbeat loop.
//
// Callers must hold rn.mu.
func (rn *RaftNode) becomeLeaderLocked() {
	rn.state = Leader
	rn.leaderID = rn.id

	// Figure 2: nextIndex starts optimistic (leader's last log index + 1),
	// matchIndex pessimistic (0, nothing confirmed). Reinitialized on every
	// election win because they are guesses about followers' logs, and a
	// new term means those guesses must be re-established.
	rn.nextIndex = make(map[string]uint64, len(rn.peers))
	rn.matchIndex = make(map[string]uint64, len(rn.peers))
	for _, peer := range rn.peers {
		rn.nextIndex[peer] = rn.lastLogIndex() + 1
		rn.matchIndex[peer] = 0
	}

	rn.logger.Info("won election", "term", rn.currentTerm)
	rn.wg.Add(1)
	go rn.runReplication(rn.currentTerm)
}
