package kv

import (
	"fmt"
	"sync"
)

// Store is the replicated state machine: a map mutated exclusively by
// apply(), which the Raft applier invokes only for committed entries, in
// log order. Determinism is the contract that matters: every node applies
// the identical command sequence, so identical maps fall out — apply must
// therefore never consult anything but the command and the current map (no
// clocks, no randomness, no node identity).
type Store struct {
	// mu exists for readers: apply calls are already serialized by Raft's
	// single applier goroutine, but Get (leader-local reads) and test
	// assertions arrive from other goroutines concurrently.
	mu sync.RWMutex
	m  map[string]string
}

func NewStore() *Store {
	return &Store{m: make(map[string]string)}
}

// apply executes one committed command.
func (s *Store) apply(cmd Command) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch cmd.Op {
	case OpPut:
		s.m[cmd.Key] = cmd.Value
	case OpDelete:
		delete(s.m, cmd.Key)
	default:
		// DecodeCommand already rejects unknown ops; reaching here means a
		// programming error, not bad input.
		return fmt.Errorf("kv: unapplicable op %q", cmd.Op)
	}
	return nil
}

// Get reads a key from local state. See Server.Get for the consistency
// caveats of serving this without consulting the log.
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.m[key]
	return v, ok
}

// Len reports the number of keys (for tests and the Phase 7 dashboard).
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.m)
}

// Snapshot returns a copy of the entire map (test helper; Phase 5 will
// grow this into real snapshot serialization).
func (s *Store) Snapshot() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]string, len(s.m))
	for k, v := range s.m {
		out[k] = v
	}
	return out
}
