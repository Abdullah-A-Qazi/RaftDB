// Package kv will contain the key-value state machine that sits on top of
// Raft: an in-memory map mutated only by Apply(command), invoked exclusively
// for committed log entries, in log order.
//
// Implemented in Phase 2. The command wire encoding (Put/Get/Delete) is also
// defined there — Raft itself treats commands as opaque bytes.
package kv
