package storage

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"github.com/Abdullah-A-Qazi/RaftDB/raft"
)

// The write-ahead log: an append-only file of framed records implementing
// raft.LogStore. Every mutation of the Raft log becomes one record:
//
//	append   — one record per entry
//	truncate — one record marking "everything from index N is gone"
//
// Truncation writes a *record* instead of rewriting the file: replay
// applies appends and truncations in order and converges to the same log,
// which keeps every write an O(1) tail append (the only I/O pattern that's
// cheap to make durable). The dead bytes are reclaimed by Phase 5's
// compaction; until then the WAL only grows.
//
// Record framing:
//
//	[4B length | 4B CRC32(payload) | payload (JSON)]
//
// The frame exists for exactly one failure: a crash (kill -9, power cut)
// mid-append leaves a partial record at the tail. On load, the first record
// that is short, oversized, or fails its CRC marks the end of the valid
// log; the file is truncated back to the last good byte and the tail is
// discarded. That is safe BECAUSE of the fsync-before-ack discipline: a
// record that wasn't fully on disk was never acknowledged to the leader or
// a client, so dropping it loses nothing anyone was promised. (Corruption
// in the *middle* of a synced file — disk rot — is indistinguishable from
// a torn tail by this scheme and would silently drop acked entries; that's
// outside our crash-only failure model and flagged in docs/phase-4.)
//
// Payloads are JSON for the same reason as the KV commands: debuggable
// with `xxd`/`jq` while learning, trivially swappable for protobuf later.

const (
	walFile = "wal.log"

	recordAppend   = "append"
	recordTruncate = "truncate"

	// maxRecordSize guards the length prefix against garbage: a torn or
	// corrupt length must not make us allocate gigabytes.
	maxRecordSize = 64 << 20
)

type walRecord struct {
	Type string `json:"type"`

	// For append records ([]byte marshals as base64).
	Term    uint64 `json:"term,omitempty"`
	Index   uint64 `json:"index,omitempty"`
	Command []byte `json:"command,omitempty"`

	// For truncate records: drop every entry with Index >= From.
	From uint64 `json:"from,omitempty"`
}

// openWAL opens (creating if absent) the WAL file for appending.
func openWAL(dir string) (*os.File, error) {
	f, err := os.OpenFile(filepath.Join(dir, walFile), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("storage: opening wal: %w", err)
	}
	return f, nil
}

// AppendEntries implements raft.LogStore: all entries in one batch, one
// fsync. The fsync must complete before this returns — the caller replies
// to the leader (or counts itself toward a quorum) immediately after.
func (s *FileStore) AppendEntries(entries []raft.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	for _, e := range entries {
		rec := walRecord{Type: recordAppend, Term: e.Term, Index: e.Index, Command: e.Command}
		if err := s.writeRecord(rec); err != nil {
			return err
		}
	}
	return s.syncWAL()
}

// TruncateSuffix implements raft.LogStore.
func (s *FileStore) TruncateSuffix(from uint64) error {
	if err := s.writeRecord(walRecord{Type: recordTruncate, From: from}); err != nil {
		return err
	}
	return s.syncWAL()
}

func (s *FileStore) writeRecord(rec walRecord) error {
	payload, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("storage: encoding wal record: %w", err)
	}
	buf := make([]byte, 8+len(payload))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(buf[4:8], crc32.ChecksumIEEE(payload))
	copy(buf[8:], payload)
	if _, err := s.wal.Write(buf); err != nil {
		return fmt.Errorf("storage: writing wal record: %w", err)
	}
	return nil
}

func (s *FileStore) syncWAL() error {
	if err := s.wal.Sync(); err != nil {
		return fmt.Errorf("storage: syncing wal: %w", err)
	}
	return nil
}

// LoadEntries implements raft.LogStore: replay the record stream into the
// resulting log. Stops at the first invalid record (torn tail) and
// truncates the file there so subsequent appends continue from a clean
// boundary.
func (s *FileStore) LoadEntries() ([]raft.LogEntry, error) {
	if _, err := s.wal.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("storage: seeking wal: %w", err)
	}

	var (
		entries []raft.LogEntry
		offset  int64 // end of the last fully valid record
		header  [8]byte
	)
	for {
		if _, err := io.ReadFull(s.wal, header[:]); err != nil {
			// io.EOF: clean end. ErrUnexpectedEOF: torn header — same
			// treatment, the record was never acked.
			break
		}
		length := binary.LittleEndian.Uint32(header[0:4])
		sum := binary.LittleEndian.Uint32(header[4:8])
		if length == 0 || length > maxRecordSize {
			break // garbage length: torn/corrupt tail
		}
		payload := make([]byte, length)
		if _, err := io.ReadFull(s.wal, payload); err != nil {
			break // torn payload
		}
		if crc32.ChecksumIEEE(payload) != sum {
			break // corrupt payload
		}
		var rec walRecord
		if err := json.Unmarshal(payload, &rec); err != nil {
			break // CRC-valid but undecodable: treat as end, same policy
		}

		switch rec.Type {
		case recordAppend:
			// Appends must extend the log contiguously. A gap or overlap
			// here (without a truncate record between) means the WAL
			// itself is inconsistent — refuse to guess. (When no entries
			// are held — a fresh, fully-truncated, or freshly-compacted
			// WAL — any starting index is accepted; raft validates the
			// start against its snapshot boundary on recovery.)
			if want := nextIndex(entries); want != 0 && rec.Index != want {
				return nil, fmt.Errorf(
					"storage: wal append record out of order: got index %d, want %d", rec.Index, want)
			}
			entries = append(entries, raft.LogEntry{Term: rec.Term, Index: rec.Index, Command: rec.Command})
		case recordTruncate:
			if rec.From < 1 {
				return nil, fmt.Errorf("storage: wal truncate record with from=%d", rec.From)
			}
			for len(entries) > 0 && entries[len(entries)-1].Index >= rec.From {
				entries = entries[:len(entries)-1]
			}
		default:
			return nil, fmt.Errorf("storage: unknown wal record type %q", rec.Type)
		}
		offset += int64(8 + length)
	}

	// Chop the torn/garbage tail (no-op when the file ended cleanly) and
	// position the handle for appending.
	if err := s.wal.Truncate(offset); err != nil {
		return nil, fmt.Errorf("storage: truncating torn wal tail: %w", err)
	}
	if _, err := s.wal.Seek(offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("storage: seeking wal end: %w", err)
	}
	return entries, nil
}

func nextIndex(entries []raft.LogEntry) uint64 {
	if len(entries) == 0 {
		return 0 // sentinel: any starting index is acceptable
	}
	return entries[len(entries)-1].Index + 1
}

// Compact implements raft.LogStore: atomically replace the WAL's contents
// with just `entries` — the suffix a snapshot didn't cover. This is where
// the space that the append-only WAL kept accumulating (dead prefixes,
// truncate records) is finally reclaimed. Build-then-rename gives the same
// crash contract as atomicWrite: recovery sees the whole old WAL or the
// whole new one. The caller (raft) guarantees the covering snapshot is
// already durable, so losing the old WAL's prefix can never lose state.
func (s *FileStore) Compact(entries []raft.LogEntry) error {
	tmp, err := os.CreateTemp(s.dir, walFile+".tmp-*")
	if err != nil {
		return fmt.Errorf("storage: creating temp wal: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op after the rename succeeds

	for _, e := range entries {
		rec := walRecord{Type: recordAppend, Term: e.Term, Index: e.Index, Command: e.Command}
		payload, err := json.Marshal(rec)
		if err != nil {
			tmp.Close()
			return fmt.Errorf("storage: encoding wal record: %w", err)
		}
		buf := make([]byte, 8+len(payload))
		binary.LittleEndian.PutUint32(buf[0:4], uint32(len(payload)))
		binary.LittleEndian.PutUint32(buf[4:8], crc32.ChecksumIEEE(payload))
		copy(buf[8:], payload)
		if _, err := tmp.Write(buf); err != nil {
			tmp.Close()
			return fmt.Errorf("storage: writing temp wal: %w", err)
		}
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("storage: syncing temp wal: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("storage: closing temp wal: %w", err)
	}
	if err := os.Rename(tmpName, filepath.Join(s.dir, walFile)); err != nil {
		return fmt.Errorf("storage: swapping wal: %w", err)
	}
	if err := syncDir(s.dir); err != nil {
		return fmt.Errorf("storage: syncing dir: %w", err)
	}

	// The old handle still points at the unlinked pre-compaction file;
	// swap it for the new one, positioned at the end for appends.
	old := s.wal
	fresh, err := openWAL(s.dir)
	if err != nil {
		return err
	}
	if _, err := fresh.Seek(0, io.SeekEnd); err != nil {
		fresh.Close()
		return fmt.Errorf("storage: seeking new wal: %w", err)
	}
	s.wal = fresh
	if old != nil {
		old.Close()
	}
	return nil
}

// Close releases the WAL file handle.
func (s *FileStore) Close() error {
	if s.wal == nil {
		return nil
	}
	err := s.wal.Close()
	s.wal = nil
	if err != nil && !errors.Is(err, os.ErrClosed) {
		return fmt.Errorf("storage: closing wal: %w", err)
	}
	return nil
}
