package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/Abdullah-A-Qazi/RaftDB/raft"
)

func mustStore(t *testing.T, dir string) *FileStore {
	t.Helper()
	s, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func entry(term, index uint64, cmd string) raft.LogEntry {
	return raft.LogEntry{Term: term, Index: index, Command: []byte(cmd)}
}

func TestWALEmpty(t *testing.T) {
	s := mustStore(t, t.TempDir())
	entries, err := s.LoadEntries()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("fresh WAL loaded %d entries", len(entries))
	}
}

func TestWALAppendLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s := mustStore(t, dir)
	want := []raft.LogEntry{
		entry(1, 1, "a"),
		entry(1, 2, `{"binary-ish": "\x00\xff"}`),
		entry(3, 3, ""),
	}
	if err := s.AppendEntries(want); err != nil {
		t.Fatal(err)
	}
	s.Close()

	// A reopened store (= process restart) must see everything.
	s2 := mustStore(t, dir)
	got, err := s2.LoadEntries()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("loaded %d entries, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Term != want[i].Term || got[i].Index != want[i].Index || string(got[i].Command) != string(want[i].Command) {
			t.Fatalf("entry %d: got %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestWALTruncateReplay(t *testing.T) {
	dir := t.TempDir()
	s := mustStore(t, dir)
	if err := s.AppendEntries([]raft.LogEntry{
		entry(1, 1, "a"), entry(1, 2, "b"), entry(1, 3, "c"),
	}); err != nil {
		t.Fatal(err)
	}
	// Conflict: drop 2–3, replace with a term-2 entry at 2.
	if err := s.TruncateSuffix(2); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendEntries([]raft.LogEntry{entry(2, 2, "b2")}); err != nil {
		t.Fatal(err)
	}
	s.Close()

	got, err := mustStore(t, dir).LoadEntries()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("loaded %d entries, want 2", len(got))
	}
	if got[1].Term != 2 || string(got[1].Command) != "b2" {
		t.Fatalf("entry at index 2 = %+v, want the term-2 replacement", got[1])
	}
}

// Reopen, load, append more: the append position must continue from the
// valid end.
func TestWALAppendAfterReload(t *testing.T) {
	dir := t.TempDir()
	s := mustStore(t, dir)
	if err := s.AppendEntries([]raft.LogEntry{entry(1, 1, "a")}); err != nil {
		t.Fatal(err)
	}
	s.Close()

	s2 := mustStore(t, dir)
	if _, err := s2.LoadEntries(); err != nil {
		t.Fatal(err)
	}
	if err := s2.AppendEntries([]raft.LogEntry{entry(1, 2, "b")}); err != nil {
		t.Fatal(err)
	}
	s2.Close()

	got, err := mustStore(t, dir).LoadEntries()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || string(got[1].Command) != "b" {
		t.Fatalf("after reload+append: %+v", got)
	}
}

// The kill -9 case: chop the file at EVERY byte offset inside the last
// record and confirm recovery yields exactly the entries before it, then
// keeps working for new appends.
func TestWALTornTailAtEveryOffset(t *testing.T) {
	// Build a reference WAL with 3 entries and remember the size after 2.
	dir := t.TempDir()
	s := mustStore(t, dir)
	if err := s.AppendEntries([]raft.LogEntry{entry(1, 1, "aa"), entry(1, 2, "bb")}); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(filepath.Join(dir, walFile))
	if err != nil {
		t.Fatal(err)
	}
	twoEntries := fi.Size()
	if err := s.AppendEntries([]raft.LogEntry{entry(2, 3, "cc")}); err != nil {
		t.Fatal(err)
	}
	fi, err = os.Stat(filepath.Join(dir, walFile))
	if err != nil {
		t.Fatal(err)
	}
	full := fi.Size()
	s.Close()
	ref, err := os.ReadFile(filepath.Join(dir, walFile))
	if err != nil {
		t.Fatal(err)
	}

	for cut := twoEntries + 1; cut < full; cut++ {
		t.Run(fmt.Sprintf("cut@%d", cut), func(t *testing.T) {
			d := t.TempDir()
			if err := os.WriteFile(filepath.Join(d, walFile), ref[:cut], 0o644); err != nil {
				t.Fatal(err)
			}
			s := mustStore(t, d)
			got, err := s.LoadEntries()
			if err != nil {
				t.Fatalf("torn tail must recover, got error: %v", err)
			}
			if len(got) != 2 {
				t.Fatalf("recovered %d entries, want 2 (the fully synced ones)", len(got))
			}
			// The WAL must be usable immediately after recovery.
			if err := s.AppendEntries([]raft.LogEntry{entry(2, 3, "cc-retry")}); err != nil {
				t.Fatal(err)
			}
			s.Close()
			got, err = mustStore(t, d).LoadEntries()
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != 3 || string(got[2].Command) != "cc-retry" {
				t.Fatalf("append after torn-tail recovery: %+v", got)
			}
		})
	}
}

// A flipped bit in the tail record's payload must fail its CRC and be
// treated as the end of the log.
func TestWALCorruptTailDropped(t *testing.T) {
	dir := t.TempDir()
	s := mustStore(t, dir)
	if err := s.AppendEntries([]raft.LogEntry{entry(1, 1, "aa"), entry(1, 2, "bb")}); err != nil {
		t.Fatal(err)
	}
	s.Close()

	path := filepath.Join(dir, walFile)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-3] ^= 0xFF // flip a bit inside the last record's payload
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}

	got, err := mustStore(t, dir).LoadEntries()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Index != 1 {
		t.Fatalf("recovered %+v, want just entry 1", got)
	}
}

// Non-contiguous appends (a store-level inconsistency, not a torn write)
// must refuse to load rather than hand raft a log with holes.
func TestWALRejectsNonContiguousLog(t *testing.T) {
	dir := t.TempDir()
	s := mustStore(t, dir)
	if err := s.AppendEntries([]raft.LogEntry{entry(1, 1, "a"), entry(1, 5, "gap")}); err != nil {
		t.Fatal(err)
	}
	s.Close()

	if _, err := mustStore(t, dir).LoadEntries(); err == nil {
		t.Fatal("loaded a log with an index gap")
	}
}
