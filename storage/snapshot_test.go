package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Abdullah-A-Qazi/RaftDB/raft"
)

func TestSnapshotLoadOnFreshStore(t *testing.T) {
	s := mustStore(t, t.TempDir())
	_, found, err := s.LoadSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("fresh store claims to have a snapshot")
	}
}

func TestSnapshotRoundTripAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	s := mustStore(t, dir)
	in := raft.Snapshot{LastIncludedIndex: 42, LastIncludedTerm: 7, Data: []byte(`{"k":"v"}`)}
	if err := s.SaveSnapshot(in); err != nil {
		t.Fatal(err)
	}
	s.Close()

	out, found, err := mustStore(t, dir).LoadSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if !found || out.LastIncludedIndex != 42 || out.LastIncludedTerm != 7 || string(out.Data) != `{"k":"v"}` {
		t.Fatalf("loaded %+v (found=%v), want %+v", out, found, in)
	}
}

func TestSnapshotOverwriteKeepsOnlyLatest(t *testing.T) {
	s := mustStore(t, t.TempDir())
	if err := s.SaveSnapshot(raft.Snapshot{LastIncludedIndex: 10, LastIncludedTerm: 1, Data: []byte("old")}); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveSnapshot(raft.Snapshot{LastIncludedIndex: 20, LastIncludedTerm: 2, Data: []byte("new")}); err != nil {
		t.Fatal(err)
	}
	out, _, err := s.LoadSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if out.LastIncludedIndex != 20 || string(out.Data) != "new" {
		t.Fatalf("loaded %+v, want the newer snapshot", out)
	}
}

func TestWALCompactReclaimsSpaceAndReloadsSuffix(t *testing.T) {
	dir := t.TempDir()
	s := mustStore(t, dir)

	var all []raft.LogEntry
	for i := uint64(1); i <= 50; i++ {
		all = append(all, entry(1, i, "some-command-payload-to-give-the-file-real-size"))
	}
	if err := s.AppendEntries(all); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(filepath.Join(dir, walFile))
	if err != nil {
		t.Fatal(err)
	}

	// Snapshot covered entries 1–48; keep only the 2-entry suffix.
	if err := s.Compact(all[48:]); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(filepath.Join(dir, walFile))
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() >= before.Size() {
		t.Fatalf("wal grew from %d to %d bytes across compaction — no space reclaimed",
			before.Size(), after.Size())
	}

	// The swapped-in handle keeps working for appends...
	if err := s.AppendEntries([]raft.LogEntry{entry(2, 51, "post-compact")}); err != nil {
		t.Fatal(err)
	}
	s.Close()

	// ...and a reopen (process restart) sees suffix + new entries only.
	got, err := mustStore(t, dir).LoadEntries()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0].Index != 49 || got[2].Index != 51 {
		t.Fatalf("reloaded %d entries starting at %d, want 3 starting at 49", len(got), got[0].Index)
	}
	if string(got[2].Command) != "post-compact" {
		t.Fatalf("last entry = %+v, want the post-compaction append", got[2])
	}
}

func TestWALCompactToEmpty(t *testing.T) {
	dir := t.TempDir()
	s := mustStore(t, dir)
	if err := s.AppendEntries([]raft.LogEntry{entry(1, 1, "a"), entry(1, 2, "b")}); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(nil); err != nil {
		t.Fatal(err)
	}
	// Appends after a compact-to-empty start wherever raft says they do.
	if err := s.AppendEntries([]raft.LogEntry{entry(2, 3, "c")}); err != nil {
		t.Fatal(err)
	}
	s.Close()

	got, err := mustStore(t, dir).LoadEntries()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Index != 3 {
		t.Fatalf("reloaded %+v, want just entry 3", got)
	}
}
