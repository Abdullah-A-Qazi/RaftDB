package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Abdullah-A-Qazi/RaftDB/raft"
)

func TestLoadOnFreshStoreFindsNothing(t *testing.T) {
	s, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	_, found, err := s.LoadHardState()
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("fresh store claims to have hard state")
	}
}

func TestSaveLoadRoundTrip(t *testing.T) {
	s, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	in := raft.HardState{CurrentTerm: 42, VotedFor: "node3"}
	if err := s.SaveHardState(in); err != nil {
		t.Fatal(err)
	}
	out, found, err := s.LoadHardState()
	if err != nil {
		t.Fatal(err)
	}
	if !found || out != in {
		t.Fatalf("loaded %+v (found=%v), want %+v", out, found, in)
	}
}

func TestSaveOverwrites(t *testing.T) {
	s, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SaveHardState(raft.HardState{CurrentTerm: 1, VotedFor: "node1"}); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveHardState(raft.HardState{CurrentTerm: 2, VotedFor: raft.None}); err != nil {
		t.Fatal(err)
	}
	out, _, err := s.LoadHardState()
	if err != nil {
		t.Fatal(err)
	}
	if out.CurrentTerm != 2 || out.VotedFor != raft.None {
		t.Fatalf("loaded %+v, want term 2 / no vote", out)
	}
}

// The state must be readable by a brand-new FileStore instance — that is
// exactly what a process restart is.
func TestStateSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	s1, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	in := raft.HardState{CurrentTerm: 7, VotedFor: "node2"}
	if err := s1.SaveHardState(in); err != nil {
		t.Fatal(err)
	}

	s2, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	out, found, err := s2.LoadHardState()
	if err != nil {
		t.Fatal(err)
	}
	if !found || out != in {
		t.Fatalf("after reopen: loaded %+v (found=%v), want %+v", out, found, in)
	}
}

func TestCorruptFileIsAnErrorNotAFreshStart(t *testing.T) {
	dir := t.TempDir()
	s, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, hardStateFile), []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Treating corruption as "no state" would silently re-enable double
	// voting; it must surface loudly instead.
	if _, _, err := s.LoadHardState(); err == nil {
		t.Fatal("corrupt hard state loaded without error")
	}
}

func TestNewFileStoreCreatesNestedDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "a", "b", "node1")
	if _, err := NewFileStore(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatal(err)
	}
}

// No stray temp files may accumulate after successful saves.
func TestNoTempFileLitter(t *testing.T) {
	dir := t.TempDir()
	s, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 10 {
		if err := s.SaveHardState(raft.HardState{CurrentTerm: uint64(i)}); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Name() != hardStateFile && e.Name() != walFile {
			t.Fatalf("unexpected file %q in store dir (temp-file litter?)", e.Name())
		}
	}
}
