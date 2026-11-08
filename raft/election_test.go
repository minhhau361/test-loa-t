package raft

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func testConfig() Configuration {
	return Configuration{OldVoters: []uint64{1, 2, 3, 4, 5}}
}

func TestStartElectionNeedsMajority(t *testing.T) {
	node, err := NewNode(1, testConfig(), NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	request, err := node.StartElection()
	if err != nil {
		t.Fatal(err)
	}
	if request.Term != 1 || request.CandidateID != 1 {
		t.Fatalf("unexpected request: %+v", request)
	}
	role, term, leader := node.State()
	if role != Candidate || term != 1 || leader != 0 {
		t.Fatalf("before quorum: role=%v term=%d leader=%d", role, term, leader)
	}
	for _, peer := range []uint64{2, 3} {
		if err := node.HandleVoteResponse(peer, VoteResponse{Term: 1, VoteGranted: true}); err != nil {
			t.Fatal(err)
		}
	}
	role, term, leader = node.State()
	if role != Leader || term != 1 || leader != 1 {
		t.Fatalf("after majority: role=%v term=%d leader=%d", role, term, leader)
	}
}

func TestNodeVotesAtMostOncePerTermAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node-state.json")
	store, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	node, err := NewNode(1, testConfig(), store)
	if err != nil {
		t.Fatal(err)
	}
	first, err := node.HandleVoteRequest(VoteRequest{
		Term: 1, CandidateID: 2, LastLogIndex: 0, LastLogTerm: 0,
	})
	if err != nil || !first.VoteGranted {
		t.Fatalf("first vote: response=%+v err=%v", first, err)
	}

	restarted, err := NewNode(1, testConfig(), store)
	if err != nil {
		t.Fatal(err)
	}
	second, err := restarted.HandleVoteRequest(VoteRequest{
		Term: 1, CandidateID: 3, LastLogIndex: 0, LastLogTerm: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if second.VoteGranted {
		t.Fatal("node granted two different candidates a vote in the same term")
	}
	if second.Term != 1 {
		t.Fatalf("term changed unexpectedly: %d", second.Term)
	}
}

func TestCandidateNeedsJointMajority(t *testing.T) {
	config := Configuration{
		OldVoters: []uint64{1, 2, 3},
		NewVoters: []uint64{3, 4, 5},
	}
	node, err := NewNode(3, config, NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := node.StartElection(); err != nil {
		t.Fatal(err)
	}
	// 3 and 1 form an old majority, but not a new majority.
	if err := node.HandleVoteResponse(1, VoteResponse{Term: 1, VoteGranted: true}); err != nil {
		t.Fatal(err)
	}
	if role, _, _ := node.State(); role != Candidate {
		t.Fatalf("elected with only an old-configuration majority: %v", role)
	}
	if err := node.HandleVoteResponse(4, VoteResponse{Term: 1, VoteGranted: true}); err != nil {
		t.Fatal(err)
	}
	if role, _, leader := node.State(); role != Leader || leader != 3 {
		t.Fatalf("joint majority did not elect candidate: role=%v leader=%d", role, leader)
	}
}

func TestHigherTermVoteResponseStepsDown(t *testing.T) {
	node, err := NewNode(1, testConfig(), NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := node.StartElection(); err != nil {
		t.Fatal(err)
	}
	if err := node.HandleVoteResponse(2, VoteResponse{Term: 2}); err != nil {
		t.Fatal(err)
	}
	role, term, leader := node.State()
	if role != Follower || term != 2 || leader != 0 {
		t.Fatalf("did not step down: role=%v term=%d leader=%d", role, term, leader)
	}
}

func TestCandidateWithStaleLogIsRejected(t *testing.T) {
	state := PersistentState{
		CurrentTerm: 2,
		Log:         []LogEntry{{Term: 2, Kind: EntrySet, Key: "k", Value: "v"}},
	}
	store := NewMemoryStore()
	if err := store.Save(state); err != nil {
		t.Fatal(err)
	}
	node, err := NewNode(1, testConfig(), store)
	if err != nil {
		t.Fatal(err)
	}
	response, err := node.HandleVoteRequest(VoteRequest{
		Term: 3, CandidateID: 2, LastLogIndex: 0, LastLogTerm: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.VoteGranted {
		t.Fatal("granted vote to candidate with stale log")
	}
	if response.Term != 3 {
		t.Fatalf("expected term 3, got %d", response.Term)
	}
}

func TestRandomizedElectionTimeout(t *testing.T) {
	minimum, maximum := 10*time.Millisecond, 20*time.Millisecond
	for i := 0; i < 100; i++ {
		timeout, err := RandomizedElectionTimeout(minimum, maximum)
		if err != nil {
			t.Fatal(err)
		}
		if timeout < minimum || timeout > maximum {
			t.Fatalf("timeout %s outside [%s, %s]", timeout, minimum, maximum)
		}
	}
	if _, err := RandomizedElectionTimeout(0, maximum); !errors.Is(err, ErrInvalidTimeoutRange) {
		t.Fatalf("expected invalid-range error, got %v", err)
	}
}

func TestFileStoreRejectsCorruptJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "broken.json")
	if err := os.WriteFile(path, []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load(); err == nil {
		t.Fatal("expected corrupt state error")
	}
}
