package raft

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

var (
	ErrInvalidNodeID       = errors.New("raft: node id must be non-zero")
	ErrNotVoter            = errors.New("raft: node is not a voter")
	ErrInvalidTimeoutRange = errors.New("raft: invalid election timeout range")
)

// Node is the consensus state for one Raft member. All mutable protocol state
// is protected by mu; persistent fields are saved before a response is returned.
// Transport and the timer loop are intentionally separate so they can be tested
// deterministically and attached to the virtual network in a later increment.
type Node struct {
	mu sync.Mutex

	id     uint64
	role   Role
	leader uint64

	currentTerm uint64
	votedFor    uint64
	hasVoted    bool
	log         []LogEntry

	configuration Configuration
	store         StableStore
	votes         map[uint64]bool
}

// NewNode loads durable Raft state and creates a node that starts as a follower.
func NewNode(id uint64, configuration Configuration, store StableStore) (*Node, error) {
	if id == 0 {
		return nil, ErrInvalidNodeID
	}
	if !configuration.HasVoter(id) {
		return nil, ErrNotVoter
	}
	if store == nil {
		return nil, errors.New("raft: stable store is required")
	}
	state, err := store.Load()
	if err != nil {
		return nil, err
	}
	if err := validatePersistentState(state); err != nil {
		return nil, err
	}
	return &Node{
		id:            id,
		role:          Follower,
		currentTerm:   state.CurrentTerm,
		votedFor:      state.VotedFor,
		hasVoted:      state.HasVoted,
		log:           clonePersistentState(state).Log,
		configuration: cloneConfiguration(configuration),
		store:         store,
	}, nil
}

// StartElection advances the term, durably votes for self, and returns the
// RequestVote message to broadcast. The caller must deliver it to the other
// voters and pass each response to HandleVoteResponse.
func (n *Node) StartElection() (VoteRequest, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role == Leader {
		return VoteRequest{}, errors.New("raft: leader cannot start an election")
	}
	if !n.configuration.HasVoter(n.id) {
		return VoteRequest{}, ErrNotVoter
	}

	oldTerm, oldVote, oldHasVote, oldRole, oldLeader := n.currentTerm, n.votedFor, n.hasVoted, n.role, n.leader
	n.currentTerm++
	n.role = Candidate
	n.leader = 0
	n.votedFor = n.id
	n.hasVoted = true
	if err := n.persistLocked(); err != nil {
		n.currentTerm, n.votedFor, n.hasVoted, n.role, n.leader = oldTerm, oldVote, oldHasVote, oldRole, oldLeader
		return VoteRequest{}, err
	}

	n.votes = map[uint64]bool{n.id: true}
	if n.configuration.HasQuorum(n.votes) {
		n.role = Leader
		n.leader = n.id
	}
	lastIndex, lastTerm := uint64(len(n.log)), uint64(0)
	if lastIndex != 0 {
		lastTerm = n.log[lastIndex-1].Term
	}
	return VoteRequest{
		Term: n.currentTerm, CandidateID: n.id,
		LastLogIndex: lastIndex, LastLogTerm: lastTerm,
	}, nil
}

// HandleVoteRequest applies Raft's term and log-freshness rules. A granted vote
// is persisted before it is returned, so a restart cannot grant a second vote
// in the same term.
func (n *Node) HandleVoteRequest(request VoteRequest) (VoteResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if request.CandidateID == 0 {
		return VoteResponse{Term: n.currentTerm}, ErrInvalidNodeID
	}
	if request.Term < n.currentTerm {
		return VoteResponse{Term: n.currentTerm}, nil
	}

	oldTerm, oldVote, oldHasVote, oldRole, oldLeader := n.currentTerm, n.votedFor, n.hasVoted, n.role, n.leader
	if request.Term > n.currentTerm {
		n.currentTerm = request.Term
		n.votedFor = 0
		n.hasVoted = false
		n.role = Follower
		n.leader = 0
		n.votes = nil
	}

	lastIndex, lastTerm := uint64(len(n.log)), uint64(0)
	if lastIndex != 0 {
		lastTerm = n.log[lastIndex-1].Term
	}
	upToDate := request.LastLogTerm > lastTerm ||
		(request.LastLogTerm == lastTerm && request.LastLogIndex >= lastIndex)
	canVote := !n.hasVoted || (n.votedFor == request.CandidateID)
	grant := n.configuration.HasVoter(request.CandidateID) && canVote && upToDate
	if grant {
		n.votedFor = request.CandidateID
		n.hasVoted = true
		n.role = Follower
		n.leader = 0
	}

	if n.currentTerm != oldTerm || grant {
		if err := n.persistLocked(); err != nil {
			n.currentTerm, n.votedFor, n.hasVoted, n.role, n.leader = oldTerm, oldVote, oldHasVote, oldRole, oldLeader
			return VoteResponse{Term: oldTerm}, err
		}
	}
	return VoteResponse{Term: n.currentTerm, VoteGranted: grant}, nil
}

// HandleVoteResponse records one peer's response. A higher response term causes
// immediate step-down; reaching the configured quorum elects this candidate.
func (n *Node) HandleVoteResponse(peerID uint64, response VoteResponse) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if peerID == 0 || !n.configuration.HasVoter(peerID) {
		return ErrNotVoter
	}
	if response.Term > n.currentTerm {
		oldTerm, oldVote, oldHasVote, oldRole, oldLeader := n.currentTerm, n.votedFor, n.hasVoted, n.role, n.leader
		n.currentTerm = response.Term
		n.votedFor = 0
		n.hasVoted = false
		n.role = Follower
		n.leader = 0
		n.votes = nil
		if err := n.persistLocked(); err != nil {
			n.currentTerm, n.votedFor, n.hasVoted, n.role, n.leader = oldTerm, oldVote, oldHasVote, oldRole, oldLeader
			return err
		}
		return nil
	}
	if n.role != Candidate || response.Term != n.currentTerm || !response.VoteGranted {
		return nil
	}
	if n.votes == nil {
		n.votes = make(map[uint64]bool)
	}
	n.votes[peerID] = true
	if n.configuration.HasQuorum(n.votes) {
		n.role = Leader
		n.leader = n.id
	}
	return nil
}

// RandomizedElectionTimeout returns a timeout uniformly selected in [minimum,
// maximum]. Use different ranges or the shared process RNG to avoid synchronized
// campaigns; the node timer loop will consume this in the transport increment.
func RandomizedElectionTimeout(minimum, maximum time.Duration) (time.Duration, error) {
	if minimum <= 0 || maximum < minimum {
		return 0, ErrInvalidTimeoutRange
	}
	span := int64(maximum-minimum) + 1
	return minimum + time.Duration(rand.Int64N(span)), nil
}

// State returns a consistent, read-only view of role, term, and known leader.
func (n *Node) State() (role Role, term, leaderID uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role, n.currentTerm, n.leader
}

func (n *Node) persistLocked() error {
	state := PersistentState{
		CurrentTerm: n.currentTerm,
		VotedFor:    n.votedFor,
		HasVoted:    n.hasVoted,
		Log:         n.log,
	}
	if err := n.store.Save(state); err != nil {
		return fmt.Errorf("raft: persist node %d: %w", n.id, err)
	}
	return nil
}

func cloneConfiguration(configuration Configuration) Configuration {
	return Configuration{
		OldVoters: append([]uint64(nil), configuration.OldVoters...),
		NewVoters: append([]uint64(nil), configuration.NewVoters...),
	}
}
