// Package raft contains the protocol types and invariants used by the Raft KV store.
//
// This first increment defines the wire messages and replicated-log entries. Election,
// persistence, and transport behavior are added in later commits.
package raft

import "fmt"

// Role is a node's current Raft role.
type Role uint8

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return fmt.Sprintf("role(%d)", r)
	}
}

// EntryKind identifies the state-machine operation carried by a log entry.
type EntryKind uint8

const (
	EntryNoop EntryKind = iota
	EntrySet
	EntryDelete
	EntryConfigJoint
	EntryConfigFinal
)

// LogEntry is an item in the replicated log. Index is implicit in slice position;
// Term records the term in which the leader created the entry.
type LogEntry struct {
	Term  uint64
	Kind  EntryKind
	Key   string
	Value string
	Nodes []uint64
}

// VoteRequest asks a peer to grant CandidateID one vote in Term.
type VoteRequest struct {
	Term         uint64
	CandidateID  uint64
	LastLogIndex uint64
	LastLogTerm  uint64
}

// VoteResponse reports the receiver's current term and whether it granted the vote.
type VoteResponse struct {
	Term        uint64
	VoteGranted bool
}

// AppendEntriesRequest is both the heartbeat and log replication request.
type AppendEntriesRequest struct {
	Term         uint64
	LeaderID     uint64
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

// AppendEntriesResponse reports log consistency and conflict hints.
type AppendEntriesResponse struct {
	Term          uint64
	Success       bool
	MatchIndex    uint64
	ConflictIndex uint64
	ConflictTerm  uint64
}

// Snapshot carries a compacted state-machine image to a lagging peer.
type Snapshot struct {
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              map[string]string
	Configuration     Configuration
}

// Configuration describes the voters in the cluster. During a joint
// configuration both voter sets must independently form a majority.
type Configuration struct {
	OldVoters []uint64
	NewVoters []uint64
}

// IsJoint reports whether both configurations are present.
func (c Configuration) IsJoint() bool { return len(c.NewVoters) != 0 }

// HasVoter reports whether id is a voter in either active voter set.
func (c Configuration) HasVoter(id uint64) bool {
	for _, voter := range c.OldVoters {
		if voter == id {
			return true
		}
	}
	for _, voter := range c.NewVoters {
		if voter == id {
			return true
		}
	}
	return false
}

// HasQuorum applies Raft's quorum rule. In joint consensus, a majority of both
// the old and new voter sets is required.
func (c Configuration) HasQuorum(votes map[uint64]bool) bool {
	if !majority(c.OldVoters, votes) {
		return false
	}
	return !c.IsJoint() || majority(c.NewVoters, votes)
}

func majority(voters []uint64, votes map[uint64]bool) bool {
	if len(voters) == 0 {
		return false
	}
	count := 0
	seen := make(map[uint64]struct{}, len(voters))
	for _, voter := range voters {
		if _, duplicate := seen[voter]; duplicate {
			continue
		}
		seen[voter] = struct{}{}
		if votes[voter] {
			count++
		}
	}
	return count >= len(seen)/2+1
}
