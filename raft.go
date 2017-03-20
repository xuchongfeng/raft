package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"sync"
	"labrpc"
	"bytes"
	"encoding/gob"
	"time"
	"math/rand"
	"sync"
)

const (
	STATE_CANDIDATE = iota
	STATE_FOLLOWER
	STATE_LEADER

	HEARTBEAT_INTERVAL = 50 * time.Millisecond
)


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}


// log entry
type LogEntry struct {
	Index        int
	Term         int
	Command      interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state int
	voteCount int
	hb_channel chan bool
	commit_channel chan bool
	grant_vote_channel chan bool
	leader_channel chan bool
	apply_channel chan ApplyMsg



	// persistent state on all servers
	currentTerm    int
	votedFor       int              // index into peers[]
	log            []LogEntry       // log of commands

	// volatile state on all servers
	commitIndex     int             // index of highest log entry known to be committed
	lastApplied     int             // index of highest log entry applied to state machine


	// volatile state on leaders
	nextIndex       []int           // index of next log entry for each server
	matchIndex      []int           // index of highest log entry replicated on server for each server



}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here.
	term = rf.currentTerm
	isleader = rf.state == STATE_LEADER
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}




//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term             int      // candidate's term
	CandidateId      int      // candidate's id
	LastLogIndex     int      // candidate's last log index
	LastLogTerm      int      // candidate's last log term
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term             int     // current term
	VoteGranted      bool    // true mean candidate receive vote
}


type AppendEntriesArgs struct {
	Term            int
	LeaderId        int
	PrevLogTerm     int
	PrevLogIndex    int
	Entries         []LogEntry
	LeaderCommit    int
}


type AppendEntriesReply struct {
	Term            int
	Success         bool
	NextIndex       int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	curTerm := rf.currentTerm
	reply.Term = curTerm
	reply.VoteGranted = false

	// server has vote for another candidate
	if rf.votedFor != -1 {
		return
	}
	// candidate's term < server's term
	if curTerm > args.Term {
		return
	}

	if len(rf.log) < (args.LastLogIndex + 1) {
		return
	}

	// candidate's last log index or candidate's last log term < server's
	if rf.commitIndex > args.LastLogIndex || rf.log[rf.commitIndex].Term > args.LastLogTerm {
		return
	}

	// vote for the candidate
	rf.votedFor = args.CandidateId
	reply.VoteGranted = true

	rf.grant_vote_channel <- true
	rf.state = STATE_FOLLOWER
	return
}


func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	
	reply.Success = false
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		term := rf.currentTerm
		if rf.state != STATE_CANDIDATE {
			return ok
		}
		if args.Term != term {
			return ok
		}
		if reply.Term > term {
			rf.currentTerm = reply.Term
			rf.state = STATE_FOLLOWER
			rf.votedFor = -1
			rf.persist()
		}
		if reply.VoteGranted {
			rf.voteCount++
			if rf.voteCount > len(rf.peers) / 2 {
				rf.state = STATE_LEADER
				rf.leader_channel <- true
			}
		}
	}
	return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// this server isn't the leader
	if rf.state != STATE_LEADER {
		return index, term, false
	}

	// this server is the leader, log the command
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
	index = len(rf.log) - 1
	term = rf.currentTerm

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}


func (rf *Raft) broadcastRequestVote() {
	var args RequestVoteArgs
	rf.mu.Lock()
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogTerm = rf.log[-1].Term
	args.LastLogIndex = len(rf.log) - 1
	rf.mu.Unlock()

	for i := range rf.peers {
		if i != rf.me && rf.state == STATE_CANDIDATE {
			go func() {
				var reply RequestVoteReply
				rf.sendRequestVote(i, args, &reply)
			}()
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.state = STATE_FOLLOWER
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{Term:0})
	rf.currentTerm = 0
	rf.hb_channel = make(chan bool)
	rf.commit_channel = make(chan bool)
	rf.grant_vote_channel = make(chan bool)
	rf.leader_channel = make(chan bool)
	rf.apply_channel = applyCh


	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func(){
		for {
			switch rf.state {
			case STATE_FOLLOWER:
				select {
				case <- rf.hb_channel:
				case <- rf.grant_vote_channel:
				case <- time.After(time.Duration(rand.Int63() % 333 + 550) * time.Millisecond):
					rf.state = STATE_CANDIDATE
				}
			case STATE_LEADER:
				time.Sleep(HEARTBEAT_INTERVAL)
			case STATE_CANDIDATE:
				rf.mu.Lock()
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.voteCount = 1
				rf.persist()
				rf.mu.Unlock()
				go rf.broadcastRequestVote()

				select {
				case <- time.After(time.Duration(rand.Int63() % 333 + 550) * time.Millisecond):
				case <- rf.hb_channel:
					rf.state = STATE_FOLLOWER
				case <- rf.leader_channel:
					rf.mu.Lock()
					rf.state = STATE_LEADER
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					for i := range rf.peers {
						rf.nextIndex[i] = rf.log[-1].Index
						rf.matchIndex[i] = 0
					}
				}
			}
		}
	}()


	go func() {
		for {
			select {
			case <- rf.commit_channel:
				rf.mu.Lock()
				commitIndex := rf.commitIndex
				baseIndex := rf.log[0].Index
				for i := rf.lastApplied + 1; i <= commitIndex; i++ {
					msg := ApplyMsg{Index:i, Command: rf.log[i-baseIndex].Command}
					applyCh <- msg
					rf.lastApplied = i
				}
				rf.mu.Unlock()
			}
		}
	}()

	return rf
}
