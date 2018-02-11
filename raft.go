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

import "sync"
import (
	"labrpc"
	"bytes"
	"labgob"
	"time"
	"math/rand"
)

// import "bytes"
// import "labgob"


const (
	STATE_CANDIDATE = iota
	STATE_FOLLOWER
	STATE_LEADER

	HEARTBEAT_TIMEOUT = 50
	ELECT_TIMEOUT = 100
	ELECT_WAIT_LOWERBOUND = 1
	ELECT_WAIT_UPBOUND = 30
)



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Index int // log index
	Term int // leader's term
	Command interface{} // command to apply
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state int // candidate or follower or leader
	voteCount int // elect for leader, receive other's vote count
	hbChannel chan bool // heartbeat from followers
	commitChannel chan bool
	grantVoteChannel chan bool
	leaderChannel chan bool
	applyChannel chan ApplyMsg

	// persistent state on all servers
	currentTerm int // current system term
	voteFor     int // voteFor's index into peers[]
	logs        []LogEntry // log entry for state machine

	// volatile state on all servers
	commitIndex int // index of highest entry known to be committed
	lastAppliedIndex int // index of highest entry applied to state machine

	// volatile state on leader
	nextIndex   []int // next log entry index to send to peer
	matchIndex  []int // index of highest log entry
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	defer rf.mu.Unlock()
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
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	e.Encode(rf.nextIndex)
	e.Encode(rf.matchIndex)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var nextIndex []int
	var matchIndex []int
	if d.Decode(&nextIndex) != nil || d.Decode(&matchIndex) != nil {
		return
	} else {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.nextIndex = nextIndex
		rf.matchIndex = matchIndex
	}
}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int // candidate's term
	CandidateIndex int // candidate's index in peers
	LastLogIndex int // candidate's last log entry's index
	LastLogTerm int // candidate's last long entry's term
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int // current term
	VoteGranted bool // granted or not
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	voteGranted := true
	voteGranted = voteGranted && args.Term < rf.currentTerm
	voteGranted = voteGranted && (args.CandidateIndex < len(rf.peers)) && rf.peers[args.CandidateIndex] != nil
	voteGranted = voteGranted && args.LastLogIndex >= rf.lastAppliedIndex && args.Term >= rf.logs[len(rf.logs)-1].Term

	reply.Term = rf.currentTerm
	reply.VoteGranted = voteGranted
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
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if rf.state != STATE_CANDIDATE {
			return ok
		}

		term := rf.currentTerm
		if args.Term != term {
			return ok
		}

		if reply.Term > term {
			rf.currentTerm = reply.Term
			rf.state = STATE_FOLLOWER
			rf.voteFor = -1
			rf.persist()
		}
		if reply.VoteGranted {
			rf.voteCount++
			if rf.voteCount > len(rf.peers) / 2 {
				rf.state = STATE_LEADER
				rf.leaderChannel <- true
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

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term, isLeader = rf.GetState()
	rf.logs = append(rf.logs, LogEntry{
		len(rf.logs)-1,
		term,
		command,
	})
	index = len(rf.logs) - 1
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

type AppendEntryArg struct {
	Index int // leader's index of peer
	Term int // leader's term
	PrevLogIndex int // index of log entry
	PrevLogTerm int // term of prev log
	Entries []LogEntry // log entries to store, empty for heartbeat
	LeaderCommit int // leader's commit index
}

type AppendEntryReply struct {
	Term int // term
	Success bool // append or not
	NextIndex int // next match index
}

func (rf *Raft) AppendEntries(arg *AppendEntryArg, reply *AppendEntryReply) {
	success := false
	term := rf.currentTerm
	nextIndex := -1

	// leader's term less than rf's term
	if arg.Term < rf.currentTerm {
		success = false
		reply.Success = success
		reply.Term = term
		reply.NextIndex = nextIndex
		return
	}

	// log term
	if len(rf.logs) > arg.PrevLogIndex && rf.logs[arg.PrevLogIndex].Term != arg.PrevLogTerm {
		success = false
		reply.Success = success
		reply.Term = term
		reply.NextIndex = nextIndex
		return
	}

	// missing log entry, append log entry
	if len(rf.logs) <= arg.PrevLogIndex {
		success = true
		reply.Success = success
		reply.Term = term
		reply.NextIndex = len(rf.logs)
		return
	}

	// heartbeat msg
	if arg.Entries == nil {
		rf.hbChannel <-true
		success = true
		reply.Success = success
		reply.Term = term
		reply.NextIndex = len(rf.logs)
		return
	}

	// repeat log entry
	rf.logs = append(rf.logs[:(arg.PrevLogIndex+1)], arg.Entries...)
	if arg.LeaderCommit > rf.commitIndex {
		rf.commitIndex = arg.LeaderCommit
	}
	reply.Success = true
	reply.Term = term
	reply.NextIndex = len(rf.logs)
}

func (rf *Raft) broadcastRequestVote() {
	var args RequestVoteArgs
	rf.mu.Lock()
	args.Term = rf.currentTerm
	args.CandidateIndex = rf.me
	args.LastLogTerm = rf.logs[len(rf.logs)-1].Term
	args.LastLogIndex = len(rf.logs) - 1
	rf.mu.Unlock()

	for i := range rf.peers {
		if i != rf.me && rf.state == STATE_CANDIDATE {
			go func() {
				var reply RequestVoteReply
				rf.sendRequestVote(i, &args, &reply)
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

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.state = STATE_CANDIDATE
	rf.voteFor = -1
	rf.logs = append(rf.logs, LogEntry{Term:0})
	rf.hbChannel = make(chan bool)
	rf.commitChannel = make(chan bool)
	rf.grantVoteChannel = make(chan bool)
	rf.leaderChannel = make(chan bool)
	rf.applyChannel = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// candidate, follower, leader, state machine
	go func() {
		for {
			switch rf.state {
			case STATE_FOLLOWER:
				select {
				case <- rf.hbChannel:
					case <- rf.grantVoteChannel:
						case <- time.After(time.Duration(rand.Int63() % 333 + 550) * time.Millisecond):
							rf.state = STATE_CANDIDATE
				}
			case STATE_LEADER:
				time.Sleep(HEARTBEAT_TIMEOUT)
			case STATE_CANDIDATE:
				rf.mu.Lock()
				rf.currentTerm++
				rf.voteFor = rf.me
				rf.voteCount = 1
				rf.persist()
				rf.mu.Unlock()

				go rf.broadcastRequestVote()

				select {
				case <- time.After(time.Duration(rand.Int63() % 333 + 550) * time.Millisecond):
					case <- rf.hbChannel:
						rf.state = STATE_FOLLOWER
						case <- rf.leaderChannel:
							rf.mu.Lock()
							rf.state = STATE_LEADER
							rf.nextIndex = make([]int, len(rf.peers))
							rf.matchIndex = make([]int, len(rf.peers))
							for i := range rf.peers {
								rf.nextIndex[i] = rf.logs[len(rf.logs)-1].Index
								rf.matchIndex[i] = 0
							}
				}
			}
		}
	}()

	go func() {
		for {
			select {
			case <- rf.commitChannel:
				rf.mu.Lock()
				commitIndex := rf.commitIndex
				baseIndex := rf.logs[0].Index
				for i := rf.lastAppliedIndex + 1; i <= commitIndex; i++ {
					msg := ApplyMsg{CommandValid: true, CommandIndex: i, Command: rf.logs[i-baseIndex].Command}
					applyCh <- msg
					rf.lastAppliedIndex = i
				}
				rf.mu.Unlock()
			}
		}
	}()


	return rf
}
