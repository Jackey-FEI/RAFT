//
// raft.go
// =======
// Write your code in this file
// We will use the original version of all other
// files for testing
//

package raft

//
// API
// ===
// This is an outline of the API that your raft implementation should
// expose.
//
// rf = NewPeer(...)
//   Create a new Raft server.
//
// rf.PutCommand(command interface{}) (index, term, isleader)
//   PutCommand agreement on a new log entry
//
// rf.GetState() (me, term, isLeader)
//   Ask a Raft peer for "me" (see line 58), its current term, and whether it thinks it
//   is a leader
//
// ApplyCommand
//   Each time a new entry is committed to the log, each Raft peer
//   should send an ApplyCommand to the service (e.g. tester) on the
//   same server, via the applyCh channel passed to NewPeer()
//

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/cmu440/rpc"
)

// Set to false to disable debug logs completely
// Make sure to set kEnableDebugLogs to false before submitting
const kEnableDebugLogs = false

// Set to true to log to stdout instead of file
const kLogToStdout = false

// Change this to output logs to a different directory
const kLogOutputDir = "./raftlogs/"

// ApplyCommand
// ========
//
// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyCommand to the service (or
// tester) on the same server, via the applyCh passed to NewPeer()
type ApplyCommand struct {
	Index   int
	Command interface{}
}

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

// Raft struct
// ===========
//
// A Go object implementing a single Raft peer
type Raft struct {
	mux   sync.Mutex       // Lock to protect shared access to this peer's state
	peers []*rpc.ClientEnd // RPC end points of all peers
	me    int              // this peer's index into peers[]
	// You are expected to create reasonably clear log files before asking a
	// debugging question on Edstem or OH. Use of this logger is optional, and
	// you are free to remove it completely.
	logger *log.Logger // We provide you with a separate logger per peer.

	// TODO - Your data here (2A, 2B).
	// Look at the Raft paper's Figure 2 for a description of what
	// state a Raft peer should maintain
	currentTerm int         // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int         //  candidateId that received vote in current term (or null if none)
	logEntries  []*LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	// Volatile state on all servers:
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// Volatile state on leaders: Reinitialized after election
	nextIndex       []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex      []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	followerReplyCh chan *AppendEntriesReply

	heartBeatCh chan *AppendEntriesReply
	voteReplyCh chan *RequestVoteReply
	applyChan   chan ApplyCommand

	// additional self-add state
	isLeader bool
}

// GetState
// ==========
//
// Return "me", current term and whether this peer
// believes it is the leader
func (rf *Raft) GetState() (int, int, bool) {
	var me int
	var term int
	var isleader bool
	// TODO - Your code here (2A)
	rf.mux.Lock()
	me = rf.me
	term = rf.currentTerm
	isleader = rf.isLeader
	rf.mux.Unlock()
	return me, term, isleader
}

// RequestVoteArgs
// ===============
//
// Example RequestVote RPC arguments structure.
//
// # Please note: Field names must start with capital letters!
type RequestVoteArgs struct {
	// TODO - Your data here (2A, 2B)
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry (§5.4)
	LastLogTerm  int // term of candidate’s last log entry (§5.4)
}

// RequestVoteReply
// ================
//
// Example RequestVote RPC reply structure.
//
// # Please note: Field names must start with capital letters!
type RequestVoteReply struct {
	// TODO - Your data here (2A)
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
	SentTerm    int  // the term when we sent the request in case we mix different terms
}

// RequestVote
// ===========
//
// Example RequestVote RPC handler
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// TODO - Your code here (2A, 2B)
	reply.Term = rf.getCurTerm()
	reply.SentTerm = args.Term
	rf.logger.Printf("receive requestVote from:%v, with args:%v %v %v", args.CandidateId, args.LastLogIndex, args.LastLogTerm, args.Term)
	if args.Term < reply.Term {
		reply.VoteGranted = false
		return
	}

	rf.mux.Lock()
	lastLogIndex := len(rf.logEntries) - 1
	lastIndexTerm := rf.logEntries[lastLogIndex].Term
	rf.mux.Unlock()

	if lastIndexTerm > args.LastLogTerm {
		reply.VoteGranted = false
		return
	}

	if lastIndexTerm == args.LastLogTerm && args.LastLogIndex < lastLogIndex {
		reply.VoteGranted = false
		return
	}
	rf.mux.Lock()
	if args.Term == rf.currentTerm {

		isLeader := rf.isLeader
		voteFor := rf.votedFor
		me := rf.me

		if isLeader {
			reply.VoteGranted = false
			rf.mux.Unlock()
			return
		}
		if voteFor != -1 && voteFor != args.CandidateId {
			if voteFor == me {
				rf.heartBeatCh <- &AppendEntriesReply{Term: args.Term}
			}
			reply.VoteGranted = false
			rf.mux.Unlock()
			return
		}

	}

	if args.Term > rf.currentTerm {
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
	}
	rf.mux.Unlock()

	rf.heartBeatCh <- &AppendEntriesReply{Term: args.Term}

	rf.mux.Lock()
	rf.votedFor = args.CandidateId
	rf.mux.Unlock()
	reply.VoteGranted = true
	return

}

// sendRequestVote
// ===============
//
// Example code to send a RequestVote RPC to a server.
//
// server int -- index of the target server in
// rf.peers[]
//
// args *RequestVoteArgs -- RPC arguments in args
//
// reply *RequestVoteReply -- RPC reply
//
// The types of args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers)
//
// The rpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost
//
// Call() sends a request and waits for a reply.
//
// If a reply arrives within a timeout interval, Call() returns true;
// otherwise Call() returns false
//
// Thus Call() may not return for a while.
//
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply
//
// Call() is guaranteed to return (perhaps after a delay)
// *except* if the handler function on the server side does not return
//
// Thus there
// is no need to implement your own timeouts around Call()
//
// Please look at the comments and documentation in ../rpc/rpc.go
// for more details
//
// If you are having trouble getting RPC to work, check that you have
// capitalized all field names in the struct passed over RPC, and
// that the caller passes the address of the reply struct with "&",
// not the struct itself
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		rf.voteReplyCh <- reply
	}
	return ok
}

// AppendEntriesArgs
// ===============
//
// # Please note: Field names must start with capital letters!
type AppendEntriesArgs struct {
	// TODO - Your data here (2A, 2B)
	Term         int         // leader’s term
	LeaderId     int         // so follower can redirect clients
	PrevLogIndex int         // index of log entry immediately preceding new ones
	PrevLogTerm  int         // term of prevLogIndex entry
	Entries      []*LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int         // leader’s commitIndex
	MatchIndex   int
}

// AppendEntriesReply
// ================
//
// # Please note: Field names must start with capital letters!
type AppendEntriesReply struct {
	// TODO - Your data here (2A)
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm

	//self-defined field, to decrease nextindex faster
	MatchIndex                int // The index of the highest log entry on which the follower and leader agree.
	ConflictTerm              int // The term of the conflicting entry.
	FirstIndexForConflictTerm int // The first index it stores for the conflicting term.
	FollowerId                int // The id of which follower sent back the reply, useful to update the nextindex array in leader
	WrongTerm                 bool
}

// RequestVote
// ===========
//
// Example AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// TODO - Your code here (2A, 2B)
	//reply.Term = rf.getCurTerm()
	rf.mux.Lock()
	reply.FollowerId = rf.me
	reply.Term = rf.currentTerm
	reply.WrongTerm = false
	rf.mux.Unlock()
	if args.Term < reply.Term {
		reply.WrongTerm = true
		return
	}
	// More for log replica

	rf.heartBeatCh <- reply

	reply.Success = false
	rf.mux.Lock()
	if len(rf.logEntries) <= args.PrevLogIndex || rf.logEntries[args.PrevLogIndex].Term != args.PrevLogTerm {
		// Find the first index in this term, going backwards from PrevLogIndex.
		if len(rf.logEntries) > args.PrevLogIndex {
			reply.ConflictTerm = rf.logEntries[args.PrevLogIndex].Term
			index := args.PrevLogIndex
			for (index > 0) && (rf.logEntries[index].Term == reply.ConflictTerm) {
				index -= 1
			}
			reply.FirstIndexForConflictTerm = index
		} else {
			reply.ConflictTerm = -1 // Use -1 to indicate no entry
			reply.FirstIndexForConflictTerm = len(rf.logEntries)
		}
		//rf.logger.Printf("server sentback failure reply: %v", ReplyToString(reply))

		rf.mux.Unlock()
		return
	}

	reply.Success = true
	// // If the code reaches here, it means the follower has the matching log entry.
	// // Now, we need to check for consistency in subsequent entries.

	// // Check if the new entries conflict with the existing log entries.
	for i := 0; i < len(args.Entries); i++ {

		if (args.PrevLogIndex + i + 1) == len(rf.logEntries) {
			rf.logEntries = append(rf.logEntries, args.Entries[i:]...)
			break
		}
		if rf.logEntries[args.PrevLogIndex+i+1].Term != args.Entries[i].Term {
			rf.logEntries = rf.logEntries[:args.PrevLogIndex+i+1]

			entries := args.Entries[i:]
			rf.logEntries = append(rf.logEntries, entries...)
			break
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.logEntries)-1)
		rf.commitIndex = min(rf.commitIndex, args.MatchIndex)
	}
	rf.mux.Unlock()

	go rf.ApplyToStateMachine()
}

func (rf *Raft) sendAppendEntrie(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, replyCh chan *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		if args.Entries == nil {
			rf.heartBeatCh <- reply
			return true
		}
		if reply.WrongTerm {
			rf.heartBeatCh <- reply
			return false
		}

		if args.Entries != nil {
			if reply.Success == false {
				// The follower includes the term of the conflicting entry and the first index it has for that term.
				conflictTerm := reply.ConflictTerm
				firstIndexForConflictTerm := reply.FirstIndexForConflictTerm

				// Find the index of the last entry with conflictTerm in the leader’s log.
				lastIndexForConflictTerm := rf.findLastIndexForTerm(conflictTerm)

				// If the follower has a conflicting term that the leader has,
				// the leader sets nextIndex to be one past the last index for that term in its log.
				// Otherwise, it sets nextIndex to the first index of the conflict term from the follower's log.
				rf.mux.Lock()
				if conflictTerm == -1 {
					rf.nextIndex[reply.FollowerId] = firstIndexForConflictTerm
				} else if lastIndexForConflictTerm != 0 {
					rf.nextIndex[reply.FollowerId] = lastIndexForConflictTerm + 1
				} else {
					rf.nextIndex[reply.FollowerId] = firstIndexForConflictTerm
				}
				rf.matchIndex[reply.FollowerId] = rf.nextIndex[reply.FollowerId] - 1
				index := max(rf.nextIndex[reply.FollowerId]-1, 0)
				args2 := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: index,
					PrevLogTerm:  rf.logEntries[index].Term,
					Entries:      rf.logEntries[(index + 1):],
					LeaderCommit: rf.commitIndex,
					MatchIndex:   rf.matchIndex[reply.FollowerId],
				}

				rf.mux.Unlock()
				reply2 := &AppendEntriesReply{}
				go rf.sendAppendEntrie(reply.FollowerId, args2, reply2, replyCh)
			} else {

				rf.mux.Lock()
				rf.nextIndex[reply.FollowerId] = max(rf.nextIndex[reply.FollowerId], args.PrevLogIndex+1+len(args.Entries))
				rf.nextIndex[reply.FollowerId] = min(len(rf.logEntries), rf.nextIndex[reply.FollowerId])
				rf.matchIndex[reply.FollowerId] = rf.nextIndex[reply.FollowerId] - 1
				rf.mux.Unlock()
				replyCh <- reply

			}

		}
	}
	return ok
}

// PutCommand
// =====
//
// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log
//
// If this server is not the leader, return false.
//
// Otherwise start the agreement and return immediately.
//
// There is no guarantee that this command will ever be committed to
// the Raft log, since the leader may fail or lose an election
//
// The first return value is the index that the command will appear at
// if it is ever committed
//
// The second return value is the current term.
//
// The third return value is true if this server believes it is
// the leader
func (rf *Raft) PutCommand(command interface{}) (int, int, bool) {
	rf.mux.Lock()
	index := len(rf.logEntries)
	term := rf.currentTerm
	isLeader := rf.isLeader
	rf.mux.Unlock()

	// TODO - Your code here (2B)
	if isLeader {
		rf.mux.Lock()
		curlen := len(rf.logEntries)
		curTerm := rf.currentTerm
		logAdd := &LogEntry{
			Command: command,
			Term:    curTerm,
			Index:   curlen,
		}
		rf.logEntries = append(rf.logEntries, logAdd)
		rf.mux.Unlock()
		go rf.LogReplica(command)
	}
	return index, term, isLeader
}

// Stop
// ====
//
// The tester calls Stop() when a Raft instance will not
// be needed again
//
// You are not required to do anything
// in Stop(), but it might be convenient to (for example)
// turn off debug output from this instance
func (rf *Raft) Stop() {
	// TODO - Your code here, if desired
}

// NewPeer
// ====
//
// The service or tester wants to create a Raft server.
//
// The port numbers of all the Raft servers (including this one)
// are in peers[]
//
// This server's port is peers[me]
//
// All the servers' peers[] arrays have the same order
//
// applyCh
// =======
//
// applyCh is a channel on which the tester or service expects
// Raft to send ApplyCommand messages
//
// NewPeer() must return quickly, so it should start Goroutines
// for any long-running work
func NewPeer(peers []*rpc.ClientEnd, me int, applyCh chan ApplyCommand) *Raft {
	rf := &Raft{
		peers:       peers,
		me:          me,
		logger:      log.New(os.Stdout, fmt.Sprintf("Raft %d: ", me), log.LstdFlags),
		currentTerm: 0,
		votedFor:    -1,
		logEntries:  []*LogEntry{{Term: -1, Index: 0}},
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   []int{},
		matchIndex:  []int{},
		applyChan:   applyCh,
		voteReplyCh: make(chan *RequestVoteReply),
		heartBeatCh: make(chan *AppendEntriesReply),
		isLeader:    false,
	}

	if kEnableDebugLogs {
		peerName := peers[me].String()
		logPrefix := fmt.Sprintf("%s ", peerName)
		if kLogToStdout {
			rf.logger = log.New(os.Stdout, peerName, log.Lmicroseconds|log.Lshortfile)
		} else {
			err := os.MkdirAll(kLogOutputDir, os.ModePerm)
			if err != nil {
				panic(err.Error())
			}
			logOutputFile, err := os.OpenFile(fmt.Sprintf("%s/%s.txt", kLogOutputDir, logPrefix), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
			if err != nil {
				panic(err.Error())
			}
			rf.logger = log.New(logOutputFile, logPrefix, log.Lmicroseconds|log.Lshortfile)
		}
		rf.logger.Println("logger initialized")
	} else {
		rf.logger = log.New(ioutil.Discard, "", 0)
	}

	// TODO - Your initialization code here (2A, 2B)
	rf.mux.Lock()
	for i := 0; i < len(peers); i++ {
		rf.nextIndex = append(rf.nextIndex, 1)
		rf.matchIndex = append(rf.matchIndex, 0)
	}
	rf.mux.Unlock()
	go rf.FollowerRoutine()
	return rf
}

func (rf *Raft) getCurTerm() int {
	// TODO - a mainroutine might need to seperate it to other routine
	rf.mux.Lock()
	defer rf.mux.Unlock()
	return rf.currentTerm
}

// FollowerRoutine
// ================
//
// Respond to RPCs from candidates and leaders
// If election timeout elapses without receiving AppendEntries, RPC from current leader or granting vote to candidate: convert to candidate
func (rf *Raft) FollowerRoutine() {
	// TODO - a follower routine
	rf.mux.Lock()
	rf.isLeader = false
	rf.mux.Unlock()
	timer := time.NewTimer(time.Duration(rand.Intn(200)+300) * time.Millisecond) // here we set election timeout in a range of 300-500 ms
	checkTimer := time.NewTimer(time.Duration(50) * time.Millisecond)
	for {
		select {
		case <-checkTimer.C:
			go rf.ApplyToStateMachine()
			checkTimer.Reset(time.Duration(50) * time.Millisecond)
		case <-timer.C:
			go rf.CandidateRoutine()
			return
		case appendReply := <-rf.heartBeatCh:
			// hearbeat
			if appendReply.Term > rf.getCurTerm() {
				rf.mux.Lock()
				rf.currentTerm = appendReply.Term
				rf.mux.Unlock()
			}
			timer.Reset(time.Duration(rand.Intn(200)+300) * time.Millisecond)
		}
	}
}

// CandidateRoutine
// ================
//
// On conversion to candidate, start election:
// Increment currentTerm
// Vote for self
// Reset election timer
// Send RequestVote RPCs to all other servers
// If votes received from majority of servers: become leader
// If AppendEntries RPC received from new leader: convert to follower
// If election timeout elapses: start new election
func (rf *Raft) CandidateRoutine() {
	// TODO - a candidate routine
	rf.mux.Lock()
	rf.currentTerm = rf.currentTerm + 1
	curterm := rf.currentTerm
	candID := rf.me
	lastindex := len(rf.logEntries) - 1
	peerNum := len(rf.peers)
	lastlogterm := rf.logEntries[len(rf.logEntries)-1].Term
	rf.votedFor = rf.me
	rf.mux.Unlock()
	timer := time.NewTimer(time.Duration(rand.Intn(200)+300) * time.Millisecond) // here we set election timeout in a range of 300-500 ms
	// write for loop here for every peer in rf.peer send a requestvote
	args := &RequestVoteArgs{
		Term:         curterm,
		CandidateId:  candID,
		LastLogIndex: lastindex,
		LastLogTerm:  lastlogterm,
	}

	for i := range rf.peers {
		if i != candID {
			reply := &RequestVoteReply{
				SentTerm: curterm,
			}
			go rf.sendRequestVote(i, args, reply)
		}
	}
	votedReceived := 1
	for {
		select {
		case <-timer.C:
			go rf.CandidateRoutine()
			return
		case voteReply := <-rf.voteReplyCh:
			if voteReply.Term >= rf.getCurTerm() {
				rf.mux.Lock()
				rf.currentTerm = voteReply.Term
				rf.mux.Unlock()
				go rf.FollowerRoutine()
				return
			}
			if voteReply.VoteGranted && voteReply.SentTerm == rf.getCurTerm() {
				votedReceived++
			}
			if votedReceived > (peerNum / 2) {
				go rf.LeaderRoutine()
				return
			}
		case appendReply := <-rf.heartBeatCh:
			// hearbeat
			rf.mux.Lock()
			if appendReply.Term > rf.currentTerm {
				rf.currentTerm = appendReply.Term
			}
			rf.mux.Unlock()
			go rf.FollowerRoutine()
			return

		}
	}
}

// LeaderRoutine
// ================
// Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server; repeat during idle periods to prevent election timeouts (§5.2)
// If command received from client: append entry to local log, respond after entry applied to state machine (§5.3)
// If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
// If successful: update nextIndex and matchIndex for follower (§5.3)
// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
func (rf *Raft) LeaderRoutine() {
	// TODO - a leader routine
	rf.mux.Lock()
	rf.isLeader = true
	curleader := rf.me
	//leadercommit := rf.commitIndex
	nextindexInit := min(rf.commitIndex+1, len(rf.logEntries))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = nextindexInit
	}
	for i := range rf.matchIndex {
		rf.matchIndex[i] = rf.nextIndex[i] - 1
	}
	rf.mux.Unlock()
	for i := range rf.peers {
		if i != curleader {
			rf.mux.Lock()
			index := max(rf.nextIndex[i]-1, 0)
			args2 := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: index,
				PrevLogTerm:  rf.logEntries[index].Term,
				Entries:      rf.logEntries[(index + 1):],
				LeaderCommit: rf.commitIndex,
				MatchIndex:   rf.matchIndex[i],
			}
			rf.mux.Unlock()
			reply := &AppendEntriesReply{}
			go rf.sendAppendEntrie(i, args2, reply, rf.heartBeatCh)
		}
	}

	heartBeatTimer := time.NewTimer(time.Duration(100) * time.Millisecond)

	for {
		select {
		case <-heartBeatTimer.C:

			for i := range rf.peers {
				if i != curleader {
					rf.mux.Lock()
					rf.logger.Printf("nextindex[%v] - 1 = %v\n", i, rf.nextIndex[i]-1)
					index := max(rf.nextIndex[i]-1, 0)
					args := &AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     curleader,
						PrevLogIndex: index,
						PrevLogTerm:  rf.logEntries[index].Term,
						//Entries:      rf.logEntries[(index + 1):],
						Entries:      nil,
						LeaderCommit: rf.commitIndex,
						MatchIndex:   rf.matchIndex[i],
					}
					rf.mux.Unlock()
					reply := &AppendEntriesReply{}
					go rf.sendAppendEntrie(i, args, reply, rf.heartBeatCh)
				}
			}
			heartBeatTimer.Reset(time.Duration(100) * time.Millisecond)
		case appendReply := <-rf.heartBeatCh:
			// hearbeat
			if appendReply.Term > rf.getCurTerm() {
				rf.mux.Lock()
				rf.currentTerm = appendReply.Term
				rf.isLeader = false
				rf.mux.Unlock()
				go rf.FollowerRoutine()
				return
			}
		}
	}

}

// LogReplica
// ================
func (rf *Raft) LogReplica(command interface{}) {
	// TODO:
	// first append the command to its own log
	rf.mux.Lock()
	curlen := len(rf.logEntries) - 1
	peerlen := len(rf.peers)
	me := rf.me
	curTerm := rf.currentTerm
	rf.logger.Printf("leader appended logentry: %v", logEntriesToString(rf.logEntries))

	// send AppendEnry command to each server
	ReplyCh := make(chan *AppendEntriesReply, peerlen-1)
	for i := range rf.peers {
		if i != me {
			index := max(rf.nextIndex[i]-1, 0)
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     me,
				PrevLogIndex: index,
				PrevLogTerm:  rf.logEntries[index].Term,
				Entries:      rf.logEntries[(index + 1):],
				LeaderCommit: rf.commitIndex,
				MatchIndex:   rf.matchIndex[i],
			}
			reply := &AppendEntriesReply{}
			go rf.sendAppendEntrie(i, args, reply, ReplyCh)
		}
	}
	rf.mux.Unlock()
	// if rejected decrement nextindex, try again
	// if majority commited, apply it state machine (another routine)
	numCommited := 1

	for {
		select {
		case reply := <-ReplyCh:
			rf.mux.Lock()
			if !rf.isLeader {
				rf.mux.Unlock()
				return
			}
			rf.mux.Unlock()
			if reply.Term == curTerm {
				// If successful, update the nextIndex to the last entry sent.
				rf.mux.Lock()
				numCommited++
				rf.logger.Printf("leader: %v receive suceess from %v", rf.me, reply.FollowerId)
				if numCommited > peerlen/2 {
					rf.commitIndex = max(curlen, rf.commitIndex)
					rf.mux.Unlock()
					go rf.ApplyToStateMachine()
					rf.logger.Printf("return after go applystatemachine with commitindex: %v", curlen)
					return
				}
				rf.mux.Unlock()
			}
		}
	}
}

// LogReplica
// ================
// A helper function to find the largest index for the conflicting term
func (rf *Raft) findLastIndexForTerm(conflictTerm int) int {
	rf.mux.Lock() // Protect access to rf.logEntries
	defer rf.mux.Unlock()
	lastIndex := 0 // Start with 0, indicating no entry found by default
	if conflictTerm < 0 {
		return lastIndex
	}
	// Iterate backwards over the log to find the last entry with the given term.
	for i := len(rf.logEntries) - 1; i >= 1; i-- {
		if rf.logEntries[i].Term == conflictTerm {
			lastIndex = i
			break
		}
	}
	// Return the index, or 0 if no matching term is found.
	return lastIndex
}

func (rf *Raft) ApplyToStateMachine() {
	rf.mux.Lock()
	rf.logger.Printf("in apply server: %v, has entries: %s", rf.me, logEntriesToString(rf.logEntries))
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied += 1
		command := ApplyCommand{
			Index:   rf.lastApplied,
			Command: rf.logEntries[rf.lastApplied].Command,
		}
		rf.mux.Unlock()
		rf.applyChan <- command
		rf.mux.Lock()
	}
	rf.logger.Printf("server: %v append lastapplied: %v\n", rf.me, rf.lastApplied)
	defer rf.mux.Unlock()
}

func logEntriesToString(entries []*LogEntry) string {
	var sb strings.Builder
	sb.WriteString("[")
	for i, entry := range entries {
		if entry != nil {
			sb.WriteString(fmt.Sprintf("{Term: %d, Command: %v}", entry.Term, entry.Command))
		} else {
			sb.WriteString("<nil>")
		}
		if i < len(entries)-1 {
			sb.WriteString(", ")
		}
	}
	sb.WriteString("]")
	return sb.String()
}
