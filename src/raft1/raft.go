package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state
	currentTerm int // 当前已知最新任期，首次启动为0，只增不减
	votedFor    int // 当前任期投给了谁；若未投票则为空
	//logs        []Log // 日志条目；每项包含状态机命令、任期号；索引从1开始

	// volatile state
	//commitIndex  int       // 已知已提交的最高日志索引
	//lastApplied  int       // 已应用到状态机的最高日志索引
	currentState     state     // 当前状态
	electionDeadline time.Time // 选举超时时间

	// volatile state on leaders
	//nextIndex  []int // 对每个follower, 下一个要发送的日志索引（初始化为leader最后一条日志索引+1）
	//matchIndex []int // 对每个follower，已知复制成功的最高日志索引

}

//type Log struct {
//	command []byte // 状态机命令
//	term    int    // 任期号
//	// Q6: 包括索引吗
//	// 当前先包括
//	index int
//}

type state int

const (
	follower state = iota
	candidate
	leader
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// var term int
	// var isleader bool
	// Your code here (3A).
	// Q1: 如何确定自己是否是leader?
	// 使用状态变量来标识
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.currentState == leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term        int // 候选人的任期
	CandidateId int // 候选人的ID
	//comment for 3A
	//LastLogIndex int // 候选人最后一条日志索引
	//LastLogTerm  int // 候选人最后一条日志的任期
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // 供候选人更新自己的currentTerm
	VoteGranted bool // 是否投票给候选人
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	// 如果 term < currentTerm，拒绝投票
	// 若RPC请求或响应中的term T > currentTerm: 另currentTerm=T，并转换为follower
	// Q7: 如果是请求投票也是吗？但是如果自己已经是candidate了呢?
	// 当前视为也是
	rf.checkAndUpdateTerm(args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		DPrintf("【请求投票处理】我是%d, 我的当前任期是%d, 请求的任期是%d, 我投给了%d, 请求的candidateId是%d, 请求投票结果是%v", rf.me, rf.currentTerm, args.Term, rf.votedFor, args.CandidateId, reply.VoteGranted)
	}()
	if args.Term < rf.currentTerm {
		// 拒绝投票
		reply.Term = rf.currentTerm
		return
	}
	// 如果尚未投票或已投给该candidate，且候选人的日志至少和自己一样新，则授予选票
	// Raft通过比较日志最后一个条目的term和index来确定哪个日志更新。如果日志最后个条目的term不同，那么有更新的term的日志更新。如果两个日志最后的term相同，那么更长的日志更新。
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// comment for 3A
		//if args.LastLogTerm < rf.logs[len(rf.logs)-1].term {
		//	// 拒绝
		//	return
		//}
		//if args.LastLogTerm == rf.logs[len(rf.logs)-1].term && args.LastLogIndex < rf.logs[len(rf.logs)-1].index {
		//	// 拒绝
		//	return
		//}
		// 接受
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	}
	return
}

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
//func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
//	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
//	return ok
//}

func (rf *Raft) sendRequestVoteChannel(server int, args *RequestVoteArgs, reply *RequestVoteReply, ch chan<- bool) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		// 若RPC请求或响应中的term T > currentTerm: 另currentTerm=T，并转换为follower
		rf.checkAndUpdateTerm(reply.Term)
		if !rf.checkCandidate() {
			return
		}
		ch <- reply.VoteGranted
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		// 若RPC请求或响应中的term T > currentTerm: 另currentTerm=T，并转换为follower
		rf.checkAndUpdateTerm(reply.Term)
		if rf.checkCandidate() {
			return reply.VoteGranted
		}
	}
	return false
}

//func (rf *Raft) SendRequestVoteChannel() {
//	args := make([]RequestVoteArgs, len(rf.peers))
//	replies := make([]RequestVoteReply, len(rf.peers))
//
//	ch := make(chan bool)
//	for i := range rf.peers {
//		rf.mu.Lock()
//		args[i] = RequestVoteArgs{
//			Term:        rf.currentTerm,
//			CandidateId: rf.me,
//		}
//		rf.mu.Unlock()
//		go rf.sendRequestVoteChannel(i, &args[i], &replies[i], ch)
//	}
//
//	// Q4: 如何发现收到多数票?
//	// 通过协程进行通信，检查结果
//	go func() {
//		count := 0
//		for range len(rf.peers) {
//			if !rf.checkCandidate() {
//				return
//			}
//			ok := <-ch
//			if ok {
//				count++
//			}
//			if count > len(rf.peers)/2 {
//				// 收到多数票，成为leader
//				rf.BecomeLeader()
//				return
//			}
//		}
//	}()
//}

func (rf *Raft) SendRequestVote(term int, candidateId int) {
	args := make([]RequestVoteArgs, len(rf.peers))
	replies := make([]RequestVoteReply, len(rf.peers))

	ch := make(chan bool, len(rf.peers)-1)
	for i := range rf.peers {
		// correct:不用给自己发RPC(防止票数被算两次)
		if i == rf.me {
			continue
		}
		rf.mu.Lock()
		args[i] = RequestVoteArgs{
			Term:        term,
			CandidateId: candidateId,
		}
		rf.mu.Unlock()
		// Q7: 形成了分区怎么办? 没有接收到响应?
		// 只要至少有一半以上的服务器处于活跃状态并且能够相互通信，Raft 就能继续运行。如果没有达到这一要求，Raft 会暂停当前操作，但一旦半数以上的服务器能够再次通信，它就会继续运行下去。
		// Correct: 请求投票不应该串行发送，网络可能存在分区收不到消息，可能等很久才失败，这时可能没有执行到下一个，自己的选举超时时间就到了(前面的慢节点会阻塞后面的快节点)
		// --> 请求投票应并行扇出
		go rf.sendRequestVoteChannel(i, &args[i], &replies[i], ch)
	}

	count := 1 // 我的Peers不包括我自己，所以count从1开始
	for range len(rf.peers) - 1 {
		if !rf.checkCandidate() {
			return
		}
		granted := <-ch
		if granted {
			count++
			DPrintf("我是%d, 当前任期是%d, 我收到了%d票", candidateId, term, count)
		}
		// Q8: 分区里的节点数是偶数怎么办?
		// 只要至少有一半以上的服务器处于活跃状态并且能够相互通信，Raft 就能继续运行。如果没有达到这一要求，Raft 会暂停当前操作，但一旦半数以上的服务器能够再次通信，它就会继续运行下去。
		if count > len(rf.peers)/2 {
			// 收到多数票，成为leader
			rf.BecomeLeader()
			return
		}
	}
}

func (rf *Raft) BecomeLeader() {
	// 当选后立即发送心跳(空的AppendEntries)，空闲期间也周期性发送
	// Q5: 也许需要先检查是否因为收到新leader的AppendEntries而转为了follower？会出现这种情况吗？
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentState == follower {
		return
	}
	DPrintf("!!!!======我是%d, 我成为了领导者！！！=====!!!!", rf.me)
	rf.currentState = leader
	go rf.SendHeartbeat(rf.currentTerm, rf.me)
}

func (rf *Raft) BecomeCandidate() {
	// 成为candidate，currentTerm+1、给自己投票、重置选举计时器
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentState = candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.resetElectionTimer()
	// 发起选举(向其他服务器发送RequestVote RPC)
	go rf.SendRequestVote(rf.currentTerm, rf.me)
}

const heartbeatInterval = 125
const electionTimeoutBase = 500

// SendHeartbeat leader当选后立即发送新空的AppendEntries(心跳)，空闲期间也周期性发送
func (rf *Raft) SendHeartbeat(term int, candidateId int) {
	// 立即发送心跳
	rf.SendAppendEntriesHeartbeat(term, candidateId)
	// 周期性发送心跳
	time.Sleep(heartbeatInterval * time.Millisecond)
	_, isLeader := rf.GetState()
	for isLeader {
		rf.SendAppendEntriesHeartbeat(term, candidateId)
		// pause for a solid amount of time
		// Q2: 心跳间隔时间?
		// 测试要求领导者每秒发送的心跳 RPC 次数不得超过十次
		time.Sleep(heartbeatInterval * time.Millisecond)
		_, isLeader = rf.GetState()
	}
}

type AppendEntriesArgs struct {
	Term     int // leader任期
	LeaderId int // leader的ID
}

type AppendEntriesReply struct {
	Term int // 供leader更新自己的currentTerm
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (3A, 3B).
	// 若RPC请求或响应中的term T > currentTerm: 另currentTerm=T，并转换为follower
	rf.checkAndUpdateTerm(args.Term)
	// 收到来自leader的日志消息或心跳 -> 刷新时间
	// candidate如果收到新leader的AppendEntries，需要转为follower
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	rf.resetElectionTimer()
	if rf.currentState == candidate {
		rf.currentState = follower
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.checkAndUpdateTerm(reply.Term)
	}
}

func (rf *Raft) SendAppendEntriesHeartbeat(term int, candidateId int) {
	for i := range rf.peers {
		arg := AppendEntriesArgs{
			Term:     term,
			LeaderId: candidateId,
		}
		reply := AppendEntriesReply{}
		go rf.sendAppendEntries(i, &arg, &reply)
	}
	return
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

func (rf *Raft) ticker() {
	for true {

		// Your code here (3A)
		// Check if a leader election should be started.
		// Q3: 选举超时时间？
		// such a range(150 to 300 milliseconds) only makes sense if the leader sends *heartbeats* considerably more often than once per 150 milliseconds(e.g., once per 10 milliseconds).
		// follower若在选举超时时间内没有收到当前leader的AppendEntries，也没有给candidate投票，则转为candidate
		// candidate若再次超时，发起新一轮选举

		needElection := false
		rf.mu.Lock()
		// Correct: 不应该把投票状态与选举超时混为一谈，是否给候选人投过票应该通过重置计时器来考虑
		// 论文里那句“without ... granting vote to candidate”，不是说“只要 votedFor != -1 就不能超时”，而是说"成功投票这个事件应该重置 election timer"。
		// 也就是说，投票发生时应该把 deadline 往后推
		// 我投过票只限制同一个 term 内还能不能再投给别人；它不限制未来因为超时进入下一个 term。
		// votedFor 实际上只是当前 term 的投票记录。
		if rf.currentState != leader && time.Now().After(rf.electionDeadline) {
			needElection = true
			DPrintf("我是%d, 当前任期是%d, 现在计时器超时了，我成为了候选人", rf.me, rf.currentTerm)
		}
		rf.mu.Unlock()

		if needElection {
			rf.BecomeCandidate()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1

	rf.currentState = follower
	rf.resetElectionTimer()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	// 该协程会在一段时间内没有收到来自其他节点的消息时，定期发送RequestVote RPC请求来触发领导选举
	go rf.ticker()

	return rf
}

// helpers

func (rf *Raft) resetElectionTimer() {
	electionTimeout := time.Duration(electionTimeoutBase+rand.Int63()%700) * time.Millisecond
	rf.electionDeadline = time.Now().Add(electionTimeout)
}

func (rf *Raft) checkAndUpdateTerm(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if term <= rf.currentTerm {
		return
	}
	DPrintf("我是%d, 我原先的状态是%v, 我原先的任期是%d, 我将成为follower, 我之后的任期是%d", rf.me, rf.currentState, rf.currentTerm, term)
	rf.currentTerm = term
	rf.votedFor = -1
	rf.currentState = follower
}

func (rf *Raft) checkCandidate() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentState == candidate
}
