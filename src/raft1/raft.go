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
	"slices"
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
	peers     []*labrpc.ClientEnd // RPC end points of all peers, 也包括当前这个服务器
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state
	currentTerm int    // 当前已知最新任期，首次启动为0，只增不减
	votedFor    int    // 当前任期投给了谁；若未投票则为-1
	logs        []*Log // 日志条目；每项包含状态机命令、任期号；索引从1开始

	// volatile state
	commitIndex      int       // 已知已提交的最高日志索引
	lastApplied      int       // 已应用到状态机的最高日志索引
	currentState     state     // 当前状态
	electionDeadline time.Time // 选举超时时间

	// volatile state on leaders
	nextIndex  []int // 对每个follower, 下一个要发送的日志索引（初始化为leader最后一条日志索引+1）--> 下一次从哪里开始给 follower i 发日志
	matchIndex []int // 对每个follower，已知复制成功的最高日志索引 --> 已经确认 follower i 复制到了哪里

	lastLogIndex int // 不确定是否需要，只是方便拿取；值为len(rf.logs)-1
	lastLogTerm  int // 不确定是否需要，只是方便拿取；值为rf.logs[rf.lastLogIndex].Term

	// 我的临时变量 - channel
	startCh chan struct{}
	// 不确定是不是persistent
	applyCh chan raftapi.ApplyMsg // 用来和上层状态机通信的channel(可以发送ApplyMsg消息)
}

type Log struct {
	Command any // 状态机命令
	Term    int // 任期号
}

type state int

const (
	follower state = iota
	candidate
	leader
)

const (
	heartbeatInterval   = 125
	electionTimeoutBase = 500
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

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
	Term         int // 候选人的任期
	CandidateId  int // 候选人的ID
	LastLogIndex int // 候选人最后一条日志索引
	LastLogTerm  int // 候选人最后一条日志的任期
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
	rf.checkAndUpdateTerm(args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	// 如果 term < currentTerm，拒绝投票
	if args.Term < rf.currentTerm {
		return
	}
	// 如果已投票并且不是投给了该candidate，则拒绝投票
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		return
	}
	// 1. 如果尚未投票或已投给该candidate，且候选人的日志至少和自己一样新，则授予选票
	// 2. Raft通过比较日志最后一个条目的term和index来确定哪个日志更"新"。如果日志最后一个条目的term不同，那么term更大的日志更"新"；如果term相同，那么日志更长(index更大)的日志更"新"。
	if (args.LastLogTerm < rf.lastLogTerm) ||
		(args.LastLogTerm == rf.lastLogTerm && args.LastLogIndex < rf.lastLogIndex) {
		return
	}
	// 接受
	rf.votedFor = args.CandidateId
	// Correct: 如果投出过票，重置计时器(防止刚投出票就计时器到期重新发起选举，打断被投票人的选举)
	rf.resetElectionTimer()
	reply.VoteGranted = true
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
		rf.checkAndUpdateTerm(reply.Term)
		ch <- reply.VoteGranted
	}
}

func (rf *Raft) SendRequestVote(term int, candidateId int, lastLogTerm int, lastLogIndex int) {
	ch := make(chan bool, len(rf.peers)-1)
	for i := range rf.peers {
		// correct:不用给自己发RPC(防止票数被算两次)
		if i == rf.me {
			continue
		}
		// Q7: 形成了分区怎么办? 没有接收到响应?
		// 只要至少有一半以上的服务器处于活跃状态并且能够相互通信，Raft 就能继续运行。如果没有达到这一要求，Raft 会暂停当前操作，但一旦半数以上的服务器能够再次通信，它就会继续运行下去。
		// Correct: 请求投票不应该串行发送，网络可能存在分区收不到消息，可能等很久才失败，这时可能没有执行到下一个，自己的选举超时时间就到了(前面的慢节点会阻塞后面的快节点) --> 请求投票应并行扇出
		args := &RequestVoteArgs{
			Term:         term,
			CandidateId:  candidateId,
			LastLogTerm:  lastLogTerm,
			LastLogIndex: lastLogIndex,
		}
		reply := &RequestVoteReply{}
		go rf.sendRequestVoteChannel(i, args, reply, ch)
	}

	count := 1 // 我的Peers不包括我自己，所以count从1开始
	for range len(rf.peers) - 1 {
		voteGranted := <-ch
		if !rf.checkCandidate() {
			return
		}
		if voteGranted {
			count++
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
	rf.currentState = leader

	// 日志处理
	// 当leader首次掌权时，它会将所有的nextIndex值初始化为其日志的最后一个条目的下一个index。
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.logs)
	}

	go rf.tickerHeartbeat()
	go rf.ListenAndSendAppendEntries(rf.currentTerm)
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
	go rf.SendRequestVote(rf.currentTerm, rf.me, rf.lastLogTerm, rf.lastLogIndex)
}

type AppendEntriesArgs struct {
	Term         int    // leader任期
	LeaderId     int    // leader的ID
	PrevLogIndex int    // 新日志之前一条日志的索引
	PrevLogTerm  int    // prevLogIndex对应日志的任期
	Entries      []*Log // 要追加的日志条目(心跳时可为空，可一次发送多条)
	LeaderCommit int    // leader的commitIndex
}

type AppendEntriesReply struct {
	Term    int  // 供leader更新自己的currentTerm
	Success bool // 如果follower在prevLogIndex处存在任期匹配的日志，则为true
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (3A, 3B).
	rf.checkAndUpdateTerm(args.Term)
	// 1. 如果收到来自leader的日志消息或心跳，需要刷新计时器
	// 2. candidate如果收到新leader的AppendEntries，需要转为follower
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

	// summary: 什么情况下会AppendEntries的Reply是false:
	// 1. 不是leader了(args.Term < rf.currentTerm)
	// 2. follower一致性校验失败(在prevLogIndex处不存在日志，或该位置任期不等于prevLogTerm)

	// 日志处理
	// 若在 prevLogIndex 处不存在日志，或该位置任期不等于prevLogTerm，返回false
	// Correct: Leader 本次发送的所有 Entries，其 term 不一定都等于 args.Term。
	if rf.lastLogIndex < args.PrevLogIndex || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		return
	}
	// 若新日志与已有日志冲突(相同索引但任期不同)，删除该旧日志及其后续日志，并追加所有本地还没有的新日志
	// Correct: 注意Raft复制应该确保: 同一个 AppendEntries RPC 即使重复到达，也不应该让日志重复增长。
	for i := range args.Entries {
		logIndex := args.PrevLogIndex + 1 + i
		if logIndex >= len(rf.logs) || rf.logs[logIndex].Term != args.Entries[i].Term {
			rf.logs = append(rf.logs[:logIndex], args.Entries[i:]...)
			break
		}
	}
	rf.resetLastLog()

	// 若leaderCommit > commitIndex, 则令 commitIndex=min(leaderCommit, 最后一条新日志索引)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndex)
	}

	// 将已提交的日志应用到状态机
	go rf.applyCommitLog()

	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.checkAndUpdateTerm(reply.Term)
	}
	return ok
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
func (rf *Raft) Start(command any) (int, int, bool) {
	// Your code here (3B).
	// Start函数 -> 发起共识(而不是已经达成共识)
	// 每个客户端请求包含一条需要被复制状态机执行的命令
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// the index that the command will appear at if it's ever committed
	index, currentTerm, isLeader := rf.lastLogIndex+1, rf.currentTerm, rf.currentState == leader
	if !isLeader {
		return -1, -1, false
	}
	// 将该指令作为新的条目追加到本地日志中
	rf.logs = append(rf.logs, &Log{
		Command: command,
		Term:    currentTerm,
	})
	rf.matchIndex[rf.me] = len(rf.logs) - 1
	rf.resetLastLog()
	// 通知日志复制协程有新的日志了
	rf.notifyStartChReady()
	return index, currentTerm, isLeader
}

func (rf *Raft) notifyStartChReady() {
	select {
	case rf.startCh <- struct{}{}:
	default:
		// 已经有一个 ready 信号了，不需要重复塞
	}
}

func (rf *Raft) ListenAndSendAppendEntries(term int) {
	// 如果有多个复制请求，那么只有一个在复制，其他请求被拦截在外面？（也就是说，复制请求只是起到signal的作用）
	// 什么时候有复制请求：1. 请求来了 2. 心跳
	_, isLeader := rf.GetState()
	for isLeader {
		select {
		case <-rf.startCh:
			rf.sendAppendEntriesToFollowers(term)
		}
		_, isLeader = rf.GetState()
	}
}

func (rf *Raft) sendAppendEntriesToFollowers(term int) {
	// 并行地向其他服务器发起AppendEntries RPC复制该条目
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go rf.startConsistent(server, term)
	}
}

func (rf *Raft) startConsistent(server int, term int) {
	// 触发时间：周期性或在新日志到来时
	// 作用：根据 nextIndex[follower] 尝试 AppendEntries；失败就调整复制起点，成功就推进 matchIndex/nextIndex。

	// 如果follower崩溃或运行缓慢，或者如果网络包丢失，无限重试AppendEntries RPC，直到follower最终存储了所有的日志条目。
	_, isLeader := rf.GetState()
	for isLeader {
		rf.mu.Lock()
		lastLogIndex := len(rf.logs) - 1
		// copy entries, not transfer entries
		entries := make([]*Log, len(rf.logs[rf.nextIndex[server]:]))
		copy(entries, rf.logs[rf.nextIndex[server]:])
		args := &AppendEntriesArgs{
			Term:         term,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[server] - 1,
			PrevLogTerm:  rf.logs[rf.nextIndex[server]-1].Term,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		reply := &AppendEntriesReply{}
		rf.mu.Unlock()
		ok := rf.sendAppendEntries(server, args, reply)
		if !ok {
			// rpc失败，可能是形成了分区，也可能是请求或响应的数据包丢失
			// 一段时间后重新尝试发送RPC请求
			time.Sleep(50 * time.Millisecond)
			continue
		}
		// rpc成功，检查响应
		if reply.Success {
			// 一致性校验成功，复制成功
			// 最终nextIndex达到leader和follower的日志匹配的点，AppendEntries成功(删除了follower日志中的冲突条目，并将leader的日志条目追加到follower的日志中)
			// 复制成功：更新该follower的nextIndex和matchIndex
			rf.mu.Lock()
			rf.nextIndex[server] = max(rf.nextIndex[server], lastLogIndex+1)
			rf.matchIndex[server] = max(rf.matchIndex[server], lastLogIndex)
			// correct: RPC reply 描述的是“请求发送时那个世界”的结果，不一定描述“reply 到达时当前世界”的状态。
			commitIndexN := rf.findCommitIndexN(lastLogIndex)
			if commitIndexN > rf.commitIndex {
				rf.commitIndex = commitIndexN
				// 将已提交的日志应用到状态机
				go rf.applyCommitLog()
			}
			rf.mu.Unlock()
			break
		} else {
			// 一致性校验失败
			// Correct: 一致性校验失败也可能是其他原因：不是leader了
			rf.mu.Lock()
			rf.nextIndex[server] = max(rf.nextIndex[server]-1, 1)
			rf.mu.Unlock()
		}
		_, isLeader = rf.GetState()
	}
}

func (rf *Raft) findCommitIndexN(lastLogIndex int) int {
	// Q10: commitIndex是？
	// 若存在某个N，满足N>commitIndex，且多数matchIndex[i]>=N，并且logs[N].Term==currentTerm，则令commitIndex=N
	// 成功事件数不代表服务器数
	for i := lastLogIndex; i > rf.commitIndex; i-- {
		count := 0
		for j := range rf.peers {
			if rf.matchIndex[j] >= i && rf.logs[i].Term == rf.currentTerm {
				count++
				if count > len(rf.peers)/2 {
					return i
				}
			}
		}
	}
	return -1
}

func (rf *Raft) majorityLowerBound() int {
	values := slices.Clone(rf.matchIndex)
	slices.Sort(values)

	n := len(values)
	count := n/2 + 1

	index := n - count
	for rf.logs[index].Term != rf.currentTerm && index >= 0 {
		index--
	}
	return index
}

// 对所有服务器，若 commitIndex > lastApplied：递增 lastApplied，并将log[lastApplied]应用到状态机
func (rf *Raft) applyCommitLog() {
	// Q9: 如何应用到状态机?
	// 当 commitIndex 增长以后，要把 lastApplied+1 ... commitIndex 之间的日志，按顺序通过 applyCh 交给状态机执行。
	// Correct: commitIndex 可以由很多并发事件推动，但 lastApplied → state machine 这条流水线本质上应该是串行的、有序的。 TODO
	for {
		rf.mu.Lock()
		if rf.commitIndex <= rf.lastApplied {
			rf.mu.Unlock()
			break
		}
		commandIndex := rf.lastApplied + 1
		command := rf.logs[commandIndex].Command
		rf.mu.Unlock()

		rf.applyCh <- raftapi.ApplyMsg{CommandValid: true, Command: command, CommandIndex: commandIndex}

		rf.mu.Lock()
		rf.lastApplied = max(rf.lastApplied, commandIndex)
		rf.mu.Unlock()
	}
}

// tickerHeartbeat leader当选后立即发送新空的AppendEntries(心跳)，空闲期间也周期性发送
func (rf *Raft) tickerHeartbeat() {
	// 立即发送心跳
	rf.notifyStartChReady()
	// 周期性发送心跳
	time.Sleep(heartbeatInterval * time.Millisecond)
	_, isLeader := rf.GetState()
	for isLeader {
		rf.notifyStartChReady()
		// Q2: 心跳间隔时间?
		// 测试要求领导者每秒发送的心跳 RPC 次数不得超过十次
		time.Sleep(heartbeatInterval * time.Millisecond)
		_, isLeader = rf.GetState()
	}
}

func (rf *Raft) ticker() {
	for {

		// Your code here (3A)
		// Check if a leader election should be started.
		if rf.checkNeedElection() {
			rf.BecomeCandidate()
		}
		// 睡眠一会再检查
		time.Sleep(50 * time.Millisecond)
	}
}

func (rf *Raft) checkNeedElection() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 1. follower若在选举超时时间内没有收到当前leader的AppendEntries，也没有给candidate投票，则转为candidate
	// 2. candidate若再次超时，发起新一轮选举
	// Correct: 不应该考虑是否投过票-votedFor只是当前任期的投票记录，但是计时器超时时(说明没有更新)仍应该发起选举。如果要考虑给其他候选人投了票，成功投票这个事件应该去重置选举超时计时器
	// 我投过票只限制同一个 term 内还能不能再投给别人；它不限制未来因为超时进入下一个 term。
	return rf.currentState != leader && time.Now().After(rf.electionDeadline)
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
	rf.logs = []*Log{
		{Term: rf.currentTerm},
	}

	rf.currentState = follower

	rf.applyCh = applyCh
	rf.startCh = make(chan struct{}, 1)

	rf.resetElectionTimer()
	rf.resetLastLog()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	// 该协程会在一段时间内没有收到来自其他节点的消息时，定期发送RequestVote RPC请求来触发领导选举
	go rf.ticker()

	return rf
}

// helpers

func (rf *Raft) resetElectionTimer() {
	// Q3: 选举超时时间？
	// such a range(150 to 300 milliseconds) only makes sense if the leader sends *heartbeats* considerably more often than once per 150 milliseconds(e.g., once per 10 milliseconds).
	electionTimeout := time.Duration(electionTimeoutBase+rand.Int63()%700) * time.Millisecond
	rf.electionDeadline = time.Now().Add(electionTimeout)
}

func (rf *Raft) checkAndUpdateTerm(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if term <= rf.currentTerm {
		return
	}
	rf.currentTerm = term
	rf.votedFor = -1
	rf.currentState = follower
}

func (rf *Raft) checkCandidate() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentState == candidate
}

func (rf *Raft) resetLastLog() {
	rf.lastLogIndex = len(rf.logs) - 1 //已知有一个dummy
	rf.lastLogTerm = rf.logs[rf.lastLogIndex].Term
}
