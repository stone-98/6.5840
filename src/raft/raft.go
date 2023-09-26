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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	// 任期号
	Term int
	// 日志条目的实体
	Command interface{}
}

const (
	Role_Follower  = 0 // follower role
	Role_Candidate = 1 // candidate role
	Role_Leader    = 2 // leader role
)

func getRole(role int) string {
	switch role {
	case Role_Follower:
		return "Follower"
	case Role_Candidate:
		return "Candidate"
	case Role_Leader:
		return "Leader"
	default:
		return "unknown"
	}
}

const (
	ElectionTimeout   = time.Millisecond * 300 // 选举超时时间/心跳超时时间
	HeartBeatInterval = time.Millisecond * 150 // leader 发送心跳
	ApplyInterval     = time.Millisecond * 100 // apply log
	RPCTimeout        = time.Millisecond * 100
	MaxLockTime       = time.Millisecond * 10 // debug
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 当前服务器的角色
	role int // current server role.
	// 当前服务器接收到的最新任期
	currentTerm int // latest term server has seen candidateId that received vote in current term.
	// 当前任期内，我给谁进行了投票
	voteFor int // candidateId that received vote in current term.
	// 每个服务器节点上都有相应的日志条目
	logs []LogEntry // log entries;each entry contains command for state machine, and term when entry was received by leader.

	// volatile state on all servers.(所有服务器上容易丢失的状态)

	// 当前服务器已知已提交的最高日志条目的索引
	commitIndex int // index of highest log entry known to be committed.
	// 应用到状态机的最高日志条目的索引
	lastApplied int // index of heighest log entry applied to state machine.

	// leader才拥有的状态

	// 对于每个服务器，发送到该服务器的下一个日志条目的索引
	nextIndex []int // for each server,index of the next log entry to send to that server.
	// 对于每个服务器，已知服务器上复制的最高日志条目的索引
	matchIndex []int // for each server,index of highest log entry known to be replicated on server.

	// 选举计时器
	electionTimer *time.Timer
	// 发送日志的定时器
	appendEntriesTimers []*time.Timer
	// todo
	applyTimer    *time.Timer
	applyCh       chan ApplyMsg
	notifyApplyCh chan struct{}
	stopCh        chan struct{}

	lastSnapshotIndex int // 快照中最后一条日志的index、可以理解为状态机的index
	lastSnapshotTerm  int // 快照中最后一个任期
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	flag := false
	if rf.role == Role_Leader {
		flag = true
	}
	return rf.currentTerm, flag
}

// 返回一个随机的超时时间，范围为 ElectionTimeout ~ 2 * ElectionTimeout
func (rf *Raft) getElectionTimeout() time.Duration {
	t := ElectionTimeout + time.Duration(rand.Int63())%ElectionTimeout
	return t
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
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
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// 候选者的任期
	Term int // candidate's term
	// 候选者请求投票
	CandidateId int // candidate requesting vote
	// 候选者最后一个日志条目的索引
	LastLogIndex int // index of candidate's last log entry
	// 候选者最后一个日志条目的任期
	LastLogTerm int // term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	lastLogTerm, lastLogIndex := rf.getLastLogTermAndIndex()
	// 默认失败返回
	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	if rf.currentTerm > args.Term {
		return
	} else if rf.currentTerm == args.LastLogTerm {
		if rf.role == Role_Leader {
			return
		}

		if args.CandidateId == rf.voteFor {
			reply.Term = args.Term
			reply.VoteGranted = true
		}

		if rf.voteFor != -1 && args.CandidateId != rf.voteFor {
			return
		}
		// 还未投过票
	}

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.changeRole(Role_Follower)
		rf.voteFor = -1
		reply.Term = rf.currentTerm
		rf.persist()
	}

	// 判断日志的完整性 todo 如果当前的大于请求中的lastLogIndex就直接返回失败，感觉这里有点问题的啊
	if lastLogTerm > args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogTerm) {
		return
	}
	rf.voteFor = args.CandidateId
	rf.changeRole(Role_Follower)

	reply.VoteGranted = true
	reply.Term = args.Term
	rf.resetElectionTimer()
	rf.persist()
	DPrintf("me: %v role：%v, voteFor: %v", rf.me, getRole(rf.role), rf.voteFor)
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	if server < 0 || server > len(rf.peers) {
		panic("server invalid in sendRequestVote!")
	}
	rpcTimer := time.NewTimer(RPCTimeout)
	defer rpcTimer.Stop()

	ch := make(chan bool, 1)
	go func() {
		for i := 0; i < 10; i++ {
			ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
			if ok {
				ch <- ok
				return
			} else {
				continue
			}
		}
	}()

	select {
	case <-rpcTimer.C:
		DPrintf("%v role: %v, send request vote to peer %v TIME OUT!!!", rf.me, getRole(rf.role), server)
		return false
	case <-ch:
		return true
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	// 选举定时
	go func() {
		for rf.killed() == false {

			// Your code here (2A)
			// Check if a leader election should be started.

			// pause for a random amount of time between 50 and 350
			// milliseconds.
			// 如果当前节点等待指定时间内没有接收到leader的请求，则开始选举
			select {
			case <-rf.stopCh:
				return
			case <-rf.electionTimer.C:
				rf.startElection()
			}
		}
	}()

	//leader发送日志定时
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(cur int) {
			for rf.killed() == false {
				select {
				case <-rf.stopCh:
					return
				case <-rf.appendEntriesTimers[cur].C:
					rf.sendAppendEntriesToPeer(cur)
				}
			}
		}(i)
	}
}

// 首先判断是否为leader，如果是leader就没必要再发起选举了
// 修改角色为候选者
// 分别为每个peer创建一个goroutine并且调用sendRequestVote函数发送给每一个其他节点，相应由grantedChan进行接收
func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.resetElectionTimer()
	// 已经成为leader，不需要再进行选举了
	if rf.role == Role_Leader {
		rf.mu.Unlock()
		return
	}
	// 改变节点状态到candidate
	rf.changeRole(Role_Candidate)
	DPrintf("me: %v role %v, start election, term: %v", rf.me, getRole(rf.role), rf.currentTerm)
	lastLogTerm, lastLogIndex := rf.getLastLogTermAndIndex()
	args := RequestVoteArgs{
		CandidateId:  rf.me,
		Term:         rf.currentTerm,
		LastLogTerm:  lastLogTerm,
		LastLogIndex: lastLogIndex,
	}
	rf.persist()
	rf.mu.Unlock()
	// peers count.(所有的peers减去自己)
	allCount := len(rf.peers)
	grantedCount := 1
	resCount := 1
	grantedChan := make(chan bool, len(rf.peers))
	for i := 0; i < allCount; i++ {
		if i == rf.me {
			continue
		}
		// 对每个peer发送rpc请求
		go func(gch chan bool, index int) {
			reply := RequestVoteReply{}
			rf.sendRequestVote(index, &args, &reply)
			gch <- reply.VoteGranted
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				// 已有更大的任期了，放弃选举
				rf.currentTerm = reply.Term
				rf.changeRole(Role_Follower)
				rf.voteFor = -1
				rf.resetElectionTimer()
				rf.persist()
			}
			rf.mu.Unlock()
		}(grantedChan, i)
	}

	// 如果当前角色是候选者
	for rf.role == Role_Candidate {
		flag := <-grantedChan
		resCount++
		if flag {
			grantedCount++
		}
		DPrintf("me: %v vote: %v, allCount: %v, resCount: %v, grantedCount: %v", rf.me, flag, allCount, resCount, grantedCount)
		if grantedCount > allCount/2 {
			// 竞选成功
			rf.mu.Lock()
			DPrintf("me: %v before try change to leader, count: %d, args: %+v, currentTerm: %v, argsTerm: %v", rf.me, grantedCount, args, rf.currentTerm, args.Term)
			if rf.role == Role_Candidate && rf.currentTerm == args.Term {
				rf.changeRole(Role_Leader)
			}
			if rf.role == Role_Leader {
				rf.resetAppendEntriesTimersZero()
			}
			rf.persist()
			rf.mu.Unlock()
			DPrintf("me: %v current role: %v", rf.me, getRole(rf.role))
		} else if resCount == allCount || resCount-grantedCount > allCount/2 {
			DPrintf("me: %v grant fail! grantedCount <= len/2:count:%d", rf.me, grantedCount)
			return
		}
	}
}

func (rf *Raft) resetAppendEntriesTimersZero() {
	for _, timer := range rf.appendEntriesTimers {
		timer.Stop()
		timer.Reset(0)
	}
}

// 重置选举计时器
func (rf *Raft) resetElectionTimer() {
	DPrintf("me: %v reset election timer.", rf.me)
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(rf.getElectionTimeout())
}

// 返回当前状态机的最后一条日志的任期和索引
// 索引一直递增，但是我们的日志队列却不可能无线增大，再队列中下标0存储快照
func (rf *Raft) getLastLogTermAndIndex() (int, int) {
	// 这个是日志最后一个数据的任期， 这个是快照最后一个任期
	return rf.logs[len(rf.logs)-1].Term, rf.lastSnapshotIndex + len(rf.logs) - 1
}

// 改变服务器角色
func (rf *Raft) changeRole(newRole int) {
	if newRole < 0 || newRole > 3 {
		panic("unknown role.")
	}
	rf.role = newRole
	switch newRole {
	case Role_Follower:
	case Role_Candidate:
		// 如果成为候选者
		// 增加当前任期
		// 投票给自己
		// 重置选举时间
		rf.currentTerm++
		rf.voteFor = rf.me
		rf.resetElectionTimer()
	case Role_Leader:
		// leader只有两个特殊的数据结构: nextIndex,matchIndex
		_, lastLogIndex := rf.getLastLogTermAndIndex()
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = lastLogIndex + 1
			rf.matchIndex[i] = lastLogIndex
		}
		rf.resetElectionTimer()
	default:
		panic("unknown role")
	}
}

type AppendEntriesArgs struct {
	Term         int        // 领导人的任期
	LeaderId     int        // 领导者的ID可以对客户端请求进行重定向（有时候客户端把请求发给了跟随着而不是领导者，则通过leaderId进行重定向）
	PrevLogIndex int        // 紧邻新日志条目之前的那个日志条目的索引
	PrevLogTerm  int        // 紧邻新日志条目之前的那个日志条目的任期
	Entries      []LogEntry // 需要被保存的日志条目
	LeaderCommit int        // 领导人已知已提交的最高的日志条目的索引
}

type AppendEntriesReply struct {
	Term      int  // 当前任期，对于领导人而言，他会更新自己的任期
	Success   bool // 如果跟随者所含的条目和preLogIndex和preLogTerm匹配上了，则为true
	NextIndex int  // 这个应该是告知leader我需要的日志条目是多少
}

// 获取要向指定节点发送的日志
func (rf *Raft) getAppendLogs(peerId int) (prevLogIndex int, prevLogTerm int, logEntries []LogEntry) {
	nextIndex := rf.nextIndex[peerId]
	lastLogTerm, lastLogIndex := rf.getLastLogTermAndIndex()
	// 如果下一个需要发送的索引小于或等于快照中下一个索引或者下一个索引
	// 下一个需要发送的索引大于
	if nextIndex <= rf.lastSnapshotIndex || nextIndex > lastLogIndex {
		// 没有要发送的log
		prevLogTerm = lastLogTerm
		prevLogIndex = lastLogIndex
		return
	}
	logEntries = make([]LogEntry, lastLogIndex-nextIndex+1)
	copy(logEntries, rf.logs[nextIndex-rf.lastSnapshotIndex:])
	prevLogIndex = nextIndex - 1
	if prevLogIndex == rf.lastSnapshotIndex {
		prevLogTerm = rf.lastSnapshotTerm
	} else {
		prevLogTerm = rf.logs[prevLogIndex-rf.lastSnapshotIndex].Term
	}
	return
}

// 重置timer
func (rf *Raft) resetAppendEntriesTimer(peerId int) {
	rf.appendEntriesTimers[peerId].Stop()
	rf.appendEntriesTimers[peerId].Reset(HeartBeatInterval)
}

// 当选举leader成功了，那么则定时发送心跳
func (rf *Raft) sendAppendEntriesToPeer(peerId int) {
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	if rf.role != Role_Leader {
		// 如果说我不是leader我就重置
		rf.resetAppendEntriesTimer(peerId)
		rf.mu.Unlock()
		return
	}
	DPrintf("me: %v send append entries to peer %v", rf.me, peerId)
	prevLogIndex, prevLogTerm, logEntries := rf.getAppendLogs(peerId)
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      logEntries,
		LeaderCommit: rf.commitIndex,
	}
	reply := AppendEntriesReply{}
	rf.resetAppendEntriesTimer(peerId)
	rf.mu.Unlock()
	//发送rpc
	rf.sendAppendEntries(peerId, &args, &reply)

	DPrintf("me: %v role: %v, send append entries to peer finish,%v,args = %+v,reply = %+v", rf.me, getRole(rf.role), peerId, args, reply)

	rf.mu.Lock()
	if reply.Term > rf.currentTerm {
		rf.changeRole(Role_Follower)
		rf.currentTerm = reply.Term
		rf.resetElectionTimer()
		rf.persist()
		rf.mu.Unlock()
		return
	}

	if rf.role != Role_Leader || rf.currentTerm != args.Term {
		rf.mu.Unlock()
		return
	}

	//如果有日志的话，接下来要做的是对日志进行处理，这里暂时可以不用写
	rf.mu.Unlock()
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rpcTimer := time.NewTimer(RPCTimeout)
	defer rpcTimer.Stop()

	ch := make(chan bool, 1)
	go func() {
		for i := 0; i < 10 && !rf.killed(); i++ {
			ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
			if !ok {
				time.Sleep(time.Millisecond * 10)
				continue
			} else {
				ch <- ok
				return
			}
		}
	}()

	select {
	case <-rpcTimer.C:
		DPrintf("%v role: %v, send append entries to peer %v TIME OUT!!!", rf.me, rf.role, server)
		return
	case <-ch:
		return
	}
}

// 目前只处理了心跳，所以不需要其他的判断
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	DPrintf("me: %v receive a appendEntries: %+v", rf.me, args)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	rf.currentTerm = args.Term
	rf.changeRole(Role_Follower)
	rf.resetElectionTimer()
	reply.Success = true

	rf.persist()
	DPrintf("me: %v role: %v, get appendentries finish,args = %v,reply = %+v", rf.me, getRole(rf.role), *args, *reply)
	rf.mu.Unlock()
}

// Make the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
// 中文翻译：
// peers：包含所有的节点的服务器信息
// me: 自己节点所在的索引
// persister：当服务器崩溃时，通过持久化恢复到崩溃之前的状态
// applyCh：测试人员希望发送的。。。todo
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	DPrintf("make a rafr, me: %v", me)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.role = Role_Follower
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.logs = make([]LogEntry, 1)
	// 初始化...
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	// initialize from state persisted before a crash, 读取持久化数据，暂时不进行实现
	rf.readPersist(persister.ReadRaftState())

	rf.electionTimer = time.NewTimer(rf.getElectionTimeout())
	rf.appendEntriesTimers = make([]*time.Timer, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.appendEntriesTimers[i] = time.NewTimer(HeartBeatInterval)
	}
	rf.applyTimer = time.NewTimer(ApplyInterval)
	rf.applyCh = applyCh
	rf.notifyApplyCh = make(chan struct{}, 100)
	rf.stopCh = make(chan struct{})
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
