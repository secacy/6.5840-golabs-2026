## Lab3: raft

系列实验：构建一个具有容错能力的键值存储系统

本实验是系列实验的第一个实验，在这里，你将实现 Raft 协议(复制状态机协议)
在下一个实验中，你将在 raft 的基础上构建出一个键值服务。之后，你会将服务拆分成多个复制状态机来提高性能。

通过在不同数量的副本服务器上存储状态的完整副本（即数据），复制服务能够实现容错性。不过，问题在于，当服务器发生故障时，副本中存储的数据可能会有差异。

Raft 将客户端请求按顺序整理起来，形成一个日志序列，并确保所有副本服务器都能看到相同的日志内容。如果某个服务器发生故障但随后恢复，Raft 会负责更新其日志内容。
只要至少有一半以上的服务器处于活跃状态并且能够相互通信，Raft 就能继续运行。如果没有达到这一要求，Raft 会暂停当前操作，但一旦半数以上的服务器能够再次通信，它就会继续运行下去。

你的 Raft 接口将支持一系列编号化的命令，这些命令也被称为日志条目。每个日志条目都会有一个索引号。具有特定索引的日志条目最终会被提交。此时，你的 Raft 应该将这条日志条目发送给更大的服务进行执行。



代码修改点：raft1/raft.go


### Lab 3A: 领导者选举
任务：实现 raft 协议的领导者选举机制以及心跳检测功能。
目标：要选出一个领导者，如果领导者没有故障则保持是领导者，如果旧领导者故障或与旧领导者间的通信数据包丢失，则新领导者接替其位置。
并发读写数据：多个 goroutine 并发访问同一份共享内存时，只要存在写操作，就必须做同步。

注意：
1. 若RPC请求或响应中的term T > currentTerm: 令currentTerm=T，并转换为follower

keys: 
1. 请求投票不应该串行发送。网络可能存在分区收不到消息，可能等很久才失败，这时可能还没有执行到下一个，自己的选举超时时间就到了(前面的慢节点会阻塞后面的快节点) --> 请求投票应并行扇出
2. 发起新一轮选举：不应该考虑是否投过票-votedFor只是当前任期的投票记录，但是计时器超时时(说明没有更新)仍应该发起选举。如果要考虑给其他候选人投了票，成功投票这个事件应该去重置选举超时计时器 我投过票只限制同一个 term 内还能不能再投给别人；它不限制未来因为超时进入下一个 term。

debug helpers:
1. log
2. 可视化文件(If you fail a test, the tester produces a file that visualizes a timeline with events marked along it, including network partitions, crashed servers, and checks performed. Here's an example of the visualization. )

summary:
什么时候需要重置计时器
1. 发起新一轮选举
2. 收到来自当前或更新任期的leader的RPC(说明leader没挂)
3. 收到请求投票RPC并投出合法的票

### Lab 3B: 日志
任务: Implement the leader and follower code to append new log entries
描述：首先实现 Start() 功能，然后编写代码通过 AppendEntries RPC 来发送和接收新的日志条目，具体流程请参考图 2。每个节点上都要按照 applyCh 的规则处理新提交的条目。
Hint:
1. 在实现日志时，可以先在 log[0] 放一个dummy entry，且它的 term 设为 0。这样可以避免很多边界情况。这样，第一个 AppendEntries RPC 就可以将 0 作为 PrevLogIndex
2. 你需要实现选举限制功能（参见论文中的 5.4.1 节）。
3. 你的代码中可能会有一些循环，这些循环会反复检查某些事件。不要让这些循环持续运行而不暂停，因为这样会导致代码执行速度过于缓慢，从而让测试无法通过。可以使用 Go 语言的条件变量，或者在每次循环迭代时插入一个 time.Sleep(10 * time.Millisecond) 符号。

summary: 什么情况下会AppendEntries的Reply是false:
1. 不是leader了(args.Term < rf.currentTerm)
2. follower一致性校验失败(在prevLogIndex处不存在日志，或该位置任期不等于prevLogTerm)

notes:
1. **网络RPC到达可能乱序、丢失、重复**
2. 并发编程原则：持有锁时，尽量不要执行可能阻塞的外部操作。RPC 是这样，channel通常也应该这样考虑。
3. 关于状态：committed 是 Raft 层确认这条日志不会再丢，applied 是把这条已经 committed 的日志真正交给状态机执行。

keys:
1. AppendEntries可能重复或迟到(内容完全正确的)，但是需要保证follower复制日志具有幂等性。收到RPC reply时，发起这个RPC时的term(leader身份)可能过时
2. 日志复制不应该是每个命令开一个协程(不是每个命令一个生命周期)，而是每个follower一条复制流。Raft 的 replication unit 并不是 command。command 是 log entry 的来源；真正的复制行为是： “根据 follower 当前的 nextIndex，把 Leader 从那个位置之后缺失的日志发过去。”
   - 新command到来改变的是logs[]和lastLogIndex
   - AppendEntries允许一次RPC复制多条命令。
   - nextIndex[] / matchIndex[] 是 per-follower的，不是 per-command

遇到的问题：
1. 死锁等待。外层函数获取锁，内层函数也获取锁
2. 对所有服务器，若 commitIndex > lastApplied：递增 lastApplied，并将`log[lastApplied]`应用到状态机。--> 是每个服务器都维护自己的状态机
3. commitIndex的推进：一开始采用了基于channel通知的方式进行推进并针对性的计数，但是可能遇到a.成功事件数不代表服务器数 b.这个数比其他大，可以满足其他数的大多数条件，但是只看到这个数则会遗漏情况。修改后的 commit 推进逻辑：每次 matchIndex 变化后，从 lastLogIndex 往 commitIndex+1 找最大的合法 N

Start(command)
负责：
1. 判断当前是不是 Leader
2. 把 command 添加到 Leader 本地日志
3. 返回 index、term、isLeader

replication机制
负责：
1. 对每个 follower 看 nextIndex
2. 构造 AppendEntries
3. 成功则推进 matchIndex 和 nextIndex
4. 失败则减小nextIndex

