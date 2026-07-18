## Lab 1: MapReduce

Task: 
实现一个分布式 MapReduce 系统，该系统由两个程序组成：协调器和处理器。 将只有一个协调器进程，而一个或多个处理器进程会并行运行。
处理器通过 RPC 方式与协调器进行通信。每个处理器进程都会循环地向协调器请求任务，从一个或多个文件中读取任务的输入数据，执行任务，将任务的输出写入一个或多个文件中，然后再次向协调器请求新的任务。
每个文件对应一个“split”，并且是某个 Map 任务的输入。
mr-out-X 的文件应包含输出结果，每个 reduce 任务对应一个这样的文件。
如果某个处理器在合理的时间内（在这个实验中为十秒）未能完成任务，协调器会将其任务分配给另一个处理器来完成。

实现位置：
协调器和工作者的“main”程序分别位于 main/mrcoordinator.go 和 main/mrworker.go 文件中；请不要修改这些文件。
您应该将您的实现代码放在 mr/coordinator.go 、 mr/worker.go 和 mr/rpc.go 文件中。

运行测试：
```bash
cd src
make mr
```

目标：
1. 当以 pg-xxx.txt 文件作为输入时， wc 和 indexer 的 MapReduce 应用程序能够产生正确的输出结果
2. 你的实现能够并行运行 Map 和 Reduce 任务，并且能够从因某些 worker 在任务执行过程中崩溃而中断的情况下继续运行。

几条规则：
1. The map phase:  should divide the intermediate keys into buckets for nReduce reduce tasks。其中， nReduce 表示 reduce 任务的个数——这个数值是由 main/mrcoordinator.go 传递给 MakeCoordinator() 的。

思路：
协调者有两个阶段：
1. map phase: 
- nReduce: reduce任务的个数。这是app参数。
- 每个mapper还需要创建 nReduce 个中间文件。对于中间文件的合理命名规则是 mr-X-Y ，其中 X 表示 Map 任务编号，Y 表示 Reduce 任务编号。
- coordinator需要分配m个任务(输入文件的数量)和
2. reduce phase:
- 
3. 如果某个处理器在合理的时间内（在这个实验中为十秒）未能完成任务，协调器会将其任务分配给另一个处理器来完成。
- tip: 协调器无法可靠地区分那些崩溃的工人、虽然还活着但因某种原因而停滞工作的工人，以及那些虽然正在执行任务但速度太慢而无法发挥作用的工人。最好的做法就是让协调器等待一段时间，然后放弃任务，将任务重新分配给其他工人。在这个实验中，让协调器等待十秒钟；超过这个时间后，协调器应认为该工人已经死亡（当然，实际情况可能并非如此）。
- 如何判断超时并进行处理？
  回调函数10s后执行，判断之前的任务是否执行成功。

4. Done(): 当 MapReduce 作业完全完成后，该方法应返回 true 值。

worker:
1. map task:
- 在map阶段，需要将中间键分成多个组，以便分配给 nReduce 个reduce任务。其中， nReduce 表示reduce任务的个数。
- worker应该将中间 Map 输出的结果保存到当前目录中的文件中。(每个mapper还需要创建 nReduce 个中间文件，供reduce任务使用。)
- 可以使用 ihash(key) 函数来为给定的键选择 reduce 任务。

2. reduce task:
- 有时候，worker需要等待。
- worker可以将包含中间Map输出的结果的文件作为输入数据来执行 Reduce 任务。
- 将第 X 个 reduce 任务的输出结果保存到文件 mr-out-X 中。每个 mr-out-X 文件: 对于每个reduce函数的输出，应该包含一行。该行应采用 "%v %v" 格式编写，需要包含键和值。

3. 任务完全完成后：
- 当任务完全完成后，工作线程应该退出。实现这一点的简单方法就是利用 call() 的返回值：如果工作线程无法联系协调器，那么可以认为协调器已经退出，因此工作线程也可以终止。根据您的设计，或许还需要设置一个“请退出”的伪任务，供协调器发送给工作线程。

提示：
1. 开始的方法之一是修改 mr/worker.go 中的 Worker() ，以向协调器发送一个 RPC 请求，请求一个任务。然后修改协调器，使其响应时返回一个尚未开始执行的map任务的文件名。接着，修改工作者程序，使其读取该文件并调用app中的map函数，就像在 mrsequential.go 中所做的那样。

实现时遇到了以下问题：
1. 并发下的竞争
```
Read at 0x00c000172290 by goroutine 14:
6.5840/mr.(*Coordinator).TaskHandler()
coordinator.go:57

Previous write at 0x00c000172290 by goroutine 16:
6.5840/mr.(*Coordinator).TaskHandler()
coordinator.go:61
```
**只要多个 goroutine 会访问同一份数据，并且至少有一个 goroutine 会修改数据，就必须进行同步。而且读操作也需要加锁。**
- sync.RWMutex 允许多个读操作并发执行
- 定时回调运行在独立 goroutine 中，需要自己重新加锁。

2. 资源创建与释放
不确定是否会导致不能通过测试，但是最终在排查问题时把资源释放都补全了一些

3. keys: **在map任务还没有完成的情况下，不应该过早分配reduce任务，因为worker进程是有限的，但是如果持有map任务的进程都crash了，则其他worker将被reduce任务占据但是没有进展。**

