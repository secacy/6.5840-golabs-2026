## Lab 2: Key/Value Server
为单台机器构建一个键值服务器。
该服务器能够确保每次 Put 操作在出现网络故障时也能最多执行一次，并且这些操作是可线性化的。
你将使用这个键值服务器来实现锁机制。

KV服务器：
客户端可以向服务器发送两种不同类型的请求：`Put(key, value, version)` 和 `Get(key)`
服务器在内存中维护一个映射，该映射为每个键记录一个(值, 版本)元组。
键和值都是字符串形式。版本号记录了该键被写入的次数。
只有当 Put 的版本号与服务器中该键的版本号匹配时， Put(key, value, version) 才会为映射中的特定键安装或替换该值。如果版本号匹配，服务器还会增加该键的版本号。如果版本号不匹配，服务器则应返回 rpc.ErrVersion 。
客户端可以通过调用 Put 来创建一个新的键，此时版本号为 0（那么服务器存储的版本号为 1）。
如果 Put 的版本号大于 0 且该键不存在，服务器应返回 rpc.ErrNoKey 。
Get(key) 会获取该键及其相关版本的值。如果服务器上不存在该键，那么服务器应该返回 rpc.ErrNoKey 。
为每个键维护一个版本号是非常有用的，这样可以在使用 Put 来实现锁机制时，并在网络不可靠且客户端需要重新传输数据时，确保 Put 的操作最多只能执行一次。

我们为你提供了骨架代码和测试代码，位于 src/kvsrv1 路径下。
kvsrv1/client.go 文件实现了一个 Clerk 类，客户端通过这个类来管理与服务器之间的 RPC 交互；Clerk 提供了 Put 和 Get 方法。
kvsrv1/server.go 文件包含了服务器的代码，其中包括了实现 RPC 请求服务器端处理的 Put 和 Get 处理程序。
你需要修改 client.go 和 server.go 文件。
RPC 请求、响应以及错误值都定义在 kvsrv1/rpc 包中，具体内容可以在 kvsrv1/rpc/rpc.go 文件中找到，不过你无需修改 rpc.go 文件。

启用并运行：
```bash
$ cd src
$ make kvsrv1
```

具有可靠网络的键值服务器
task: implement a solution that works when there are no dropped messages.
你需要将在 client.go 中的 Clerk 的 Put/Get 方法中添加用于发送 RPC 的代码，并在 server.go 中实现 Put 和 Get 的 RPC 处理程序。
```bash
$ cd src
$ make RUN="-run Reliable" kvsrv1
```

使用 K/V 客户端实现一个锁：
知识：在许多分布式应用程序中，运行在不同机器上的客户端会使用键值服务器来协调它们的操作。例如，ZooKeeper 和 Etcd 允许客户端通过分布式锁来协作。ZooKeeper 和 Etcd 通过带有条件的 put 操作来实现这种锁机制。
task: implement locks, using your key/value server to store whatever per-lock state your design needs.
可以创建多个独立的锁，每个锁都有唯一的名称，这些名称作为参数传递给 `MakeLock` 。
一个锁支持两种操作： Acquire 和 Release 。
一次只有单个客户端可以成功获取某个锁；其他客户端必须等待，直到第一个客户端通过 Release 释放该锁为止。

你需要修改 src/kvsrv1/lock/lock.go 代码。
同时，你的 Acquire 和 Release 模块应通过调用 lk.ck.Put() 和 lk.ck.Get() 函数来分别存储每个锁的状态信息。
Hint: 你需要为每个锁客户端提供一个唯一的标识符；可以使用 kvtest.RandValue(8) 来生成随机字符串。

by the way, 如果客户端在持有锁的时候发生崩溃，那么锁将永远无法被释放。在比这个模型更复杂的设计中，客户端会为锁创建一个租约。当租约到期时，锁服务器会代表客户端释放锁。在这个模型中，客户端不会崩溃，因此这个问题可以被忽略不计。

运行测试：
```bash
$ cd src
$ make RUN="-run Reliable" lock1
```


Key/value server with dropped(丢失的) messages
背景：网络可能会重新排序、延迟或丢失RPC请求或响应。客户端需要不断重新尝试每个 RPC 操作，直到收到服务器的响应为止。
一个棘手的情况是，当服务器对 Clerk 重新尝试的 RPC 发送 rpc.ErrVersion 作为响应时。在这种情况下，Clerk 无法知道服务器的确执行了它的 Put 还是没有执行
因此，如果 Clerk 收到关于重新传输的 Put RPC 的 rpc.ErrVersion ，那么 Clerk.Put 应该向应用程序返回 rpc.ErrMaybe 而不是 rpc.ErrVersion ，因为请求可能确实被执行了。接下来就由应用程序来处理这种情况了。
如果服务器对第一次的 Put RPC 响应了 rpc.ErrVersion ，那么 Clerk 应该向应用程序返回 rpc.ErrVersion ，因为显然该 RPC 并没有被服务器执行。
对于应用程序开发者来说，如果 Put 的操作能够做到一次性完成（即不会出现 rpc.ErrMaybe 错误），那就更方便了。不过，如果没有在服务器上为每个 Clerk 维护状态，那就很难保证这种效果。在本实验的最后一个环节中，你将使用 Clerk 来实现一个锁机制，从而了解如何以最多一次 Clerk.Put 的方式来进行编程。

现在，您应该修改您的代码，以便在遇到 RPC 请求失败的情况下继续运行。
当客户端从服务器的 ck.clnt.Call() 处收到 RPC 回复时，其返回值应为 true ；而当客户端没有收到任何回复时，返回值应为 false 。
更准确地说，当客户端等待回复消息超过超时时间仍未收到回复时，系统会返回 false 值。
您的 Clerk 应该持续发送 RPC 请求，直到收到回复为止。
您的解决方案不需要对服务器进行任何修改。

hint: Before the client retries, 应该等待一段时间；你可以使用 Go 语言的 time 包，并调用 time.Sleep(100 * time.Millisecond) 函数。