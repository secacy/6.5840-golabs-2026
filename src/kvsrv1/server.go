package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KValue struct {
	value   string
	version rpc.Tversion
}

type KVServer struct {
	mu    sync.Mutex
	kvMap map[string]*KValue
	// Your definitions here.
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	kv.kvMap = make(map[string]*KValue)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if v, ok := kv.kvMap[args.Key]; ok {
		reply.Value = v.value
		reply.Version = v.version
	} else {
		reply.Err = rpc.ErrNoKey
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if v, ok := kv.kvMap[args.Key]; !ok {
		if args.Version != 0 {
			// 如果 Put 的版本号大于 0 且该键不存在，服务器应返回 rpc.ErrNoKey
			reply.Err = rpc.ErrNoKey
		} else {
			kv.kvMap[args.Key] = &KValue{args.Value, 1}
		}
	} else {
		if v.version != args.Version {
			reply.Err = rpc.ErrVersion
		} else {
			v.value = args.Value
			v.version++
		}
	}
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
