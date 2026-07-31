package lock

import (
	"log"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	lockName string
	clientId string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.lockName = lockname
	lk.ck.Put(lk.lockName, "", 0)
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	// 锁有名称 lk.lockName
	// 一次只有一个客户端获取到锁；其他客户端必须等待直到锁释放
	lk.clientId = kvtest.RandValue(8) // 这是客户端自己的clientId，记录在自己的锁变量中
	// 进行put操作
	err := lk.ck.Put(lk.lockName, lk.clientId, 0)
	for err == rpc.ErrVersion || err == rpc.ErrMaybe {
		// 如果是maybe，可能没有成功获取到锁，需要验证锁的clientId是否是自己
		// 锁被持有
		// 尝试get锁
		var rpcVersion rpc.Tversion
		var cId string
		for {
			cId, rpcVersion, _ = lk.ck.Get(lk.lockName)
			if cId == "" || cId == lk.clientId {
				break
			}
		}
		// 尝试put锁
		err = lk.ck.Put(lk.lockName, lk.clientId, rpcVersion)
	}
	// 成功获取到了锁
}

func (lk *Lock) Release() {
	// Your code here
	// 如何得知锁被被释放了 -> 把clientId设置为 ""
	kvClientId, rpcVersion, err := lk.ck.Get(lk.lockName)
	if err == rpc.ErrNoKey {
		log.Printf("lock %s not found", lk.lockName)
		return
	}
	if kvClientId != lk.clientId {
		log.Printf("lock %s is not hold by me, %s", lk.lockName, lk.clientId)
		return
	}
	putErr := lk.ck.Put(lk.lockName, "", rpcVersion)
	if putErr == rpc.ErrVersion {
		log.Printf("lock %s fail to release", lk.lockName)
	}
	// 如果没有被释放，需要不断重试
	for putErr == rpc.ErrMaybe {
		// log.Printf("lock %s fail to release", lk.lockName)
		putErr = lk.ck.Put(lk.lockName, "", rpcVersion)
	}
}
