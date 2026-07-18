package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type mapTask struct {
	filename string // 文件名
	mapNum   int    // map编号
}

type Coordinator struct {
	// Your definitions here.
	nMap    int
	nReduce int

	// 有哪些还没有开始的map任务
	mapTask []mapTask
	// 有哪些还没有开始的reduce任务
	reduceTask []int
	// map任务是否完成
	mMap map[int]bool
	// reduce任务是否完成
	rMap map[int]bool
	// map任务完成的数量
	mDoneCnt int
	// reduce任务完成的数量
	rDoneCnt int
	// mapTask的锁
	mapTaskMu sync.Mutex
	// reduceTask的锁
	reduceTaskMu sync.Mutex
	// mMap和mDoneCnt的锁
	mDoneMu sync.RWMutex
	// rMap和rDoneCnt的锁
	rDoneMu sync.RWMutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// my RPC task handler.
func (c *Coordinator) TaskHandler(args *TaskArgs, reply *TaskReply) error {
	c.mapTaskMu.Lock()
	c.reduceTaskMu.Lock()
	c.mDoneMu.RLock()
	c.rDoneMu.RLock()
	defer c.mapTaskMu.Unlock()
	defer c.reduceTaskMu.Unlock()
	defer c.mDoneMu.RUnlock()
	defer c.rDoneMu.RUnlock()

	if len(c.mapTask) > 0 {
		// 取出mapTask任务
		var task mapTask
		for len(c.mapTask) > 0 {
			t := c.mapTask[0]
			c.mapTask = c.mapTask[1:]
			if !c.mMap[t.mapNum] {
				task = t
				break
			}
		}

		reply.Type = TaskMap
		reply.MapTask = MapTaskReply{
			Filename: task.filename,
			MapNum:   task.mapNum,
			NReduce:  c.nReduce,
		}

		//log.Printf("分配map任务，%v", reply.MapTask.MapNum)

		// 设置回调函数，10秒后执行
		timer := time.AfterFunc(10*time.Second, func() {
			c.mapTaskMu.Lock()
			defer c.mapTaskMu.Unlock()
			if !c.mMap[task.mapNum] {
				// 没有完成则移到任务列表中
				c.mapTask = append(c.mapTask, task)
				//log.Printf("重新将map任务 %v 加入队列", reply.MapTask.MapNum)
			}
		})
		_ = timer
	} else if c.mDoneCnt == c.nMap && len(c.reduceTask) > 0 {
		// 取出reduceTask任务
		var reduceNum int
		for len(c.reduceTask) > 0 {
			t := c.reduceTask[0]
			c.reduceTask = c.reduceTask[1:]
			if !c.rMap[t] {
				reduceNum = t
				break
			}
		}

		reply.Type = TaskReduce
		reply.ReduceTask = ReduceTaskReply{
			ReduceNum: reduceNum,
			NMap:      c.nMap,
		}

		//log.Printf("分配reduce任务，%v", reduceNum)

		// 设置回调函数，10秒后执行
		timer := time.AfterFunc(10*time.Second, func() {
			c.reduceTaskMu.Lock()
			defer c.reduceTaskMu.Unlock()
			if !c.rMap[reduceNum] {
				// 没有完成则移到任务列表中
				c.reduceTask = append(c.reduceTask, reduceNum)
				//log.Printf("重新将reduce任务 %d 加入队列", reduceNum)
			}
		})
		_ = timer
	} else if c.rDoneCnt == c.nReduce {
		reply.Type = TaskExit
	} else {
		reply.Type = TaskWait
	}
	return nil
}

// mark worker finish the task
func (c *Coordinator) TaskFinishHandler(args *TaskFinishArgs, reply *TaskFinishReply) error {
	c.mDoneMu.Lock()
	c.rDoneMu.Lock()
	defer c.mDoneMu.Unlock()
	defer c.rDoneMu.Unlock()

	if args.Type == TaskMap {
		if !c.mMap[args.MapTask.MapNum] {
			c.mMap[args.MapTask.MapNum] = true
			c.mDoneCnt++
		}
		//log.Printf("map任务 %d 完成了~", args.MapTask.MapNum)
	} else if args.Type == TaskReduce {
		if !c.rMap[args.ReduceTask.ReduceNum] {
			c.rMap[args.ReduceTask.ReduceNum] = true
			c.rDoneCnt++
		}
		//log.Printf("reduce任务 %d 完成了~", args.ReduceTask.ReduceNum)
	}
	return nil
}

// get whether map tasks all finish
func (c *Coordinator) MapDoneHandler(args *MapDoneArgs, reply *MapDoneReply) error {
	c.mDoneMu.RLock()
	defer c.mDoneMu.RUnlock()
	if c.mDoneCnt == c.nMap {
		reply.Done = true
		//log.Printf("map任务完成了")
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.rDoneMu.RLock()
	defer c.rDoneMu.RUnlock()
	if c.rDoneCnt == c.nReduce {
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.mapTask = []mapTask{}
	for i, filename := range files {
		c.mapTask = append(c.mapTask, mapTask{filename, i})
	}
	c.reduceTask = []int{}
	for i := range nReduce {
		c.reduceTask = append(c.reduceTask, i)
	}
	c.mMap = map[int]bool{}
	c.rMap = map[int]bool{}
	c.nReduce = nReduce
	c.nMap = len(files)

	c.server(sockname)
	return &c
}
