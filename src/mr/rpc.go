package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type TaskType int

const (
	//TaskInvalid TaskType = iota
	TaskMap TaskType = iota
	TaskReduce
	TaskWait
	TaskExit
)

type TaskArgs struct {
}

type TaskReply struct {
	Type       TaskType
	MapTask    MapTaskReply
	ReduceTask ReduceTaskReply
}

type MapTaskReply struct {
	MapNum   int    // map任务编号
	Filename string // 尚未开始执行的map任务的文件名
	NReduce  int    // reduce任务数量
}

type ReduceTaskReply struct {
	ReduceNum int // reduce任务编号
	NMap      int // map任务数量
}

type MapDoneArgs struct {
}

type MapDoneReply struct {
	Done bool // 所有map任务是否执行完成
}

type TaskFinishArgs struct {
	Type       TaskType
	MapTask    MapTaskReply
	ReduceTask ReduceTaskReply
}

type TaskFinishReply struct {
}
