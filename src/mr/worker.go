package mr

import (
	"fmt"
	"io/ioutil"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "encoding/json"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
// mapf 和 reducef 定义了 map 和 reduce 处理方式，直接调用即可
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	// 循环地向 coordinator 请求任务，通信方式：RPC
	for {
		args := TaskArgs{}
		reply := TaskReply{}

		// 发送 rpc 请求，等待响应
		ok := call("Coordinator.TaskHandler", &args, &reply)
		if !ok {
			// 调用失败，也许是协调者进程退出了，说明任务执行完了，自己也要退出
			fmt.Printf("get task call failed!\n")
			break
		}

		// 调用成功，成功请求到了任务
		switch reply.Type {
		case TaskMap:
			mapReply := reply.MapTask
			handleMap(mapReply.Filename, mapReply.MapNum, mapReply.NReduce, mapf)
		case TaskReduce:
			reduceReply := reply.ReduceTask
			handleReduce(reduceReply.ReduceNum, reduceReply.NMap, reducef)
		case TaskWait:
			time.Sleep(1 * time.Second)
		case TaskExit:
			break
		}

		if reply.Type == TaskMap || reply.Type == TaskReduce {
			// 发送 rpc 请求，标识完成了任务
			finishArgs := TaskFinishArgs{reply.Type, reply.MapTask, reply.ReduceTask}
			finishReply := TaskFinishReply{}
			fok := call("Coordinator.TaskFinishHandler", &finishArgs, &finishReply)
			if !fok {
				fmt.Printf("finish task call failed!\n")
				break
			}
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// keys: 每个文件对应一个“分割”，并且是某个 Map 任务的输入
func handleMap(filename string, mapNum int, nReduce int, mapf func(string, string) []KeyValue) {
	// 读取文件内容
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	// 应用map函数，获取中间键值对
	kva := mapf(filename, string(content))

	// 创建 nReduce 个中间文件，供reduce任务使用。
	files := make([]*os.File, nReduce)
	encs := make([]*json.Encoder, nReduce)
	tmpNames := make([]string, nReduce)

	cleanup := func() {
		for i, file := range files {
			if file != nil {
				_ = file.Close()
			}

			if i < len(tmpNames) && tmpNames[i] != "" {
				_ = os.Remove(tmpNames[i])
			}
		}
	}

	for i := range nReduce {
		// 先使用临时文件，然后在文件完全写入后将其原子性地重命名
		tmp, err := os.CreateTemp(".", "mr-tmp-*")
		if err != nil {
			cleanup()
			log.Fatalf("cannot create tmp file, err = %v", err)
		}

		files[i] = tmp
		encs[i] = json.NewEncoder(tmp)
		tmpNames[i] = tmp.Name()
	}

	// 存储中间键值对
	for _, kv := range kva {
		idx := ihash(kv.Key) % nReduce
		if err := encs[idx].Encode(&kv); err != nil {
			cleanup()
			log.Fatalf("cannot encode, err = %v", err)
		}
	}

	// 在文件完全写入后将其原子性地重命名
	for reduceNum := range nReduce {
		f := files[reduceNum]

		if err := f.Sync(); err != nil {
			_ = file.Close()
			_ = os.Remove(tmpNames[reduceNum])
			log.Fatalf("cannot sync temporary file: %v", err)
		}
		if err := f.Close(); err != nil {
			_ = os.Remove(tmpNames[reduceNum])
			log.Fatalf("cannot close temporary file: %v", err)
		}

		iName := fmt.Sprintf("mr-%d-%d", mapNum, reduceNum)
		if err := os.Rename(tmpNames[reduceNum], iName); err != nil {
			_ = os.Remove(tmpNames[reduceNum])
			log.Fatalf("rename fail, err = %v", err)
		}
	}
}

func mapDone() bool {
	args := MapDoneArgs{}
	reply := MapDoneReply{}
	ok := call("Coordinator.MapDoneHandler", &args, &reply)
	if ok {
		return reply.Done
	}
	return false
}

func handleReduce(reduceNum int, nMap int, reducef func(string, []string) string) {
	// reduces can't start until the last map has finished
	for !mapDone() {
		time.Sleep(1 * time.Second)
	}

	// 读取所有中间键值对
	var kva []KeyValue
	for i := range nMap {
		filename := fmt.Sprintf("mr-%d-%d", i, reduceNum)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	// 对中间数据按照键进行排序，以便将所有键相同的键值对分为一组
	sort.Sort(ByKey(kva))

	// 创建中间文件
	tmp, err := os.CreateTemp(".", "tmp-*.txt")
	if err != nil {
		log.Fatalf("cannot create tmp file, err = %v", err)
	}
	defer os.Remove(tmp.Name())
	defer tmp.Close()

	// 遍历每一个遇到的中间键值对，将键和该键对应的一系列值传递给用户定义的reduce函数
	// 存储 reduce 任务的结果
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tmp, "%v %v\n", kva[i].Key, output)

		i = j
	}

	// 在文件完全写入后将其原子性地重命名
	oName := fmt.Sprintf("./mr-out-%d", reduceNum)
	if err := os.Rename(tmp.Name(), oName); err != nil {
		log.Fatalf("rename fail, err = %v", err)
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
