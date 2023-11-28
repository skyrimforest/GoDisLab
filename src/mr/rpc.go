package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// MapReduce task type
type TaskTypeOpt string

const (
	TaskTypeMap      TaskTypeOpt = "Map"
	TaskTypeReduce   TaskTypeOpt = "Reduce"
	TaskTypeFinished TaskTypeOpt = "Finished"
)

// TaskInfo MapReduce task
type TaskInfo struct {
	Id       string //任务ID
	Type     TaskTypeOpt
	Index    int    //任务序号,表示第几个任务
	File     string //需要处理的文件
	WorkerId string //需要处理该任务的WorkerId
	Deadline time.Time
}

// AckAndQueryNewTaskRequest rpc request for Workers to query a task after finished previously task.
type AckAndQueryNewTaskRequest struct {
	//The finished previous task index(if it has finished task)
	PreviousTaskIndex int         //上一个执行完任务的索引，用于Commit上一个任务
	TaskType          TaskTypeOpt //上一个执行完任务的类型，用于Commit上一个任务
	WorkerId          string      //上一个执行完任务的WorkerId
}

// AckAndQueryNewTaskResponse rpc response for Workers to query a task
type AckAndQueryNewTaskResponse struct {
	//The task id(filename) for Map or Reduce to yield results(if there has)
	Task            *TaskInfo //*TaskInfo类型,新申请的任务，若不存在可调度任务，就返回nil
	MapWorkerCnt    int       //分配执行Map的Worker数，用于生成临时文件
	ReduceWorkerCnt int       //同上
}

// type ExampleArgs struct {
// 	X int
// }

// type ExampleReply struct {
// 	Y int
// }

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
