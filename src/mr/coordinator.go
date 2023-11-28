package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	lock sync.RWMutex
	//phase of all tasks
	status TaskTypeOpt
	//the number of workers
	mapCnt    int
	reduceCnt int
	//all tasks 正在执行+可以分配
	tasks map[string]*TaskInfo
	//all ongoing tasks 可以分配
	availableTasks chan *TaskInfo
}

// generateTaskId Generate TaskId for the given task
// The id follows the format: "taskType-taskIndex"
func generateTaskId(taskType TaskTypeOpt, index int) string {
	return fmt.Sprintf("%s-%d", taskType, index)
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
//	func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
//		reply.Y = args.X + 1
//		return nil
//	}
func (c *Coordinator) AckAndQueryNewTask(req *AckAndQueryNewTaskRequest, resp *AckAndQueryNewTaskResponse) error {
	//step1:mark previous task finished if necessary
	if req.TaskType != "" {
		err := c.handlePreviousTask(req)
		if err != nil {
			errorf("handlePreviousTask err: %v", err)
			return err
		}
	}
	//step2:get next task
	//如果Channel未关闭，且没有下一个任务，则会在此阻塞等待下一个任务
	//如果Channel已经关闭，则ok会是false
	task, ok := <-c.availableTasks
	if !ok {
		c.lock.RLock()
		defer c.lock.RUnlock()
		retTask := &TaskInfo{}
		if c.status == TaskTypeFinished {
			retTask.Type = TaskTypeFinished
		}
		resp.Task = retTask
		return nil
	}
	//step3:assign the task to the worker
	c.lock.Lock()
	defer c.lock.Unlock()
	infof("Assign task %v to worker %s", task, req.WorkerId)
	task.WorkerId = req.WorkerId
	task.Deadline = time.Now().Add(10 * time.Second)
	c.tasks[generateTaskId(task.Type, task.Index)] = task
	//step4:handle response
	resp.Task = &TaskInfo{
		Id:       task.Id,
		Type:     task.Type,
		Index:    task.Index,
		File:     task.File,
		WorkerId: task.WorkerId,
		Deadline: task.Deadline,
	}
	resp.MapWorkerCnt = c.mapCnt
	resp.ReduceWorkerCnt = c.reduceCnt
	return nil
}

// 不同的任务类型,给出不同的处理函数
var (
	taskFinishHandlerMap = map[TaskTypeOpt]func(workerId string, taskIdx, reduceCnt int) error{
		TaskTypeMap:    handleFinishedMapTask,
		TaskTypeReduce: handleFinishedReduceTask,
	}
)

// map-type task finished handler
func handleFinishedMapTask(workerId string, taskIdx, reduceCnt int) error {
	//mark the task's temporary file to final file(Rename)
	for reduceIdx := 0; reduceIdx < reduceCnt; reduceIdx++ {
		tmpMapFileName := tmpMapOutFile(workerId, taskIdx, reduceIdx)
		finalMapOutFileName := finalMapOutFile(taskIdx, reduceIdx)
		err := os.Rename(tmpMapFileName, finalMapOutFileName)
		if err != nil {
			errorf("Failed to mark map output file `%s` as final: %e", tmpMapFileName, err)
			return err
		}
	}
	infof("handleFinishedMapTask success: workerId: %s, taskIdx: %d", workerId, taskIdx)
	return nil
}

// reduce-type task finished handler
func handleFinishedReduceTask(workerId string, taskIdx, _ int) error {
	tmpReduceFileName := tmpReduceOutFile(workerId, taskIdx)
	finalReduceOutFileName := finalReduceOutFile(taskIdx)
	err := os.Rename(tmpReduceFileName, finalReduceOutFileName)
	if err != nil {
		errorf("Failed to mark reduce output file `%s` as final: %v", tmpReduceFileName, err)
		return err
	}
	infof("handleFinishedReduceTask success: workerId: %s, taskIdx: %d, finalReduceOutFileName: %s",
		workerId, taskIdx, finalReduceOutFileName)
	return nil
}

// The temporary file that map-type task yield
func tmpMapOutFile(workerId string, taskIdx, reduceIdx int) string {
    return fmt.Sprintf("mr-map-%s-%d-%d", workerId, taskIdx, reduceIdx)
}

// The final file that map-type task yield(for reduce)
func finalMapOutFile(taskIdx, reduceIdx int) string {
    return fmt.Sprintf("mr-map-%d-%d", taskIdx, reduceIdx)
}

// The temporary file that reduce-type task yield
func tmpReduceOutFile(workerId string, reduceIdx int) string {
    return fmt.Sprintf("mr-reduce-%s-%d", workerId, reduceIdx)
}

// The final file that reduce-type task yield(the MapReduce task yield)
func finalReduceOutFile(taskIndex int) string {
    return fmt.Sprintf("mr-out-%d", taskIndex)
}


func (c *Coordinator) handlePreviousTask(req *AckAndQueryNewTaskRequest) error {
	previousTaskId := generateTaskId(req.TaskType, req.PreviousTaskIndex)

	c.lock.Lock()
	defer c.lock.Unlock()
	taskInfo, exists := c.tasks[previousTaskId]
	if exists {
		if taskInfo.WorkerId == req.WorkerId { //this task belongs to the worker
			infof("Mark task [%v] finished on worker %s", taskInfo, req.WorkerId)
			//step1:handle the previous finished task
			handler, handlerExists := taskFinishHandlerMap[taskInfo.Type]
			if !handlerExists || handler == nil {
				return fmt.Errorf("handler not found for task:%v", taskInfo)
			}
			err := handler(req.WorkerId, req.PreviousTaskIndex, c.reduceCnt)
			if err != nil {
				errorf("Failed to handle previous task: %v", err)
				return err
			}
			delete(c.tasks, previousTaskId)
			//step2:transit job phase if necessary
			if len(c.tasks) <= 0 {
				c.transit()
			}
			return nil
		} else { //this task is no longer belongs to this worker，提示但不报错
			infof("Task %v is no longer belongs to this worker", taskInfo)
			return nil
		}
	} else { //previous task not found in task map
		warnf("[Warn] Previous task: %v not found in map", taskInfo)
		return nil
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//transit the job phase (from map to reduce)
func (c *Coordinator) transit() {
	if c.status==TaskTypeMap{
		//All map-type tasks finished,change to reduce phase
		infof("All map-type tasks finished. Transit to REDUCE stage!")
		c.status=TaskTypeReduce
		//yield reduce tasks
		for reduceIdx:=0;reduceIdx<c.reduceCnt;reduceIdx++{
			task:=&TaskInfo{
				Type:TaskTypeReduce,
				Index:reduceIdx,
			}
			c.tasks[generateTaskId(task.Type,task.Index)]=task
			c.availableTasks<-task
		}
	}else if c.status==TaskTypeReduce{
        infof("All reduce-type tasks finished. Prepare to exit!")
		close(c.availableTasks)
		c.status=TaskTypeFinished
	}
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
// 初始化Coordinator
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	//step1:create master
	m := &Coordinator{
		status:         TaskTypeMap,
		mapCnt:         len(files),
		reduceCnt:      nReduce,
		tasks:          make(map[string]*TaskInfo),
		availableTasks: make(chan *TaskInfo, max(len(files), nReduce)),
	}
	//step2:store data
	for idx, file := range files {
		task := &TaskInfo{
			Id:    generateTaskId(TaskTypeMap, idx),
			Type:  TaskTypeMap,
			Index: idx,
			File:  file,
		}
		m.tasks[task.Id] = task
		m.availableTasks <- task
	}
	//step3:start coordinator server
	m.server()
	infof("master server started: %v", m)

	//step4:start workers heartbeats checker
	go func() {
		for {
			time.Sleep(500 * time.Millisecond)
			m.checkWorkers()
		}
	}()
	return m
}
