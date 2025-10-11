package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type Task struct {
	FileName  string
	TaskId    int // Map任务ID，生成中间文件要用
	ReduceNum int // Reduce 任务总数
	ReduceId  int // Reduce 任务ID，生成中间文件要用
}

type Coordinator struct {
	// Your definitions here.
	State          int32     // 处理状态 0：Map,1:Reduce,2:Finished
	MapChan        chan Task // Map任务channel（未使用，保留）
	ReduceChan     chan Task // Reduce任务channel（未使用，保留）
	NumMapTask     int       // Map 任务总数
	MapTaskTime    sync.Map  // Map 任务的时间戳，以及是否完成（未使用，保留）
	ReduceTaskTime sync.Map  // Reduce 任务的时间戳，以及是否完成（未使用，保留）
	files          []string

	mu            sync.Mutex
	nReduce       int
	epoch         int64 // 全局尝试ID生成器
	mapTasks      []taskMeta
	reduceTasks   []taskMeta
	completedMap  int
	completedReduce int
}

type taskStatus int

const (
	taskPending taskStatus = iota
	taskRunning
	taskDone
)

type taskMeta struct {
	id       int
	file     string    // map 任务的输入文件（reduce 为空）
	status   taskStatus
	attempt  int64
	deadline time.Time
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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
    log.Printf("Coordinator listening on %s", sockname)
    go http.Serve(l, nil)
}

// 分配下一个可用的 Map 任务
func (c *Coordinator) assignMapTaskLocked(reply *RequestTaskReply) bool {
	lease := 15 * time.Second
	now := time.Now()
	for i := range c.mapTasks {
		m := &c.mapTasks[i]
		switch m.status {
		case taskDone:
			continue
		case taskRunning:
			if now.After(m.deadline) {
				m.status = taskPending // 超时重试
			} else {
				continue
			}
		}
		// pending
		c.epoch++
		m.status = taskRunning
		m.attempt = c.epoch
		m.deadline = now.Add(lease)
		reply.TaskType = MapTask
		reply.TaskID = m.id
		reply.MapID = m.id
		reply.ReduceID = -1
		reply.MapFile = m.file
		reply.NumReduce = c.nReduce
		reply.NumMap = len(c.mapTasks)
		reply.AttemptID = m.attempt
		reply.LeaseMs = int(lease / time.Millisecond)
		reply.Epoch = c.epoch
		return true
	}
	return false
}

// 分配下一个可用的 Reduce 任务
func (c *Coordinator) assignReduceTaskLocked(reply *RequestTaskReply) bool {
	lease := 15 * time.Second
	now := time.Now()
	for i := range c.reduceTasks {
		r := &c.reduceTasks[i]
		switch r.status {
		case taskDone:
			continue
		case taskRunning:
			if now.After(r.deadline) {
				r.status = taskPending // 超时重试
			} else {
				continue
			}
		}
		// pending
		c.epoch++
		r.status = taskRunning
		r.attempt = c.epoch
		r.deadline = now.Add(lease)
		reply.TaskType = ReduceTask
		reply.TaskID = r.id
		reply.MapID = -1
		reply.ReduceID = r.id
		reply.MapFile = ""
		reply.NumReduce = c.nReduce
		reply.NumMap = len(c.mapTasks)
		reply.AttemptID = r.attempt
		reply.LeaseMs = int(lease / time.Millisecond)
		reply.Epoch = c.epoch
		return true
	}
	return false
}

// RequestTask: Worker 请求任务
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if atomic.LoadInt32(&c.State) == 2 {
		reply.TaskType = ExitTask
		return nil
	}

	if atomic.LoadInt32(&c.State) == 0 {
		if c.assignMapTaskLocked(reply) {
			return nil
		}
		// 如果没有可分配 map 任务，检查是否全部完成，切换到 reduce
		if c.completedMap == len(c.mapTasks) {
			atomic.StoreInt32(&c.State, 1)
		} else {
			reply.TaskType = WaitTask
			return nil
		}
	}

	// Reduce 阶段
	if atomic.LoadInt32(&c.State) == 1 {
		if c.assignReduceTaskLocked(reply) {
			return nil
		}
		if c.completedReduce == len(c.reduceTasks) {
			atomic.StoreInt32(&c.State, 2)
			reply.TaskType = ExitTask
			return nil
		}
		reply.TaskType = WaitTask
		return nil
	}

	reply.TaskType = WaitTask
	return nil
}

// ReportTask: Worker 上报任务完成/失败
func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	success := args.Success || (args.AttemptID == 0 && args.Error == "")

	switch args.TaskType {
	case MapTask:
		if args.TaskID >= 0 && args.TaskID < len(c.mapTasks) {
			m := &c.mapTasks[args.TaskID]
			// 仅当未完成时处理
			if m.status != taskDone {
				if success {
					m.status = taskDone
					c.completedMap++
				} else {
					m.status = taskPending // 失败则重试
				}
			}
		}
	case ReduceTask:
		if args.TaskID >= 0 && args.TaskID < len(c.reduceTasks) {
			r := &c.reduceTasks[args.TaskID]
			if r.status != taskDone {
				if success {
					r.status = taskDone
					c.completedReduce++
				} else {
					r.status = taskPending
				}
			}
		}
	}

	reply.Ack = true
	return nil
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	if atomic.LoadInt32(&c.State) == 2 {
		return true
	}
	// 在 reduce 阶段监测是否完成
	if atomic.LoadInt32(&c.State) == 1 && c.completedReduce == len(c.reduceTasks) {
		atomic.StoreInt32(&c.State, 2)
		return true
	}
	return false
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.files = append(c.files, files...)
	c.NumMapTask = len(files)
	c.nReduce = nReduce
	atomic.StoreInt32(&c.State, 0)

	// 初始化任务元数据
	c.mapTasks = make([]taskMeta, len(files))
	for i, f := range files {
		c.mapTasks[i] = taskMeta{id: i, file: f, status: taskPending}
	}
	c.reduceTasks = make([]taskMeta, nReduce)
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = taskMeta{id: i, file: "", status: taskPending}
	}

	c.server()
	return &c
}
