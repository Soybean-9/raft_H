package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
    "os"
    "strconv"
    "6.5840/labgob"
)

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

// 任务类型
type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask // 当没有任务时，让 worker 等待
	ExitTask // 所有任务完成，让 worker 退出
)

// Worker 请求任务的参数
type RequestTaskArgs struct {
    // 可以为空，因为 Coordinator 知道要分配什么
    // 为容错增加可选信息
    WorkerId   string
    // 上一次完成的任务信息（可选用于“粘性”调度或去重）
    LastTaskID   int
    LastTaskType TaskType
    LastAttempt  int64 // TaskAttemptId
}

// Coordinator 回复任务请求的返回值
type RequestTaskReply struct {
	TaskType  TaskType // 分配的任务类型 (Map, Reduce, Wait, or Exit)
	TaskID    int      // 任务的唯一 ID
	MapFile   string   // Map 任务需要读取的文件名
	NumReduce int      // Reduce 任务的总数 (所有 Worker 都需要知道)
	NumMap    int      // Map 任务的总数
    // 容错/租约
    AttemptID int64 // TaskAttemptId，本次分配的尝试ID
    LeaseMs   int   // 建议的任务租约时长，超时后可被重试
    Epoch     int64 // 协调者任务分配纪元，用于去重
    // 明确标识（向后兼容：若未设置则与 TaskID 相同或为 -1）
    MapID    int
    ReduceID int
}

// Worker 报告任务完成的参数
type ReportTaskArgs struct {
    WorkerId  string
    TaskID    int
    TaskType  TaskType
    AttemptID int64 // 与分配时的 AttemptID 对应
    Success   bool
    Error     string
    DurationMs int64
    OutputFiles []string
}

// Coordinator 回复任务报告的返回值
type ReportTaskReply struct {
    Ack bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

// 使用 labgob 注册 MR 的 RPC 类型，确保通过 labrpc 传输时能正确编码/解码。
func init() {
    labgob.Register(TaskType(0))
    labgob.Register(ExampleArgs{})
    labgob.Register(ExampleReply{})
    labgob.Register(RequestTaskArgs{})
    labgob.Register(RequestTaskReply{})
    labgob.Register(ReportTaskArgs{})
    labgob.Register(ReportTaskReply{})
}
