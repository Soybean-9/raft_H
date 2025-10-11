package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "time"
import "encoding/json"
import "sort"
import "io"
import "6.5840/labrpc"
import "math/rand"
import "bufio"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
    reducef func(string, []string) string) {
    if thisWorkerId == "" {
        thisWorkerId = fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
    }
    log.Printf("Worker %s starting", thisWorkerId)
    for {
        req := RequestTaskArgs{
            WorkerId:    thisWorkerId,
            LastTaskID:   lastTaskID,
            LastTaskType: lastTaskType,
            LastAttempt:  lastAttemptID,
        }
        reply, ok := requestTask(req)
        if !ok {
            // Coordinator not ready; wait and retry
            log.Printf("Worker %s: requestTask no reply, retrying", thisWorkerId)
            time.Sleep(500 * time.Millisecond)
            continue
        }

        log.Printf("Worker %s: got task type=%v id=%d attempt=%d", thisWorkerId, reply.TaskType, reply.TaskID, reply.AttemptID)
        switch reply.TaskType {
        case MapTask:
            outputs, durMs, err := runMapTask(mapf, reply)
            if err != nil {
                log.Printf("map task %d failed: %v", reply.TaskID, err)
                // On failure, report failure and back off; coordinator may re-assign
                _ = reportTaskDone(MapTask, reply.TaskID, reply.AttemptID, false, nil, durMs)
                // 随机退避，避免thundering herd在crash场景下反复争抢相同任务
                time.Sleep(time.Duration(200+rand.Intn(600)) * time.Millisecond)
                continue
            }
            _ = reportTaskDone(MapTask, reply.TaskID, reply.AttemptID, true, outputs, durMs)
            log.Printf("map task %d done, outputs=%v", reply.TaskID, outputs)
            lastTaskID = reply.TaskID
            lastTaskType = MapTask
            lastAttemptID = reply.AttemptID
        case ReduceTask:
            outputs, durMs, err := runReduceTask(reducef, reply)
            if err != nil {
                log.Printf("reduce task %d failed: %v", reply.TaskID, err)
                _ = reportTaskDone(ReduceTask, reply.TaskID, reply.AttemptID, false, nil, durMs)
                time.Sleep(time.Duration(200+rand.Intn(600)) * time.Millisecond)
                continue
            }
            _ = reportTaskDone(ReduceTask, reply.TaskID, reply.AttemptID, true, outputs, durMs)
            log.Printf("reduce task %d done, outputs=%v", reply.TaskID, outputs)
            lastTaskID = reply.TaskID
            lastTaskType = ReduceTask
            lastAttemptID = reply.AttemptID
        case WaitTask:
            // No task available right now; wait a bit
            log.Printf("Worker %s: wait", thisWorkerId)
            time.Sleep(300 * time.Millisecond)
        case ExitTask:
            log.Printf("Worker %s: exit", thisWorkerId)
            return
        default:
            // Unknown type; avoid hot loop
            log.Printf("Worker %s: unknown task type %v", thisWorkerId, reply.TaskType)
            time.Sleep(300 * time.Millisecond)
        }
    }
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

// requestTask asks the coordinator for the next task to execute.
func requestTask(args RequestTaskArgs) (RequestTaskReply, bool) {
    var reply RequestTaskReply
    ok := call("Coordinator.RequestTask", &args, &reply)
    return reply, ok
}

// reportTaskDone notifies the coordinator that a task has been completed.
func reportTaskDone(taskType TaskType, taskID int, attemptID int64, success bool, outputs []string, durationMs int64) bool {
    args := ReportTaskArgs{WorkerId: thisWorkerId, TaskID: taskID, TaskType: taskType, AttemptID: attemptID, Success: success, DurationMs: durationMs, OutputFiles: outputs}
    var reply ReportTaskReply
    return call("Coordinator.ReportTask", &args, &reply)
}

// runMapTask executes a Map task and writes partitioned JSON outputs.
func runMapTask(mapf func(string, string) []KeyValue, reply RequestTaskReply) ([]string, int64, error) {
    started := time.Now()
    content, err := os.ReadFile(reply.MapFile)
    if err != nil {
        return nil, 0, err
    }

    kva := mapf(reply.MapFile, string(content))

    encoders := make([]*json.Encoder, reply.NumReduce)
    tmpFiles := make([]*os.File, reply.NumReduce)
    bufWriters := make([]*bufio.Writer, reply.NumReduce)
    tmpNames := make([]string, reply.NumReduce)
    finalNames := make([]string, reply.NumReduce)

    for i := 0; i < reply.NumReduce; i++ {
        f, err := os.CreateTemp(".", "mr-tmp-")
        if err != nil {
            return nil, 0, err
        }
        tmpFiles[i] = f
        tmpNames[i] = f.Name()
        bw := bufio.NewWriterSize(f, 1<<20)
        bufWriters[i] = bw
        encoders[i] = json.NewEncoder(bw)
        finalNames[i] = fmt.Sprintf("mr-%d-%d", reply.TaskID, i)
    }

    for _, kv := range kva {
        r := ihash(kv.Key) % reply.NumReduce
        if err := encoders[r].Encode(&kv); err != nil {
            return nil, 0, err
        }
    }

    for i := 0; i < reply.NumReduce; i++ {
        if bufWriters[i] != nil {
            if err := bufWriters[i].Flush(); err != nil {
                return nil, 0, err
            }
        }
        if err := tmpFiles[i].Close(); err != nil {
            return nil, 0, err
        }
        if err := os.Rename(tmpNames[i], finalNames[i]); err != nil {
            return nil, 0, err
        }
    }

    return finalNames, int64(time.Since(started) / time.Millisecond), nil
}

// runReduceTask executes a Reduce task by aggregating intermediate files.
func runReduceTask(reducef func(string, []string) string, reply RequestTaskReply) ([]string, int64, error) {
    started := time.Now()
    reduceID := reply.TaskID
    keyToValues := make(map[string][]string)

    for m := 0; m < reply.NumMap; m++ {
        filename := fmt.Sprintf("mr-%d-%d", m, reduceID)
        f, err := os.Open(filename)
        if err != nil {
            if os.IsNotExist(err) {
                continue
            }
            return nil, 0, err
        }
        dec := json.NewDecoder(f)
        for {
            var kv KeyValue
            if err := dec.Decode(&kv); err != nil {
                if err == io.EOF {
                    break
                }
                f.Close()
                return nil, 0, err
            }
            keyToValues[kv.Key] = append(keyToValues[kv.Key], kv.Value)
        }
        _ = f.Close()
    }

    keys := make([]string, 0, len(keyToValues))
    for k := range keyToValues {
        keys = append(keys, k)
    }
    sort.Strings(keys)

    out, err := os.CreateTemp(".", "mr-out-tmp-")
    if err != nil {
        return nil, 0, err
    }
    bw := bufio.NewWriterSize(out, 1<<20)
    for _, k := range keys {
        res := reducef(k, keyToValues[k])
        if _, err := fmt.Fprintf(bw, "%v %v\n", k, res); err != nil {
            bw.Flush()
            out.Close()
            return nil, 0, err
        }
    }
    if err := bw.Flush(); err != nil {
        out.Close()
        return nil, 0, err
    }
    if err := out.Close(); err != nil {
        return nil, 0, err
    }
    final := fmt.Sprintf("mr-out-%d", reduceID)
    if err := os.Rename(out.Name(), final); err != nil {
        return nil, 0, err
    }
    return []string{final}, int64(time.Since(started) / time.Millisecond), nil
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
    // Prefer labrpc client if provided (test/simulated network), fallback to net/rpc via unix socket
    if rpcClient != nil {
        ok := rpcClient.Call(rpcname, args, reply)
        if !ok {
            fmt.Println("labrpc call failed:", rpcname)
        }
        return ok
    }
    // c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
    sockname := coordinatorSock()
    c, err := rpc.DialHTTP("unix", sockname)
    if err != nil {
        // 避免直接退出，返回 false 让上层重试
        log.Printf("dialing failed: %v", err)
        return false
    }
    defer c.Close()

    err = c.Call(rpcname, args, reply)
    if err == nil {
        return true
    }

    fmt.Println(err)
    return false
}

// SetLabRPCClient allows tests to inject a labrpc client end.
func SetLabRPCClient(e *labrpc.ClientEnd) {
    rpcClient = e
}

var rpcClient *labrpc.ClientEnd
var thisWorkerId string
var lastTaskID int = -1
var lastAttemptID int64 = 0
var lastTaskType TaskType = WaitTask
