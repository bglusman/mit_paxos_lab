package mapreduce

import "container/list"
import "fmt"
import "time"
// import "net"

type WorkerInfo struct {
	address string
	currentJobId int
	// You can add definitions here.
}

func (w *WorkerInfo) SetAddress(address string) {
    w.address = address
}

func (w *WorkerInfo) Address() string {
    return w.address
}
// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunWorker(workerId string, workerChannel chan string, jobQueue chan DoJobArgs, completeJobs *int) {
	jobArgs := <- jobQueue
	reply := DoJobReply{}
	call(workerId, "Worker.DoJob",jobArgs, &reply)
	if reply.OK {
		workerChannel <- workerId
		*completeJobs++
	} else {
		jobQueue <- jobArgs
	}

}
func (mr *MapReduce) RunMaster() *list.List {

	jobQueue := make(chan DoJobArgs)
	completeJobsCount := 0
	go func(){
		for workerId := range mr.registerChannel {
			go mr.RunWorker(workerId, mr.registerChannel, jobQueue, &completeJobsCount)
		}
	}()

	for n:=0;n < mr.nMap; n++ {
		jobQueue <- DoJobArgs{mr.file, Map, n, mr.nReduce}
	}

	for completeJobsCount < mr.nMap {
		fmt.Println("jobs remaining, jobCount:", completeJobsCount )
		time.Sleep(1 * time.Millisecond)
	}
	for n:=0;n < mr.nReduce; n++ {
		jobQueue <- DoJobArgs{mr.file, Reduce, n, mr.nMap}
	}

	return mr.KillWorkers()
}
