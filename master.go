package mapreduce

import "container/list"
import "fmt"
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

func (mr *MapReduce) RunWorker(workerId string,
															 workerChannel chan string,
															 jobQueue chan DoJobArgs,
															 completeJobs chan int) {
	jobArgs := <- jobQueue
	reply := DoJobReply{}
	call(workerId, "Worker.DoJob",jobArgs, &reply)
	if reply.OK {
		workerChannel <- workerId
		count := <- completeJobs
		completeJobs <- count + 1
	} else {
		jobQueue <- jobArgs
	}

}
func (mr *MapReduce) RunMaster() *list.List {

	jobQueue := make(chan DoJobArgs)
	completeJobs := make(chan int)
	go func(){
		for workerId := range mr.registerChannel {
			go mr.RunWorker(workerId, mr.registerChannel, jobQueue, completeJobs)
		}
	}()

	for n:=0;n < mr.nMap; n++ {
		jobQueue <- DoJobArgs{mr.file, Map, n, mr.nReduce}
	}
	completeJobs <- 0
	fmt.Println("created map jobs:", mr.nMap)
	for completeJobsCount := range completeJobs {
		fmt.Println("completeJobs:", completeJobsCount)
		if completeJobsCount < mr.nMap {
				completeJobs <- completeJobsCount
			 } else { break }

	}
	for n:=0;n < mr.nReduce; n++ {
		jobQueue <- DoJobArgs{mr.file, Reduce, n, mr.nMap}
	}
	fmt.Println("created reduce jobs:", mr.nReduce)
	return mr.KillWorkers()
}
