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

func CheckAllReplies(replies []DoJobReply, allGood chan bool){
	//fmt.Println("checking replies")
	for {
		allDone := true
		for reply := range replies {
			//fmt.Println("reply value:", replies[reply])
			if !replies[reply].OK { allDone = false; break }
		}
		//fmt.Println("checking if all good")
		if allDone { allGood <- true ; return }//fmt.Println("all good!!!") ;return }
	}
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

	}
	for n:=0;n < mr.nReduce; n++ {
		jobQueue <- DoJobArgs{mr.file, Reduce, n, mr.nMap}
	}

	// workerStrings := make([]string, 2)
	// info1 := new(WorkerInfo)
	// info2 := new(WorkerInfo)
	// info1.SetAddress( <- mr.registerChannel)
	// info2.SetAddress( <- mr.registerChannel)
	// mapReplies :=  make([]DoJobReply, mr.nMap)
	// reduceReplies := make([]DoJobReply, mr.nReduce)

	// reduceQueue := make(chan DoJobArgs, mr.nReduce)
	// doneChannel := make(chan bool)
	//fmt.Println("made queues")

	//fmt.Println("populated map queue")
	// close(mapQueue)
	// go func(){
	// 	ok := true
	// 	i :=0
	// 	for worker2jobArgs := range mapQueue {
	// 		//fmt.Println("Sending... 1", i)
	// 		ok =  call(info2.Address(), "Worker.DoJob",worker2jobArgs, &mapReplies[worker2jobArgs.JobNumber])
	// 		//fmt.Println("ok returned worker 1", ok, info2.Address())
	// 		i++
	// 	}
	// 	<- doneChannel
	// 	doneChannel <- true
	// }()
	// //fmt.Println("created worker 1 map calls")
	// go func(){
	// 	ok := true
	// 	i :=0
	// 	for worker1jobArgs := range mapQueue {
	// 		//fmt.Println("sending... 0", i)
	// 		ok = call(info1.Address(), "Worker.DoJob",worker1jobArgs, &mapReplies[worker1jobArgs.JobNumber])
	// 		//fmt.Println("ok returned worker 0", ok, info1.Address())
	// 		i++
	// 	}
	// 	//fmt.Println("write done channel 2")
	// 	doneChannel <- true
	// }()
	// //fmt.Println("created worker 0 map calls")
	// <- doneChannel
	// //fmt.Println("done channel 1")
	// <- doneChannel
	// //fmt.Println("done channel 2")
	// go CheckAllReplies(mapReplies, doneChannel)
	// <- doneChannel

	// for n:=0;n < mr.nReduce; n++ {
	// 	doJobArgs2 := DoJobArgs{mr.file, Reduce, n, mr.nMap}
	// 	reduceQueue <- doJobArgs2
	// }
	// close(reduceQueue)
	// //fmt.Println("populated reduce queue")
	// go func(){
	// 	for worker1jobArgs := range reduceQueue {
	// 		call(info1.Address(), "Worker.DoJob",worker1jobArgs, &reduceReplies[worker1jobArgs.JobNumber])
	// 	}
	// 	doneChannel <- true
	// 	//fmt.Println("reduce routine 1 done")
	// }()
	// go func(){
	// 	for worker2jobArgs := range reduceQueue {
	// 		call(info2.Address(), "Worker.DoJob",worker2jobArgs, &reduceReplies[worker2jobArgs.JobNumber])
	// 	}
	// 	doneChannel <- true
	// 	//fmt.Println(" reduce routine 2 done")
	// }()

	// <- doneChannel
	// <- doneChannel
	// go CheckAllReplies(reduceReplies, doneChannel)
	// <- doneChannel

	return mr.KillWorkers()
}
