package mapreduce

import "container/list"
import "fmt"

// import "net"

type WorkerInfo struct {
	address string
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
	fmt.Println("checking replies")
	for {
		allDone := true
		for reply := range replies {
			fmt.Println("reply value:", replies[reply])
			if !replies[reply].OK { allDone = false; break }
		}
		fmt.Println("checking if all good")
		if allDone { allGood <- true ; fmt.Println("all good!!!") ;return }
	}
}

func (mr *MapReduce) RunMaster() *list.List {

			// workerStrings := make([]string, 2)
			info1 := new(WorkerInfo)
			info2 := new(WorkerInfo)
			info1.SetAddress( <- mr.registerChannel)
			info2.SetAddress( <- mr.registerChannel)
			mapReplies :=  make([]DoJobReply, mr.nMap)
			reduceReplies := make([]DoJobReply, mr.nReduce)
			mapQueue := make(chan DoJobArgs, mr.nMap)
			reduceQueue := make(chan DoJobArgs, mr.nReduce)
			doneChannel := make(chan bool)
			fmt.Println("made queues")
			for n:=0;n < mr.nMap; n++ {
				fmt.Println("making queues")
				mapQueue <- DoJobArgs{mr.file, Map, n, mr.nReduce}
			}
			fmt.Println("populated map queue")
			close(mapQueue)
			go func(){
				for worker2jobArgs := range mapQueue {
					call(info2.Address(), "Worker.DoJob",worker2jobArgs, &mapReplies[worker2jobArgs.JobNumber])
				}
				fmt.Println("write done channel 1")
				doneChannel <- true
			}()
			fmt.Println("created worker 1 map calls")
			go func(){
				for worker1jobArgs := range mapQueue {
					call(info1.Address(), "Worker.DoJob",worker1jobArgs, &mapReplies[worker1jobArgs.JobNumber])
				}
				fmt.Println("write done channel 2")
				doneChannel <- true
			}()
			fmt.Println("created worker 0 map calls")
			<- doneChannel
			fmt.Println("done channel 1")
			<- doneChannel
			fmt.Println("done channel 2")
			go CheckAllReplies(mapReplies, doneChannel)
			<- doneChannel

			for n:=0;n < mr.nReduce; n++ {
				doJobArgs2 := DoJobArgs{mr.file, Reduce, n, mr.nMap}
				reduceQueue <- doJobArgs2
			}
			close(reduceQueue)
			fmt.Println("populated reduce queue")
			go func(){
				for worker1jobArgs := range reduceQueue {
					call(info1.Address(), "Worker.DoJob",worker1jobArgs, &reduceReplies[worker1jobArgs.JobNumber])
				}
				doneChannel <- true
				fmt.Println("reduce routine 1 done")
			}()
			go func(){
				for worker2jobArgs := range reduceQueue {
					call(info2.Address(), "Worker.DoJob",worker2jobArgs, &reduceReplies[worker2jobArgs.JobNumber])
				}
				doneChannel <- true
				fmt.Println(" reduce routine 2 done")
			}()

			fmt.Println("launched reduce goroutines")

		  // mr.Workers["first"] := info1
		  // mr.Workers["second"] := info2
		  // c1,err1 := net.Dial("unix",workerStrings[0])
		  // c2,err2 := net.Dial("unix",workerStrings[1])
		  // if err1 == nil && err2 == nil {
			 //  c1.Write([]byte("hi1\n"))
			 //  c2.Write([]byte("hi2\n"))
		  // }
			<- doneChannel
			<- doneChannel
			go CheckAllReplies(reduceReplies, doneChannel)
			<- doneChannel

			fmt.Println("sending mr.donechannel=true")

		  // mr.DoneChannel <- true
		// keys := make([]int, 0, len(mr.Workers))
    // for key,worker := range mr.Workers {
    // 	fmt.Println("worker:",worker)
    // 	fmt.Println("key:",key)
    //   // keys = append(keys, k)
    // }

   //  for key := range keys {
			// fmt.Println(key)
   //  }

    // fmt.Println(keys)

	return mr.KillWorkers()
}
