package raft

import (
	"log"
)

const (
	CHANGE_ROLE_F_TO_C = 1
	CHANGE_ROLE_C_TO_L = 2
	CHANGE_ROLE_L_TO_F = 3
	CHANGE_ROLE_C_TO_F = 4
)

func TimeOut(r *RAFT) {

	log.Println("Timer Start ")

	defer r.wait.Done()
	defer log.Println("Timer Close")

	for {
		_, b := <-r.timeout.C
		if b == false {
			return
		}

		log.Printf("Now Self : %s , Role: %s , Term %d , Leader %s \r\n", r.name, r.role, r.curTerm, r.leader)

		switch r.role {
		case ROLE_LEADER:
			{
				KeepAlive(r)
			}
		case ROLE_CANDIDATE:
			{
				LeaderVote(r)
			}
		case ROLE_FOLLOWER:
			{
				r.mlock.Lock()
				r.role = ROLE_CANDIDATE
				r.leader = ""
				r.curTerm++
				r.mlock.Unlock()

				LeaderVote(r)
			}
		default:
			log.Println("Recv Stop Raft")
			return
		}
	}
}

func ChangeRole_F_TO_C(r *RAFT) {

}

func KeepAlive(r *RAFT) {

	log.Println("KeepAlive timer start ")

	totalnum := len(r.othername)
	queue := make(chan bool, totalnum)

	for _, othername := range r.othername {

		go func(name string) {

			request := new(LeaderInfo)
			response := new(bool)

			request.ServerName = r.name
			request.Term = r.curTerm

			err := r.otherserver.SyncCall(name, "RAFT.Heartbeats", request, response, RPC_TIMEOUT)
			if err != nil {
				log.Println(err.Error())
				queue <- false
				return
			}
			log.Println("Recv Heartbeats ", name, *response)
			if *response {
				queue <- true
			} else {
				queue <- false
			}
		}(othername)
	}

	agreenum := 0
	for i := 0; i < totalnum; i++ {
		response := <-queue
		if response == true {
			agreenum++
		}
	}

	close(queue)

	if agreenum == 0 {
		r.mlock.Lock()

		r.leader = ""
		r.role = ROLE_FOLLOWER
		r.timeout.Reset(GetDymicTimeOut())

		r.mlock.Unlock()
	} else {

		r.mlock.Lock()
		r.timeout.Reset(KEEP_ALIVE)
		r.mlock.Unlock()
	}
}

func LeaderVote(r *RAFT) {

	log.Println("Start LeaderVote for ", r.name)

	r.mlock.Lock()
	if r.leader != "" {
		r.mlock.Unlock()

		log.Println("Stop LeaderVote for ", r.leader)
		return
	}
	r.leader = r.name
	r.mlock.Unlock()

	totalnum := len(r.othername)
	queue := make(chan bool, totalnum)

	for _, othername := range r.othername {

		go func(name string) {

			request := new(LeaderInfo)
			response := new(bool)

			request.ServerName = r.name
			request.Term = r.curTerm

			err := r.otherserver.SyncCall(name, "RAFT.RequestVote", request, response, RPC_TIMEOUT)
			if err != nil {
				log.Println(err.Error())
				queue <- false
				return
			}

			log.Println("Recv Heartbeats ", name, *response)
			if *response {
				queue <- true
			} else {
				queue <- false
			}
		}(othername)
	}

	for i := 0; i < totalnum; i++ {
		<-queue
	}

	for _, othername := range r.othername {

		go func(name string) {

			request := new(LeaderInfo)
			response := new(bool)

			request.ServerName = r.name
			request.Term = r.curTerm

			err := r.otherserver.SyncCall(name, "RAFT.AppendEntries", request, response, RPC_TIMEOUT)
			if err != nil {
				log.Println(err.Error())
				queue <- false
				return
			}

			log.Println("Recv Heartbeats ", name, *response)
			if *response {
				queue <- true
			} else {
				queue <- false
			}
		}(othername)
	}

	agreenum := 0
	for i := 0; i < totalnum; i++ {
		response := <-queue
		if response == true {
			agreenum++
		}
	}

	close(queue)

	if agreenum*2 >= totalnum {

		r.mlock.Lock()
		r.role = ROLE_LEADER
		r.timeout.Reset(KEEP_ALIVE)
		r.mlock.Unlock()

		log.Println("LeaderVote success! ")
	} else {

		r.mlock.Lock()
		r.leader = ""
		r.timeout.Reset(GetDymicTimeOut())
		r.mlock.Unlock()

		log.Println("LeaderVote failed! ")
	}

	log.Println("LeaderVote timer close ")
}
