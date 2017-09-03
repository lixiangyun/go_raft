package raft

import (
	"crypto/rand"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"strings"
	"sync"
	"time"
)

const (
	ROLE_FOLLOWER  = "Follower"
	ROLE_CANDIDATE = "Candidate"
	ROLE_LEADER    = "Leader"
)

const (
	RPC_TIMEOUT = 500 * time.Millisecond
	KEEP_ALIVE  = 1 * time.Second
)

type LeaderInfo struct {
	ServerName string
	Term       uint64
}

var debug bool

type RAFT struct {
	role string
	port string
	name string

	curTerm uint64
	leader  string
	votecnt int

	othername   []string
	otherserver *Cluster

	lis net.Listener

	timeout *time.Timer
	mlock   *sync.Mutex

	wait *sync.WaitGroup
}

func (r *RAFT) GetLogFileName() string {
	return fmt.Sprintf("runlog_%s.txt", r.port)
}

func Log(v ...interface{}) {
	if debug == true {
		log.Println(v)
	}
}

func GetRandInt() int {

	var buf [100]byte
	var val int

	rand.Read(buf[0:])

	for i := 0; i < 25; i++ {
		val += int(buf[i*4]) + int(buf[i*4+1])<<8 + int(buf[i*4+2])<<16 + int(buf[i*4+3])<<24
	}

	return val
}

func GetDymicTimeOut() (tm time.Duration) {
	val := GetRandInt() % 3000
	tm = 3*time.Second + time.Duration(val)*time.Millisecond
	Log("get timeout length : ", tm.Seconds())
	return
}

func (r *RAFT) RequestVote(l *LeaderInfo, b *bool) error {

	Log("new leader vote request for :", l.ServerName)

	r.mlock.Lock()
	defer r.mlock.Unlock()

	*b = false

	switch r.role {
	case ROLE_CANDIDATE:
		{
			if l.Term >= r.curTerm {
				*b = true
			}
		}
	case ROLE_FOLLOWER:
		{
			if l.Term >= r.curTerm {
				*b = true
			}
		}
	case ROLE_LEADER:
		{
			if l.Term > r.curTerm {
				*b = true
			}
		}
	}

	if *b == true {

		r.leader = l.ServerName
		r.role = ROLE_FOLLOWER
		r.curTerm = l.Term

		r.timeout.Reset(GetDymicTimeOut())

		Log("agree new Leader :", l.ServerName)

	} else {
		Log("not agree vote for leader :", l.ServerName)
	}

	return nil
}

func (r *RAFT) AppendEntries(l *LeaderInfo, b *bool) error {

	r.mlock.Lock()
	defer r.mlock.Unlock()

	if r.role == ROLE_FOLLOWER && r.leader == l.ServerName {
		r.timeout.Reset(GetDymicTimeOut())

		*b = true
		Log("agree append entries request from Leader :", l.ServerName)
	} else {

		*b = false
		Log("reject append entries request from Leader :", l.ServerName)
	}

	return nil
}

func (r *RAFT) Heartbeats(l *LeaderInfo, b *bool) error {

	*b = false

	r.mlock.Lock()
	defer r.mlock.Unlock()

	switch r.role {
	case ROLE_FOLLOWER:
		{
			if l.Term >= r.curTerm {
				*b = true
			}
		}
	case ROLE_CANDIDATE:
		{
			if l.Term >= r.curTerm {
				*b = true
			}
		}
	case ROLE_LEADER:
		{
			if l.Term > r.curTerm {
				*b = true
			}
		}
	}

	if *b == true {

		r.role = ROLE_FOLLOWER
		r.leader = l.ServerName
		r.curTerm = l.Term

		r.timeout.Reset(GetDymicTimeOut())

		Log("agree heartbeats from leader ", l.ServerName)
	} else {

		Log("reject heartbeats from Leader :", l.ServerName)
	}

	return nil
}

func TimeOut(r *RAFT) {

	Log("Timer Start ")

	defer r.wait.Done()
	defer Log("Timer Close")

	for {
		_, b := <-r.timeout.C
		if b == false {
			return
		}

		log := fmt.Sprintf("name: %s , role: %s , term: %d , leader: %s ",
			r.name, r.role, r.curTerm, r.leader)
		Log(log)

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
			Log("Recv Stop Raft")
			return
		}
	}
}

func KeepAlive(r *RAFT) {

	Log("keep alive start")

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
				Log(err.Error())
				queue <- false
				return
			}
			Log("recv heartbeats ", name, *response)
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

	r.mlock.Lock()

	if agreenum == 0 {
		r.leader = ""
		r.role = ROLE_FOLLOWER
		r.timeout.Reset(GetDymicTimeOut())

		Log("not any partner response")

	} else {
		r.timeout.Reset(KEEP_ALIVE)

		Log("get ", agreenum, " partners response")
	}

	r.mlock.Unlock()
}

func LeaderVote(r *RAFT) {

	Log("start vote for leader ", r.name)

	r.mlock.Lock()
	if r.leader != "" {
		r.mlock.Unlock()

		Log("stop vote for leader ", r.leader)
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
				Log(err.Error())
				queue <- false
				return
			}

			Log("recv request vote ", name, *response)
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
				Log(err.Error())
				queue <- false
				return
			}

			Log("recv append entries ", name, *response)
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

		Log("leader vote success! ")
	} else {

		r.mlock.Lock()
		r.leader = ""
		r.timeout.Reset(GetDymicTimeOut())
		r.mlock.Unlock()

		Log("leader vote failed! not enough partners. totalnum: ", totalnum, " agreenum: ", agreenum)
	}
}

func Debug(b bool) {
	debug = b
}

func NewRaft(selfaddr string, otheraddr []string) (*RAFT, error) {

	idx := strings.Index(selfaddr, ":")
	if idx == -1 {
		return nil, errors.New("Input Selfaddr invailed : " + selfaddr)
	}

	r := new(RAFT)

	r.name = selfaddr
	r.port = selfaddr[idx+1:]
	r.role = ROLE_FOLLOWER

	r.curTerm = 0
	r.leader = ""
	r.votecnt = 0

	r.otherserver = NewCluster()
	r.othername = otheraddr

	for _, v := range otheraddr {
		r.otherserver.AddNode(v)
	}

	r.wait = new(sync.WaitGroup)
	r.mlock = new(sync.Mutex)

	return r, nil
}

func Start(r *RAFT) error {

	Log("server start listen")

	server := rpc.NewServer()

	err := server.Register(r)
	if err != nil {
		Log(err.Error())
		return err
	}

	server.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)

	listen, err := net.Listen("tcp", ":"+r.port)
	if err != nil {
		Log(err.Error())
		return err
	}

	r.lis = listen

	r.wait.Add(2)

	r.timeout = time.NewTimer(GetDymicTimeOut())
	go TimeOut(r)

	go func() {

		defer r.wait.Done()

		err = http.Serve(listen, nil)

		Log("server shut down!")

		if err != nil {
			Log(err.Error())
			return
		}
	}()

	return nil
}

func Stop(r *RAFT) {

	r.otherserver.Close()

	r.mlock.Lock()
	r.role = ""
	r.lis.Close()
	r.timeout.Reset(0)
	r.mlock.Unlock()

	r.wait.Wait()
}
