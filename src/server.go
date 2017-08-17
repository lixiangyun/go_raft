package raft

import (
	"crypto/rand"
	"errors"
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

type RAFT struct {
	role    string
	port    string
	name    string
	curTerm uint64

	leader  string
	votecnt int

	othername   []string
	otherserver *Cluster

	lis net.Listener

	timeout *time.Timer
	mlock   *sync.Mutex
	wait    *sync.WaitGroup
}

func GetDymicTimeOut() time.Duration {

	var buf [4]byte
	var val int

	_, err := rand.Read(buf[0:])
	if err != nil {
		val = time.Now().Nanosecond()
	} else {
		val = int(buf[0]) + int(buf[1])<<8 + int(buf[2])<<16 + int(buf[3])<<24
	}

	val = val % 3000

	log.Println("val : ", val)

	return 3*time.Second + time.Duration(val)*time.Millisecond
}

func (r *RAFT) RequestVote(l *LeaderInfo, b *bool) error {

	log.Println("Recv RequestVote from Leader :", l.ServerName)

	r.mlock.Lock()
	defer r.mlock.Unlock()

	*b = false

	switch r.role {
	case ROLE_CANDIDATE:
		{
			if l.Term >= r.curTerm && r.leader == "" {

				r.leader = l.ServerName
				r.role = ROLE_FOLLOWER
				r.curTerm = l.Term
				r.timeout.Reset(GetDymicTimeOut())

				log.Println("change role to follower from Leader :", l.ServerName)

				*b = true
			} else {
				log.Printf("RequestVote Name : %s , Role: %s , Term %d , Leader %s \r\n", r.name, r.role, r.curTerm, r.leader)
			}
		}
	case ROLE_FOLLOWER:
		{
			if l.Term >= r.curTerm && r.leader == "" {

				r.leader = l.ServerName
				r.curTerm = l.Term
				r.timeout.Reset(GetDymicTimeOut())

				log.Println("agree new Leader :", l.ServerName)
				*b = true
			} else {
				log.Printf("RequestVote Name : %s , Role: %s , Term %d , Leader %s \r\n", r.name, r.role, r.curTerm, r.leader)
			}
		}
	case ROLE_LEADER:
		{
			if l.Term > r.curTerm {

				r.leader = l.ServerName
				r.role = ROLE_FOLLOWER
				r.curTerm = l.Term
				r.timeout.Reset(GetDymicTimeOut())

				log.Println("agree new Leader :", l.ServerName)
				*b = true
			} else {
				log.Printf("RequestVote Name : %s , Role: %s , Term %d , Leader %s \r\n", r.name, r.role, r.curTerm, r.leader)
			}
		}
	}
	return nil
}

func (r *RAFT) AppendEntries(l *LeaderInfo, b *bool) error {

	log.Println("Recv AppendEntries from Leader :", l.ServerName)

	r.mlock.Lock()
	defer r.mlock.Unlock()

	*b = false
	if r.role == ROLE_FOLLOWER {
		if r.leader == l.ServerName {
			r.timeout.Reset(GetDymicTimeOut())
			log.Println("agree append entries request from Leader :", l.ServerName)
			*b = true
		} else {
			log.Printf("AppendEntries Name : %s , Role: %s , Term %d , Leader %s \r\n", r.name, r.role, r.curTerm, r.leader)
		}
	} else {
		log.Printf("AppendEntries Name : %s , Role: %s , Term %d , Leader %s \r\n", r.name, r.role, r.curTerm, r.leader)
	}

	return nil
}

func (r *RAFT) Heartbeats(l *LeaderInfo, b *bool) error {

	log.Println("Recv Heartbeats from Leader :", l.ServerName)

	*b = false

	r.mlock.Lock()
	defer r.mlock.Unlock()

	switch r.role {
	case ROLE_FOLLOWER:
		{
			if l.ServerName == r.leader {

				r.timeout.Reset(GetDymicTimeOut())
				log.Println("Get Heartbeats from Leader :", l.ServerName)

				*b = true
			}
		}
	case ROLE_CANDIDATE:
		{
			if l.Term >= r.curTerm {

				r.role = ROLE_FOLLOWER
				r.leader = l.ServerName
				r.curTerm = l.Term
				r.timeout.Reset(GetDymicTimeOut())

				log.Println("follower new Leader :", l.ServerName)
			}
		}
	case ROLE_LEADER:
		{
			if l.Term > r.curTerm {

				r.role = ROLE_FOLLOWER
				r.leader = l.ServerName
				r.curTerm = l.Term
				r.timeout.Reset(GetDymicTimeOut())

				log.Println("follower new Leader :", l.ServerName)
			}
		}
	}

	return nil
}

func NewRaft(selfaddr string, otheraddr []string) (*RAFT, error) {

	idx := strings.Index(selfaddr, ":")
	if idx == -1 {
		return nil, errors.New("Input Selfaddr invailed : " + selfaddr)
	}

	r := new(RAFT)

	r.curTerm = 0
	r.leader = ""
	r.name = selfaddr
	r.othername = otheraddr
	r.port = selfaddr[idx+1:]
	r.role = ROLE_FOLLOWER
	r.votecnt = 0

	r.otherserver = NewCluster()

	for _, v := range otheraddr {
		r.otherserver.AddNode(v)
	}

	r.wait = new(sync.WaitGroup)

	r.mlock = new(sync.Mutex)

	return r, nil
}

func Start(r *RAFT) error {

	log.Println("Server Start")

	server := rpc.NewServer()

	err := server.Register(r)
	if err != nil {
		log.Println(err.Error())
		return err
	}

	server.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)

	listen, err := net.Listen("tcp", ":"+r.port)
	if err != nil {
		log.Println(err.Error())
		return err
	}

	r.lis = listen

	r.wait.Add(2)

	r.timeout = time.NewTimer(GetDymicTimeOut())
	go TimeOut(r)

	go func() {

		defer r.wait.Done()

		err = http.Serve(listen, nil)

		log.Println("Server End")

		if err != nil {
			log.Println(err.Error())
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
