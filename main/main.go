package main

import (
	"go_raft/src"
	"log"
	"os"
	"time"
)

func main() {

	args := os.Args
	if len(args) < 4 {
		log.Println("Usage: <IP:PORT> <IP:PORT> <IP:PORT> ...")
		return
	}

	r, err := raft.NewRaft(args[1], args[2:])
	if err != nil {
		log.Println(err.Error())
		return
	}

	err = raft.Start(r)
	if err != nil {
		log.Println(err.Error())
		return
	}

	log.Println("Server start ok!")

	// for test
	val := (raft.GetRandInt() % 100) + 10

	log.Println("proccess maybe exit at ", val, " second laster.")

	for i := 0; i < val; i++ {

		// run forever not to stop
		time.Sleep(time.Duration(time.Second))
		log.Println("pass ", i, " second.")
	}

	log.Println("Server stop ok!")

	raft.Stop(r)
}
