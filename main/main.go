package main

import (
	"fmt"
	"go_raft/src"
	"log"
	"os"
	"time"
)

func main() {

	args := os.Args
	if len(args) < 4 {
		fmt.Println("Usage: <IP:PORT> <IP:PORT> <IP:PORT> ...")
		return
	}

	r, err := raft.NewRaft(args[1], args[2:])
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	file, err := os.OpenFile(r.GetLogFileName(), os.O_WRONLY, 0)
	if err != nil {
		file, err = os.Create(r.GetLogFileName())
		if err != nil {
			fmt.Println("create file error!")
			return
		}
		fmt.Println("create log file ", r.GetLogFileName())
	} else {
		fileinfo, err := file.Stat()
		if err == nil {
			file.Seek(fileinfo.Size(), 0)
			fmt.Println("append log to file ", file.Name())
		}
	}

	defer file.Close()

	log.SetOutput(file)

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
		//log.Println("pass ", i, " second.")
	}

	log.Println("Server stop ok!")

	raft.Stop(r)
}
