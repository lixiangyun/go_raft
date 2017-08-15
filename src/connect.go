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

type Node struct {
	enable bool
	name   string
	client *rpc.Client
}

var node map[string]*Node

func AddNode(name string) {
	_, b := node[name]
	if b == true {
		return
	}

	newnode := new(Node)
	newnode.enable = false
	newnode.name = name
	newnode.client, err = rpc.DialHTTP("tcp", name)

	node[name] = newnode
}

func KeepConnect()
