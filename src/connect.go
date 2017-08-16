package raft

import (
	"errors"
	"log"
	"net/rpc"
	"sync"
	"time"
)

const (
	PING_TIMEOUT = 2 * time.Second
)

type Node struct {
	enable bool
	name   string
	client *rpc.Client
}

type Cluster struct {
	num   int
	node  map[string]*Node
	lock  *sync.Mutex
	wait  *sync.WaitGroup
	timer *time.Timer
}

func NewCluster() *Cluster {
	c := new(Cluster)

	c.num = 0
	c.lock = new(sync.Mutex)
	c.wait = new(sync.WaitGroup)
	c.node = make(map[string]*Node)
	c.timer = time.NewTimer(PING_TIMEOUT)

	go KeepConnect(c)

	c.wait.Add(1)

	return c
}

func (c *Cluster) Close() {

	c.lock.Lock()
	defer c.lock.Unlock()

	for _, node := range c.node {
		if node == nil {
			continue
		}

		if node.enable == false {
			continue
		}

		err := node.client.Close()
		if err != nil {
			log.Println(err.Error())
		}
	}

	c.wait.Wait()
}

func (c *Cluster) AddNode(name string) {

	c.lock.Lock()
	defer c.lock.Unlock()

	_, b := c.node[name]
	if b == true {
		return
	}

	newnode := new(Node)
	newnode.enable = false
	newnode.name = name

	c.node[name] = newnode
	c.num++
}

func (c *Cluster) DelNode(name string) {

	c.lock.Lock()
	defer c.lock.Unlock()

	delnode, b := c.node[name]
	if b == false {
		return
	}

	if delnode.enable {
		err := delnode.client.Close()
		if err != nil {
			log.Println(err.Error())
		}
	}

	delete(c.node, name)
	c.num--
}

func KeepConnect(c *Cluster) {

	defer c.wait.Done()

	for {
		_, b := <-c.timer.C
		if b == false {
			return
		}

		for _, node := range c.node {
			if node.enable == true {
				continue
			}

			client, err := rpc.DialHTTP("tcp", node.name)
			if err != nil {
				log.Println(err.Error())
				continue
			}

			c.lock.Lock()
			node.client = client
			node.enable = true
			c.lock.Unlock()
		}

		c.timer.Reset(PING_TIMEOUT)
	}
}

func (c *Cluster) SyncCall(serviceName, serviceMethod string, args interface{}, reply interface{}, timeout time.Duration) error {

	tm := time.NewTimer(timeout)

	c.lock.Lock()
	node, b := c.node[serviceName]
	if b == false {
		c.lock.Unlock()
		return errors.New("have not add node " + serviceName)
	}

	if node.enable == false {
		c.lock.Unlock()
		return errors.New("have not connect to " + serviceName)
	}

	call := node.client.Go(serviceMethod, args, reply, nil)
	c.lock.Unlock()

	select {
	case result := <-call.Done:
		{
			if result.Error != nil {

				c.lock.Lock()
				node.client.Close()
				node.enable = false
				c.lock.Unlock()

				return result.Error
			} else {
				return nil
			}
		}
	case <-tm.C:
		{
			return errors.New("call " + serviceName + " timeout " + string(timeout))
		}
	}

	return nil
}

func (c *Cluster) AsyncCall(serviceName, serviceMethod string, args interface{}, reply interface{}, done chan *rpc.Call) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	node, b := c.node[serviceName]
	if b == false {
		c.lock.Unlock()
		return errors.New("have not add node " + serviceName)
	}

	if node.enable == false {
		c.lock.Unlock()
		return errors.New("have not connect to " + serviceName)
	}

	node.client.Go(serviceMethod, args, reply, done)

	c.lock.Unlock()

	return nil
}
