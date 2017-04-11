package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

var port int
var nodename string
var nodetimeout time.Duration

func init() {
	flag.IntVar(&port, "port", 6666, "the port the server is supposed to be running on")
	flag.DurationVar(&nodetimeout, "nodetimeout", time.Second*10, "time waiting for node to start")
	var err error
	nodename, err = os.Hostname()
	if err != nil {
		log.Fatalln("fatal:", err)
	}
}

func split2(s string, by string, left interface{}, right interface{}) error {
	splitresult := strings.SplitN(s, by, 2)
	if len(splitresult) != 2 {
		return errors.New("string does not match format")
	}
	_, err := fmt.Sscan(splitresult[0], left)
	if err != nil {
		return err
	}
	_, err = fmt.Sscan(splitresult[1], right)
	return err
}

type trackedProcess struct {
	cmd        *exec.Cmd
	exited     bool
	exitStatus int
	waitExited chan bool
}

func newTrackedProcess(startedCommand *exec.Cmd) *trackedProcess {
	tp := &trackedProcess{
		cmd:        startedCommand,
		exited:     false,
		exitStatus: -1,
		waitExited: make(chan bool),
	}
	go func() {
		err := startedCommand.Wait()
		tp.exited = true
		close(tp.waitExited)
		if err == nil {
			tp.exitStatus = 0
			log.Println("process", tp.cmd.Process.Pid, "exited successfully")
		} else {
			if exiterr, ok := err.(*exec.ExitError); ok {
				if waitStatus, ok := exiterr.Sys().(syscall.WaitStatus); ok {
					tp.exitStatus = waitStatus.ExitStatus()
					log.Println("process", tp.cmd.Process.Pid, "exited with status", tp.exitStatus)
					return
				}
			}
			log.Fatalf("unexpected error for %v, %v", startedCommand, err)
		}
	}()
	return tp
}

type NodeWorker struct {
	name      string
	processes map[string]*trackedProcess
	count     chan int
}

func NewNodeWorker() *NodeWorker {
	nw := &NodeWorker{
		name:      nodename,
		processes: make(map[string]*trackedProcess),
		count:     make(chan int),
	}
	go func() {
		for i := 0; true; i++ {
			nw.count <- i
		}
	}()
	return nw
}

func (nw *NodeWorker) newProcessID() string {
	return fmt.Sprintf("%v.%v", nw.name, <-nw.count)
}

type LaunchArgs struct {
	Name string   `json:"name"`
	Args []string `json:"args"`
	Env  []string `json:"env"`
	Cwd  string   `json:"cwd"`
	CPUs int      `json:"ncpu"`
}

type LaunchReply struct {
	ID string `json:"id"`
}

func (nw *NodeWorker) Launch(args *LaunchArgs, reply *LaunchReply) error {
	cmd := exec.Command(args.Name, args.Args...)
	cmd.Env = args.Env
	cmd.Dir = args.Cwd
	id := nw.newProcessID()
	stdout, err := os.Create(id + "o")
	if err != nil {
		return err
	}
	defer stdout.Close()
	stderr, err := os.Create(id + "e")
	if err != nil {
		return err
	}
	defer stderr.Close()
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	err = cmd.Start()
	if err != nil {
		return err
	}
	log.Printf("launched %s: %d %s", id, cmd.Process.Pid, args.Name)
	nw.processes[id] = newTrackedProcess(cmd)
	reply.ID = id
	return nil
}

type QueryArgs struct {
	ID string `json:"id"`
}

type QueryReply struct {
	Exited     bool `json:"exited"`
	ExitStatus int  `json:"exit_status"`
}

func (reply *QueryReply) setProcess(tp *trackedProcess) {
	reply.Exited = tp.exited
	reply.ExitStatus = tp.exitStatus
}

func (nw *NodeWorker) findProcess(id string) (*trackedProcess, error) {
	tp, ok := nw.processes[id]
	if !ok {
		return nil, fmt.Errorf("No such process: %v", id)
	}
	return tp, nil
}

func (nw *NodeWorker) Query(args *QueryArgs, reply *QueryReply) error {
	tp, err := nw.findProcess(args.ID)
	if err != nil {
		return err
	}
	reply.setProcess(tp)
	return nil
}

func (nw *NodeWorker) Wait(args *QueryArgs, reply *QueryReply) error {
	tp, err := nw.findProcess(args.ID)
	if err != nil {
		return err
	}
	<-tp.waitExited
	reply.setProcess(tp)
	return nil
}

type SignalArgs struct {
	ID     string
	Signal int
}

func (nw *NodeWorker) Signal(args *SignalArgs, reply *QueryReply) error {
	tp, err := nw.findProcess(args.ID)
	if err != nil {
		return err
	}
	if !tp.exited {
		err = syscall.Kill(tp.cmd.Process.Pid, syscall.Signal(args.Signal))
		log.Printf(
			"sent signal %v to %v, pid: %v",
			args.Signal,
			args.ID,
			tp.cmd.Process.Pid,
		)
	}
	reply.setProcess(tp)
	return err
}

func jsonrpcDialTimeout(network, address string, timeout time.Duration) (*rpc.Client, error) {
	conn, err := net.DialTimeout(network, address, timeout)
	if err != nil {
		return nil, err
	}
	return jsonrpc.NewClient(conn), nil
}

func rpcMain(rcvrs ...interface{}) {
	rpc := rpc.NewServer()
	for _, rcvr := range rcvrs {
		err := rpc.Register(rcvr)
		if err != nil {
			log.Fatalf("fatal: error registering rpc: %v", err)
		}
	}

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("fatal: listen error: %v", err)
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatalf("fatal: accept error: %v", err)
		}
		go rpc.ServeCodec(jsonrpc.NewServerCodec(conn))
	}
}

func nodeWorkerMain() {
	log.Println("node running at port", port)
	rpcMain(NewNodeWorker())
}

type Node struct {
	Name     string
	CPUs     int
	busyCPUs int
	rpc      *rpc.Client
	sshCmd   *exec.Cmd
}

func (node *Node) startWorker() {
	abspath, err := filepath.Abs(os.Args[0])
	if err != nil {
		log.Fatalln("fatal:", err)
	}
	localname, err := os.Hostname()
	if err != nil {
		log.Fatalln("fatal:", err)
	}
	if node.Name != localname {
		// maybe implement using ssh library sometime
		sshCmd := exec.Command("ssh", "-t", "-t", node.Name, "--", abspath, "node")
		sshCmd.Stdout = os.Stdout
		sshCmd.Stderr = os.Stderr
		err := sshCmd.Start()
		if err != nil {
			log.Fatalln("fatal:", err)
		}
		node.sshCmd = sshCmd
	}
	for rt := nodetimeout; true; rt -= time.Second {
		sleepCh := time.After(time.Second)
		var rpc *rpc.Client
		rpc, err = jsonrpcDialTimeout(
			"tcp",
			fmt.Sprintf("%s:%d", node.Name, port),
			rt,
		)
		if err == nil {
			node.rpc = rpc
			log.Println("started node worker at", node.Name)
			return
		}
		<-sleepCh
	}
	log.Fatalln("fatal:", err)
}

func (node *Node) stopWorker() {
	if node.sshCmd != nil {
		node.sshCmd.Process.Signal(os.Interrupt)
	}
}

type Master struct {
	Nodes map[string]*Node
}

func (m *Master) Launch(args *LaunchArgs, reply *LaunchReply) error {
	requestedCPUs := args.CPUs
	if requestedCPUs < 1 {
		requestedCPUs = 1
	}
	for _, node := range m.Nodes {
		if requestedCPUs+node.busyCPUs <= node.CPUs {
			err := node.rpc.Call("NodeWorker.Launch", args, reply)
			if err == nil {
				node.busyCPUs += requestedCPUs
				log.Printf("%v CPU utilization %v/%v", node.Name, node.busyCPUs, node.CPUs)
				go func() {
					var qreply QueryReply
					node.rpc.Call(
						"NodeWorker.Wait",
						&QueryArgs{ID: reply.ID},
						&qreply)
					node.busyCPUs -= requestedCPUs
				}()
			}
			return err
		}
	}
	return errors.New("No idle CPU available")
}

func (m *Master) proxyQuery(serviceMethod string, id string, args interface{}, reply *QueryReply) error {
	var hostname string
	var subid int
	err := split2(id, ".", &hostname, &subid)
	if err != nil {
		return err
	}
	node, ok := m.Nodes[hostname]
	if !ok {
		return fmt.Errorf("No such node: %q", hostname)
	}
	return node.rpc.Call("NodeWorker."+serviceMethod, args, reply)
}

func (m *Master) Query(args *QueryArgs, reply *QueryReply) error {
	return m.proxyQuery("Query", args.ID, args, reply)
}

func (m *Master) Signal(args *SignalArgs, reply *QueryReply) error {
	return m.proxyQuery("Signal", args.ID, args, reply)
}

func (m *Master) Wait(args *QueryArgs, reply *QueryReply) error {
	return m.proxyQuery("Wait", args.ID, args, reply)
}

type WaitAnyArgs struct {
	IDs []string `json:"ids"`
}

type WaitAnyReply struct {
	ID     string      `json:"id"`
	Status *QueryReply `json:"status"`
}

func (m *Master) WaitAny(args *WaitAnyArgs, reply *WaitAnyReply) error {
	if len(args.IDs) == 0 {
		return errors.New("require at least 1 id to wait")
	}
	// chech for process existence
	for _, id := range args.IDs {
		if err := m.Query(&QueryArgs{ID: id}, new(QueryReply)); err != nil {
			return err
		}
	}
	donechan := make(chan *rpc.Call, len(args.IDs))
	for _, id := range args.IDs {
		var hostname string
		var subid int
		split2(id, ".", &hostname, &subid)
		node := m.Nodes[hostname]
		node.rpc.Go("NodeWorker.Wait", &QueryArgs{ID: id}, new(QueryReply), donechan)
	}
	firstReturningCall := <-donechan
	if firstReturningCall.Error != nil {
		return firstReturningCall.Error
	}
	reply.ID = firstReturningCall.Args.(*QueryArgs).ID
	reply.Status = firstReturningCall.Reply.(*QueryReply)
	return nil
}

func getNodesFromMachineFile(filename string) map[string]*Node {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalln("fatal:", err)
	}
	defer file.Close()
	result := make(map[string]*Node)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), ":", 2)
		if len(parts) != 2 {
			log.Fatalf("bad word: %q", scanner.Text())
		}
		hostname := parts[0]
		var cpucount int
		_, err := fmt.Sscan(parts[1], &cpucount)
		if err != nil {
			log.Fatalf("bad word (cpus): %q", scanner.Text())
		}
		result[hostname] = &Node{
			Name: hostname,
			CPUs: cpucount,
		}
	}
	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading input: %v", err)
	}
	return result
}

func masterMain() {
	var master Master
	master.Nodes = getNodesFromMachineFile("machinefile")
	go rpcMain(&master, NewNodeWorker())
	for _, node := range master.Nodes {
		node.startWorker()
	}
	log.Println("master running at port", port)

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt)
	<-sigchan
	fmt.Println("received Ctrl+C, quitting")
	for _, node := range master.Nodes {
		node.stopWorker()
	}
}

func main() {
	log.SetFlags(log.Ltime | log.Lshortfile)
	log.SetPrefix(nodename + " ")
	flag.Parse()
	if len(os.Args) != 2 {
		log.Fatalf("usage: %v {master,node}", os.Args[0])
	}
	command := os.Args[1]
	if command == "master" {
		masterMain()
	} else if command == "node" {
		nodeWorkerMain()
	} else {
		log.Fatal("unknown command: ", command)
	}
}
