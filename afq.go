package main

import (
	"bufio"
	"container/heap"
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
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var port int
var isNode bool
var nodename string
var nodetimeout time.Duration
var nodecwd string
var executable string

func init() {
	flag.IntVar(&port, "port", 6666, "the port the server is supposed to be running on")
	flag.DurationVar(&nodetimeout, "nodetimeout", time.Second*10, "time waiting for node to start")
	flag.StringVar(&nodecwd, "nodecwd", ".", "node working directory")
	flag.BoolVar(&isNode, "node", false, "run node worker")
	var err error
	nodename, err = os.Hostname()
	if err != nil {
		log.Fatalln("fatal:", err)
	}
	executable, err = os.Executable()
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
		if err == nil {
			tp.exitStatus = 0
			close(tp.waitExited)
			log.Println("process", tp.cmd.Process.Pid, "exited successfully")
		} else {
			if exiterr, ok := err.(*exec.ExitError); ok {
				if waitStatus, ok := exiterr.Sys().(syscall.WaitStatus); ok {
					tp.exitStatus = waitStatus.ExitStatus()
					close(tp.waitExited)
					log.Println("process", tp.cmd.Process.Pid, "exited with status", tp.exitStatus)
					return
				}
			}
			close(tp.waitExited)
			log.Fatalf("unexpected error for %v, %v", startedCommand, err)
		}
	}()
	return tp
}

// NodeWorker -- process manager running on nodes
type NodeWorker struct {
	name            string
	processMap      map[string]*trackedProcess
	processMapMutex sync.RWMutex
	counter         int32
}

// NewNodeWorker returns a initialized NodeWorker
func NewNodeWorker() *NodeWorker {
	nw := &NodeWorker{
		name:       nodename,
		processMap: make(map[string]*trackedProcess),
	}
	return nw
}

// newProcessID returns a unique process id for the node
func (nw *NodeWorker) newProcessID() string {
	return fmt.Sprintf("%v.%v", nw.name, atomic.AddInt32(&nw.counter, 1))
}

// getProcess returns a trackedProcess in node.processMap
func (nw *NodeWorker) getProcess(id string) (process *trackedProcess, ok bool) {
	nw.processMapMutex.RLock()
	process, ok = nw.processMap[id]
	nw.processMapMutex.RUnlock()
	return
}

// NodeLaunchArgs -- the command, arguments, environment, working directory of the command
type NodeLaunchArgs struct {
	Name string   `json:"name"`
	Args []string `json:"args"`
	Env  []string `json:"env"`
	Cwd  string   `json:"cwd"`
}

// NodeLaunchReply -- the reply of launching on a node
type NodeLaunchReply struct {
	// The ID of the process on the node,
	// this is unlikely to be the same as the ID on the master
	ID string `json:"id"`
}

// Launch a process on the node worker, returning the ID
func (nw *NodeWorker) Launch(args *NodeLaunchArgs, reply *NodeLaunchReply) error {
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
	nw.processMapMutex.Lock()
	nw.processMap[id] = newTrackedProcess(cmd)
	nw.processMapMutex.Unlock()
	reply.ID = id
	return nil
}

// QueryArgs -- arguments to perform Query and Wait
type QueryArgs struct {
	ID string `json:"id"`
}

// QueryReply -- result of Query and Wait
type QueryReply struct {
	Exited     bool `json:"exited"`
	ExitStatus int  `json:"exit_status"`
}

func (reply *QueryReply) setProcess(tp *trackedProcess) {
	reply.Exited = tp.exited
	reply.ExitStatus = tp.exitStatus
}

func (nw *NodeWorker) findProcess(id string) (*trackedProcess, error) {
	tp, ok := nw.getProcess(id)
	if !ok {
		return nil, fmt.Errorf("No such process: %v", id)
	}
	return tp, nil
}

// Query a process by its ID, return its state and exit status
func (nw *NodeWorker) Query(args *QueryArgs, reply *QueryReply) error {
	tp, err := nw.findProcess(args.ID)
	if err != nil {
		return err
	}
	reply.setProcess(tp)
	return nil
}

// Wait for a process to exit, by its ID, return its state and exit status
func (nw *NodeWorker) Wait(args *QueryArgs, reply *QueryReply) error {
	tp, err := nw.findProcess(args.ID)
	if err != nil {
		return err
	}
	<-tp.waitExited
	reply.setProcess(tp)
	return nil
}

// SignalArgs -- Signal of Arguments of Signal
type SignalArgs struct {
	ID     string
	Signal int
}

// Signal a process by its ID and signal number, return its state and exit status
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
	err := os.Chdir(nodecwd)
	if err != nil {
		log.Fatalf("fatal: error changing directory %v", err)
	}
	log.Println("node running at port", port)
	rpcMain(NewNodeWorker())
}

// Resource represents CPU, memory, etc
type Resource struct {
	max       int
	available int
	sync.Mutex
}

// NewResource creates a new Resource with the specified number of units
func NewResource(max int) *Resource {
	return &Resource{max: max, available: max}
}

// Max returns the maximum number of resource
func (r *Resource) Max() int {
	return r.max
}

// TestAcquire tests whtehter the requested amount of resource is available
// if yes, acquires the resource
// returns whether the resource is acquired and the amount remaining
func (r *Resource) TestAcquire(amount int) (bool, int) {
	r.Lock()
	defer r.Unlock()
	if r.available >= amount {
		r.available -= amount
		return true, r.available
	}
	return false, r.available
}

// Release the specified amount of resource, returns the amount available
func (r *Resource) Release(amount int) (remaining int) {
	r.Lock()
	r.available += amount
	remaining = r.available
	r.Unlock()
	return
}

// Node -- master abstraction of a node worker
type Node struct {
	Name   string
	cpu    *Resource
	rpc    *rpc.Client
	sshCmd *exec.Cmd
}

func (node *Node) startWorker() {
	localname, err := os.Hostname()
	if err != nil {
		log.Fatalln("fatal:", err)
	}
	if node.Name != localname {
		// maybe implement using ssh library sometime
		wd, err := os.Getwd()
		if err != nil {
			log.Fatalf("fatal: getwd %v", err)
		}
		sshCmd := exec.Command("ssh", "-t", "-t", node.Name, "--", executable, "-node", "-nodecwd", wd)
		sshCmd.Stdout = os.Stdout
		sshCmd.Stderr = os.Stderr
		err = sshCmd.Start()
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

// masterProcess -- abstraction of a tracked process on the master
type masterProcess struct {
	args       *LaunchArgs
	node       *Node
	remoteID   string
	launched   chan struct{} // channel, closed after the process is launched
	exited     chan struct{} // channel, closed after the process is exited
	exitStatus int
}

func newMasterProcess(args *LaunchArgs) *masterProcess {
	return &masterProcess{
		args:       args,
		launched:   make(chan struct{}),
		exited:     make(chan struct{}),
		exitStatus: -1,
	}
}

func (process *masterProcess) runOn(node *Node) {
	process.node = node
	var launchReply NodeLaunchReply
	err := node.rpc.Call("NodeWorker.Launch", process.args.NodeLaunchArgs, &launchReply)
	if err != nil {
		panic(err)
	}
	process.remoteID = launchReply.ID
	close(process.launched)

	var waitReply QueryReply
	err = node.rpc.Call("NodeWorker.Wait", QueryArgs{ID: launchReply.ID}, &waitReply)
	if err != nil {
		panic(err)
	}
	remaining := node.cpu.Release(process.args.CPUs)
	log.Printf("node %v process exited, cpu: %v/%v", node.Name, remaining, node.cpu.Max())
	process.exitStatus = waitReply.ExitStatus
	close(process.exited)
}

type masterProcessQueueInterface []*masterProcess

func (q masterProcessQueueInterface) Len() int { return len(q) }

func (q masterProcessQueueInterface) Less(i, j int) bool {
	return q[i].args.CPUs < q[j].args.CPUs
}

func (q masterProcessQueueInterface) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
}

func (q *masterProcessQueueInterface) Push(x interface{}) {
	typed := x.(*masterProcess)
	*q = append(*q, typed)
}

func (q *masterProcessQueueInterface) Pop() interface{} {
	old := *q
	n := old.Len()
	item := old[n-1]
	*q = old[0 : n-1]
	return item
}

type masterProcessQueue struct {
	data           masterProcessQueueInterface
	enqueueRequest chan *masterProcess
	dequeueRequest chan chan *masterProcess
}

func newMasterProcessQueue() (q *masterProcessQueue) {
	q = &masterProcessQueue{
		enqueueRequest: make(chan *masterProcess),
		dequeueRequest: make(chan chan *masterProcess),
	}
	go func() {
		push := func(process *masterProcess) {
			heap.Push(&q.data, process)
			log.Printf("enqueued job: %v, in queue: %v", process.args.Name, q.data.Len())
		}
		for {
			if q.data.Len() == 0 {
				push(<-q.enqueueRequest)
			} else {
				select {
				case process := <-q.enqueueRequest:
					push(process)
				case popchan := <-q.dequeueRequest:
					popchan <- heap.Pop(&q.data).(*masterProcess)
				}
			}
		}
	}()
	return
}

func (q *masterProcessQueue) Enqueue(process *masterProcess) {
	q.enqueueRequest <- process
}

func (q *masterProcessQueue) Dequeue() *masterProcess {
	resultChan := make(chan *masterProcess)
	q.dequeueRequest <- resultChan
	return <-resultChan
}

// Master -- RPC Master
type Master struct {
	nodes            []*Node
	processes        []*masterProcess
	processArrayLock sync.RWMutex
	queue            *masterProcessQueue
}

// LaunchArgs -- arguments to Master.Launch
type LaunchArgs struct {
	NodeLaunchArgs
	CPUs int `json:"ncpu"`
}

// LaunchReply -- result of Master.Launch
type LaunchReply struct {
	// The ID on the master, this is unlikely to be the same as the one on the node
	ID string `json:"id"`
}

// Submit a process
func (m *Master) Submit(args *LaunchArgs, reply *LaunchReply) error {
	if args.CPUs < 1 {
		args.CPUs = 1
	}
	process := newMasterProcess(args)
	m.processArrayLock.Lock()
	reply.ID = fmt.Sprint(len(m.processes))
	m.processes = append(m.processes, process)
	m.processArrayLock.Unlock()
	m.queue.Enqueue(process)
	return nil
}

// getProcess -- get the process of the given id
func (m *Master) findProcess(id string) (*masterProcess, error) {
	m.processArrayLock.Lock()
	defer m.processArrayLock.Unlock()
	index, err := strconv.ParseInt(id, 10, 32)
	if err != nil {
		return nil, err
	}
	if int(index) < len(m.processes) {
		return m.processes[index], nil
	}
	return nil, fmt.Errorf("No such process: %v", id)
}

// Query the process
func (m *Master) Query(args *QueryArgs, reply *QueryReply) error {
	process, err := m.findProcess(args.ID)
	if err != nil {
		return err
	}
	select {
	case <-process.exited:
		reply.Exited = true
		reply.ExitStatus = process.exitStatus
	default:
		reply.Exited = false
		reply.ExitStatus = -1
	}
	return nil
}

// Signal -- send a signal to the process
func (m *Master) Signal(args *SignalArgs, reply *QueryReply) error {
	process, err := m.findProcess(args.ID)
	if err != nil {
		return err
	}
	select {
	case <-process.launched:
		return process.node.rpc.Call(
			"NodeWorker.Signal",
			SignalArgs{
				ID:     process.remoteID,
				Signal: args.Signal,
			},
			reply,
		)
	default:
		return errors.New("process has not yet been started")
	}
}

// Wait for a process to exit, returning its status
func (m *Master) Wait(args *QueryArgs, reply *QueryReply) error {
	process, err := m.findProcess(args.ID)
	if err != nil {
		return err
	}
	<-process.exited
	reply.Exited = true
	reply.ExitStatus = process.exitStatus
	return nil
}

// WaitAnyArgs -- argument for Master.WaitAny
type WaitAnyArgs struct {
	IDs []string `json:"ids"`
}

// WaitAnyReply -- argument for Master.WaitAny
type WaitAnyReply struct {
	ID     string     `json:"id"`
	Status QueryReply `json:"status"`
}

// WaitAny waits for any process to exit
// ids should be a array containing at least one id
// if any id specified in ids is invalid, errors.
func (m *Master) WaitAny(args *WaitAnyArgs, reply *WaitAnyReply) error {
	if len(args.IDs) == 0 {
		return errors.New("require at least 1 id to wait")
	}
	// chech for process existence
	cases := make([]reflect.SelectCase, len(args.IDs))
	for index, id := range args.IDs {
		var err error
		process, err := m.findProcess(id)
		if err != nil {
			return err
		}
		cases[index] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(process.exited),
		}
	}
	index, _, _ := reflect.Select(cases)
	completedID := args.IDs[index]
	process, err := m.findProcess(completedID)
	if err != nil {
		panic(err)
	}
	reply.ID = completedID
	reply.Status.Exited = true
	reply.Status.ExitStatus = process.exitStatus
	return nil
}

func getNodesFromMachineFile(filename string) (result []*Node) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalln("fatal:", err)
	}
	defer file.Close()
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
		result = append(result, &Node{
			Name: hostname,
			cpu:  NewResource(cpucount),
		})
	}
	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading input: %v", err)
	}
	return
}

func (m *Master) launchLoop() {
	recheck := make(chan struct{}, 1)
	notify := func() {
		select {
		case recheck <- struct{}{}:
		default:
		}
	}
	for {
		process := m.queue.Dequeue()
		notify()
		for _ = range recheck {
			for _, node := range m.nodes {
				acquired, remaining := node.cpu.TestAcquire(process.args.CPUs)
				if acquired {
					log.Printf("launch on %s, cpu: %v/%v", node.Name, remaining, node.cpu.Max())
					go func() {
						// release of resource is done in runOn()
						process.runOn(node)
						notify()
					}()
					goto doNext
				}
			}
		}
	doNext:
	}
}

func masterMain() {
	master := Master{
		nodes: getNodesFromMachineFile("machinefile"),
		queue: newMasterProcessQueue(),
	}
	go rpcMain(&master, NewNodeWorker())

	for _, node := range master.nodes {
		node.startWorker()
	}
	log.Println("master running at port", port)

	go master.launchLoop()

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt)
	<-sigchan
	fmt.Println("received Ctrl+C, quitting")
	for _, node := range master.nodes {
		node.stopWorker()
	}
}

func main() {
	log.SetFlags(log.Ltime | log.Lshortfile)
	log.SetPrefix(nodename + " ")
	flag.Parse()
	if isNode {
		nodeWorkerMain()
	} else {
		masterMain()
	}
}
