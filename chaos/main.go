// Command chaos is a Jepsen-style test for ShardCache: it runs a 3-node
// cluster whose inter-node links pass through fault-injecting proxies,
// drives it with concurrent clients while a nemesis partitions, pauses and
// crashes nodes, records every operation, and checks the history for
// linearizability with Porcupine.
package main

import (
	"errors"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/anishathalye/porcupine"
)

const numNodes = 3

var (
	binary      = flag.String("binary", "../build/shardcache_app", "path to shardcache_app")
	duration    = flag.Duration("duration", 30*time.Second, "workload duration")
	clients     = flag.Int("clients", 9, "concurrent clients")
	keys        = flag.Int("keys", 4, "number of distinct keys (few keys = more contention)")
	faults      = flag.String("faults", "partition,pause", "comma-separated nemesis faults: none, partition, pause, kill")
	interval    = flag.Duration("interval", 4*time.Second, "time between nemesis actions")
	consistency = flag.String("consistency", "", "CONSISTENCY mode passed to the nodes (empty = server default)")
	outDir      = flag.String("out", "out", "directory for logs and the visualization")
	seed        = flag.Int64("seed", time.Now().UnixNano(), "random seed")
	basePort    = flag.Int("port", 7501, "first node port")
	latency     = flag.Duration("latency", 0, "max random one-way delay added to inter-node messages")
)

type node struct {
	id     string
	port   int
	cmd    *exec.Cmd
	log    *os.File
	exited chan struct{} // closed when the process exits
}

type cluster struct {
	nodes   []*node
	proxies [numNodes][numNodes]*Proxy // proxies[from][to]
	mu      sync.Mutex
}

func proxyPort(from, to int) int { return *basePort + 100 + from*10 + to }

func (c *cluster) start() error {
	for i := 0; i < numNodes; i++ {
		c.nodes = append(c.nodes, &node{id: fmt.Sprintf("n%d", i+1), port: *basePort + i})
	}
	for from := 0; from < numNodes; from++ {
		for to := 0; to < numNodes; to++ {
			if from == to {
				continue
			}
			p, err := NewProxy(fmt.Sprintf("127.0.0.1:%d", proxyPort(from, to)),
				fmt.Sprintf("127.0.0.1:%d", c.nodes[to].port), *latency)
			if err != nil {
				return err
			}
			c.proxies[from][to] = p
		}
	}
	for i := range c.nodes {
		if err := c.launch(i); err != nil {
			return err
		}
	}
	return waitReady(c.nodes)
}

// launch starts node i; its view of every peer goes through that link's proxy.
func (c *cluster) launch(i int) error {
	n := c.nodes[i]
	var peers []string
	for j, other := range c.nodes {
		port := other.port
		if j != i {
			port = proxyPort(i, j)
		}
		peers = append(peers, fmt.Sprintf("%s:127.0.0.1:%d", other.id, port))
	}
	logFile, err := os.OpenFile(filepath.Join(*outDir, n.id+".log"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	cmd := exec.Command(*binary)
	cmd.Env = append(os.Environ(),
		"NODE_ID="+n.id,
		fmt.Sprintf("PORT=%d", n.port),
		"CLUSTER_PEERS="+strings.Join(peers, ","),
		"IO_THREADS=2",
		"RPC_THREADS=16",
	)
	if *consistency != "" {
		cmd.Env = append(cmd.Env, "CONSISTENCY="+*consistency)
	}
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		return err
	}
	n.cmd, n.log = cmd, logFile
	n.exited = make(chan struct{})
	go func() {
		cmd.Wait()
		close(n.exited)
	}()
	return nil
}

func waitReady(nodes []*node) error {
	for _, n := range nodes {
		// Something else answering on the port (e.g. a leftover process)
		// must not count as our node being up.
		select {
		case <-n.exited:
			return fmt.Errorf("node %s exited during startup; see %s.log", n.id, n.id)
		case <-time.After(150 * time.Millisecond):
		}
		ready := false
		for i := 0; i < 100 && !ready; i++ {
			c := NewRespClient(fmt.Sprintf("127.0.0.1:%d", n.port), 200*time.Millisecond)
			if r, err := c.Do("PING"); err == nil && r.Str == "PONG" {
				ready = true
			}
			c.Close()
			time.Sleep(50 * time.Millisecond)
		}
		if !ready {
			return fmt.Errorf("node %s did not start", n.id)
		}
	}
	return nil
}

func (c *cluster) stop() {
	for _, n := range c.nodes {
		if n.cmd != nil && n.cmd.Process != nil {
			n.cmd.Process.Signal(syscall.SIGCONT)
			n.cmd.Process.Kill()
			<-n.exited
			n.log.Close()
		}
	}
	for from := range c.proxies {
		for to := range c.proxies[from] {
			if p := c.proxies[from][to]; p != nil {
				p.Close()
			}
		}
	}
}

// isolate cuts every link to and from node i.
func (c *cluster) isolate(i int, on bool) {
	for j := 0; j < numNodes; j++ {
		if j == i {
			continue
		}
		c.proxies[i][j].SetPartitioned(on)
		c.proxies[j][i].SetPartitioned(on)
	}
}

func (c *cluster) signal(i int, sig syscall.Signal) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nodes[i].cmd.Process.Signal(sig)
}

func (c *cluster) restart(i int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := c.nodes[i]
	n.cmd.Process.Kill()
	<-n.exited
	n.log.Close()
	if err := c.launch(i); err != nil {
		return err
	}
	return waitReady([]*node{n})
}

type event struct {
	at   time.Duration
	desc string
}

func runNemesis(c *cluster, kinds []string, rng *rand.Rand, start time.Time, stop <-chan struct{}, log func(string)) {
	if len(kinds) == 0 || kinds[0] == "none" {
		return
	}
	for {
		select {
		case <-stop:
			return
		case <-time.After(*interval / 2):
		}
		target := rng.Intn(numNodes)
		kind := kinds[rng.Intn(len(kinds))]
		hold := *interval / 2
		switch kind {
		case "partition":
			log(fmt.Sprintf("partition: isolate %s", c.nodes[target].id))
			c.isolate(target, true)
			sleepOrStop(hold, stop)
			c.isolate(target, false)
			log("heal partition")
		case "pause":
			log(fmt.Sprintf("pause %s (SIGSTOP)", c.nodes[target].id))
			c.signal(target, syscall.SIGSTOP)
			sleepOrStop(hold, stop)
			c.signal(target, syscall.SIGCONT)
			log(fmt.Sprintf("resume %s", c.nodes[target].id))
		case "kill":
			log(fmt.Sprintf("crash and restart %s (memory lost)", c.nodes[target].id))
			if err := c.restart(target); err != nil {
				log("restart failed: " + err.Error())
			}
		}
	}
}

func sleepOrStop(d time.Duration, stop <-chan struct{}) {
	select {
	case <-stop:
	case <-time.After(d):
	}
}

type stats struct {
	mu                                sync.Mutex
	ok, failed, indeterminate, errors map[string]int
}

func newStats() *stats {
	return &stats{ok: map[string]int{}, failed: map[string]int{}, indeterminate: map[string]int{}, errors: map[string]int{}}
}

func (s *stats) add(m map[string]int, k string) {
	s.mu.Lock()
	m[k]++
	s.mu.Unlock()
}

func runClient(id int, c *cluster, start time.Time, deadline time.Time, rng *rand.Rand, st *stats) []porcupine.Operation {
	var history []porcupine.Operation
	target := c.nodes[id%numNodes]
	client := NewRespClient(fmt.Sprintf("127.0.0.1:%d", target.port), 1500*time.Millisecond)
	defer client.Close()
	now := func() int64 { return time.Since(start).Nanoseconds() }

	for seq := 0; time.Now().Before(deadline); seq++ {
		key := fmt.Sprintf("k%d", rng.Intn(*keys))
		var in kvInput
		var args []string
		switch r := rng.Intn(10); {
		case r < 5:
			in = kvInput{Op: opGet, Key: key}
			args = []string{"GET", key}
		case r < 9:
			in = kvInput{Op: opPut, Key: key, Value: fmt.Sprintf("c%d-%d", id, seq)}
			args = []string{"SET", key, in.Value}
		default:
			in = kvInput{Op: opDel, Key: key}
			args = []string{"DEL", key}
		}
		name := args[0]

		call := now()
		reply, err := client.Do(args...)
		ret := now()

		var serverErr *ServerError
		switch {
		case err == nil:
			st.add(st.ok, name)
			out := kvOutput{}
			if in.Op == opGet && !reply.Nil {
				out = kvOutput{Found: true, Value: reply.Str}
			}
			history = append(history, porcupine.Operation{ClientId: id, Input: in, Call: call, Output: out, Return: ret})
		case in.Op == opGet:
			// A failed read has no effect and tells us nothing: drop it.
			st.add(st.failed, name)
		default:
			// A failed or timed-out write may still have reached some
			// replicas (and spread via read repair or hints). It is
			// indeterminate: it may take effect at any point after its call.
			if errors.As(err, &serverErr) {
				st.add(st.failed, name)
				st.add(st.errors, serverErr.Msg)
			} else {
				st.add(st.indeterminate, name)
			}
			history = append(history, porcupine.Operation{ClientId: id, Input: in, Call: call, Output: kvOutput{}, Return: math.MaxInt64})
		}
		if err != nil {
			time.Sleep(20 * time.Millisecond)
		}
	}
	return history
}

func main() {
	flag.Parse()
	// run returns instead of exiting so the cluster is always torn down.
	os.Exit(run())
}

func run() int {
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	rng := rand.New(rand.NewSource(*seed))
	kinds := strings.Split(*faults, ",")
	fmt.Printf("seed=%d faults=%s duration=%s clients=%d keys=%d latency=%s consistency=%q\n",
		*seed, *faults, *duration, *clients, *keys, *latency, *consistency)

	c := &cluster{}
	defer c.stop()
	if err := c.start(); err != nil {
		fmt.Fprintln(os.Stderr, "start:", err)
		return 2
	}

	start := time.Now()
	deadline := start.Add(*duration)
	var events []event
	var evMu sync.Mutex
	logEvent := func(s string) {
		evMu.Lock()
		events = append(events, event{time.Since(start), s})
		evMu.Unlock()
		fmt.Printf("  [%6.2fs] %s\n", time.Since(start).Seconds(), s)
	}

	stopNemesis := make(chan struct{})
	nemesisDone := make(chan struct{})
	go func() {
		runNemesis(c, kinds, rand.New(rand.NewSource(rng.Int63())), start, stopNemesis, logEvent)
		close(nemesisDone)
	}()

	st := newStats()
	var wg sync.WaitGroup
	histories := make([][]porcupine.Operation, *clients)
	for i := 0; i < *clients; i++ {
		wg.Add(1)
		clientRng := rand.New(rand.NewSource(rng.Int63()))
		go func(i int) {
			defer wg.Done()
			histories[i] = runClient(i, c, start, deadline, clientRng, st)
		}(i)
	}
	wg.Wait()
	close(stopNemesis)
	<-nemesisDone

	var history []porcupine.Operation
	for _, h := range histories {
		history = append(history, h...)
	}

	fmt.Println("operations:")
	for _, name := range []string{"GET", "SET", "DEL"} {
		fmt.Printf("  %-4s ok=%-6d failed=%-5d indeterminate=%d\n", name, st.ok[name], st.failed[name], st.indeterminate[name])
	}
	for msg, n := range st.errors {
		fmt.Printf("  error %q x%d\n", msg, n)
	}

	if n, examples := staleReads(history); n > 0 {
		fmt.Printf("stale reads: %d\n", n)
		for _, e := range examples {
			fmt.Println("  " + e)
		}
	} else {
		fmt.Println("stale reads: 0")
	}

	if n, examples := inversions(history); n > 0 {
		fmt.Printf("new-old inversions: %d\n", n)
		for _, e := range examples {
			fmt.Println("  " + e)
		}
	} else {
		fmt.Println("new-old inversions: 0")
	}

	result, info := porcupine.CheckOperationsVerbose(kvModel, history, 60*time.Second)
	viz := filepath.Join(*outDir, "linearizability.html")
	if err := porcupine.VisualizePath(kvModel, info, viz); err == nil {
		fmt.Println("visualization:", viz)
	}
	fmt.Println("linearizable:", result)
	switch result {
	case porcupine.Ok:
		return 0
	case porcupine.Illegal:
		return 1
	default:
		return 3
	}
}
