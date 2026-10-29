package main

import (
	"io"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Proxy forwards one directed node-to-node link (A's traffic to B) and can be
// partitioned. A partitioned proxy blackholes traffic: bytes are read and
// discarded, nothing is forwarded, and new connections are accepted but go
// nowhere. Peers therefore see timeouts rather than fast connection errors,
// which is what a real network partition looks like.
type Proxy struct {
	listener    net.Listener
	target      string
	partitioned atomic.Bool
	// Each forwarded chunk is delayed by a uniform random duration in
	// [0, maxLatency), modelling network jitter; order is preserved.
	maxLatency time.Duration

	mu    sync.Mutex
	conns map[net.Conn]struct{}
}

func NewProxy(listenAddr, target string, maxLatency time.Duration) (*Proxy, error) {
	l, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}
	p := &Proxy{listener: l, target: target, maxLatency: maxLatency, conns: map[net.Conn]struct{}{}}
	go p.acceptLoop()
	return p, nil
}

func (p *Proxy) SetPartitioned(on bool) {
	p.partitioned.Store(on)
	if on {
		// In-flight connections were established before the partition; cut
		// them so no buffered request slips through.
		p.mu.Lock()
		for c := range p.conns {
			c.Close()
		}
		p.conns = map[net.Conn]struct{}{}
		p.mu.Unlock()
	}
}

func (p *Proxy) Close() {
	p.listener.Close()
	p.SetPartitioned(true)
}

func (p *Proxy) track(c net.Conn) {
	p.mu.Lock()
	p.conns[c] = struct{}{}
	p.mu.Unlock()
}

func (p *Proxy) acceptLoop() {
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}
		go p.handle(client)
	}
}

func (p *Proxy) handle(client net.Conn) {
	p.track(client)
	if p.partitioned.Load() {
		io.Copy(io.Discard, client)
		client.Close()
		return
	}
	upstream, err := net.Dial("tcp", p.target)
	if err != nil {
		client.Close()
		return
	}
	p.track(upstream)
	go p.pipe(upstream, client)
	p.pipe(client, upstream)
}

func (p *Proxy) pipe(dst, src net.Conn) {
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 && p.maxLatency > 0 {
			time.Sleep(time.Duration(rand.Int63n(int64(p.maxLatency))))
		}
		if n > 0 && !p.partitioned.Load() {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				break
			}
		}
		if err != nil {
			break
		}
	}
	dst.Close()
	src.Close()
}
