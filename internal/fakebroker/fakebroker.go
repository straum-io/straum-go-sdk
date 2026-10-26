// Package fakebroker is an in-process broker for unit tests: it answers
// Hello itself and passes every other request to a handler.
package fakebroker

import (
	"io"
	"net"
	"strconv"
	"sync"
	"testing"

	"github.com/straum-io/straum-go-sdk/wire"
)

// Conn is one accepted connection.
type Conn struct {
	net.Conn
	Index int // 0 for the first connection accepted, 1 for the next, …
	mu    sync.Mutex
}

// Reply sends resp with the correlation id corr. Safe to call from any
// goroutine.
func (c *Conn) Reply(corr uint64, resp wire.Response) {
	b, err := wire.AppendResponse(nil, corr, resp)
	if err != nil {
		panic(err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	_, _ = c.Write(b)
}

// Handler answers a request; it may reply later, from another goroutine,
// or not at all.
type Handler func(c *Conn, corr uint64, req wire.Request)

// Broker listens on a loopback port.
type Broker struct {
	ln    net.Listener
	hello wire.Response
	mu    sync.Mutex
	conns []*Conn
}

// DefaultHello is the HelloOk of Start.
var DefaultHello = wire.HelloOk{ClusterID: "fake", NodeID: 1, MaxFrameBytes: 8 << 20, MaxMessageBytes: 1 << 20}

// Start runs a broker until the test ends; it answers Hello with
// DefaultHello.
func Start(t testing.TB, h Handler) *Broker {
	t.Helper()
	hello := DefaultHello
	return StartHello(t, &hello, h)
}

// StartHello is Start with the given answer to Hello.
func StartHello(t testing.TB, hello wire.Response, h Handler) *Broker {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	b := &Broker{ln: ln, hello: hello}
	t.Cleanup(b.Close)
	go func() {
		for i := 0; ; i++ {
			nc, err := ln.Accept()
			if err != nil {
				return
			}
			c := &Conn{Conn: nc, Index: i}
			b.mu.Lock()
			b.conns = append(b.conns, c)
			b.mu.Unlock()
			go serve(c, b.hello, h)
		}
	}()
	return b
}

// Addr is the broker's "host:port".
func (b *Broker) Addr() string { return b.ln.Addr().String() }

// HostPort splits Addr.
func (b *Broker) HostPort() (string, uint16) {
	host, port, _ := net.SplitHostPort(b.Addr())
	p, _ := strconv.Atoi(port)
	return host, uint16(p)
}

// Accepted is the number of connections accepted so far.
func (b *Broker) Accepted() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.conns)
}

// Close stops listening and closes every connection.
func (b *Broker) Close() {
	_ = b.ln.Close()
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, c := range b.conns {
		_ = c.Close()
	}
}

func serve(c *Conn, hello wire.Response, h Handler) {
	defer c.Close()
	head := make([]byte, wire.HeaderSize)
	for {
		if _, err := io.ReadFull(c, head); err != nil {
			return
		}
		fh, err := wire.ParseHeader(head)
		if err != nil {
			return
		}
		body := make([]byte, fh.BodyLen())
		if _, err := io.ReadFull(c, body); err != nil {
			return
		}
		req, err := wire.DecodeRequest(fh.Type, body)
		if err != nil {
			return
		}
		if _, ok := req.(*wire.Hello); ok {
			c.Reply(fh.CorrelationID, hello)
			continue
		}
		h(c, fh.CorrelationID, req)
	}
}
