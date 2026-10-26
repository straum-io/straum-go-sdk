package client

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/straum-io/straum-go-sdk/internal/wire"
	"github.com/straum-io/straum-go-sdk/straumerr"
)

// ErrConnection marks a failure of the connection itself: dial, write,
// read, or a connection closed before the response. The client retries the
// request on a new connection (docs/protocol.md section 6, rule 3); the
// error reaches the caller after the last attempt.
var ErrConnection = errors.New("straum: connection failed")

// ErrProtocol marks a response that does not decode or does not answer the
// request. It is not retried.
var ErrProtocol = errors.New("straum: protocol violation")

// ErrInFlightFull is returned instead of waiting when the connection's
// in-flight bytes are at Config.MaxInFlightBytes and Config.FailWhenFull
// is set.
var ErrInFlightFull = errors.New("straum: in-flight bytes at the limit")

// ErrClosed is returned by calls on a closed Client.
var ErrClosed = errors.New("straum: client closed")

type connError struct {
	op, addr string
	err      error
}

func (e *connError) Error() string   { return fmt.Sprintf("straum: %s %s: %v", e.op, e.addr, e.err) }
func (e *connError) Unwrap() []error { return []error{ErrConnection, e.err} }

// budget bounds the bytes of requests sent and not yet answered on one
// connection.
type budget struct {
	mu      sync.Mutex
	used    int
	limit   int
	changed chan struct{} // closed and replaced on every release
}

func newBudget(limit int) *budget { return &budget{limit: limit, changed: make(chan struct{})} }

// acquire takes n bytes, waiting for releases unless failFast. A request
// larger than the limit is admitted alone, when nothing else is in flight.
// ponytail: no FIFO among waiters; a large request can wait behind small ones.
func (b *budget) acquire(ctx context.Context, n int, failFast bool, done <-chan struct{}) error {
	for {
		b.mu.Lock()
		if b.used == 0 || b.used+n <= b.limit {
			b.used += n
			b.mu.Unlock()
			return nil
		}
		ch := b.changed
		b.mu.Unlock()
		if failFast {
			return ErrInFlightFull
		}
		select {
		case <-ch:
		case <-done:
			return nil // the caller sees the connection's error next
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (b *budget) release(n int) {
	b.mu.Lock()
	b.used -= n
	close(b.changed)
	b.changed = make(chan struct{})
	b.mu.Unlock()
}

type result struct {
	resp wire.Response
	err  error
}

type pendingCall struct {
	ch     chan result // buffered: the reader never blocks on it
	weight int
	want   uint16 // the success response type
}

// conn is one TCP connection with pipelining: any number of goroutines
// send requests, each waits for the response with its correlation id.
type conn struct {
	nc       net.Conn
	addr     string
	hello    wire.HelloOk
	nextID   atomic.Uint64
	wlock    chan struct{} // write lock, held by a send in progress
	budget   *budget
	failFast bool

	mu      sync.Mutex
	pending map[uint64]*pendingCall
	err     error         // first failure; set once
	done    chan struct{} // closed on failure
}

func dialConn(ctx context.Context, cfg *Config, addr string) (*conn, error) {
	if cfg.ConnectTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.ConnectTimeout)
		defer cancel()
	}
	nc, err := cfg.Dial(ctx, "tcp", addr)
	if err != nil {
		if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
			return nil, ctx.Err()
		}
		return nil, &connError{"dial", addr, err}
	}
	c := &conn{
		nc:       nc,
		addr:     addr,
		wlock:    make(chan struct{}, 1),
		budget:   newBudget(cfg.MaxInFlightBytes),
		failFast: cfg.FailWhenFull,
		pending:  make(map[uint64]*pendingCall),
		done:     make(chan struct{}),
	}
	go c.readLoop(cfg.MaxResponseBytes)
	resp, err := c.roundTrip(ctx, &wire.Hello{
		MinVersion:    wire.Version,
		MaxVersion:    wire.Version,
		ClientName:    cfg.ClientName,
		AuthMechanism: cfg.Auth.Mechanism,
		AuthPayload:   cfg.Auth.Payload,
	})
	if err != nil {
		c.close(err)
		return nil, err
	}
	switch r := resp.(type) {
	case *wire.HelloOk:
		if r.Version != wire.Version {
			err = fmt.Errorf("%w: broker chose version %d, offered %d", ErrProtocol, r.Version, wire.Version)
			c.close(err)
			return nil, err
		}
		c.hello = *r
		return c, nil
	case *wire.Error:
		err := wire.ErrorFromResponse(r)
		c.close(err)
		return nil, err
	}
	return nil, nil // unreachable: roundTrip checks the response type
}

func (c *conn) dead() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

// close fails the connection with err: pending calls get it, the socket is
// closed. Only the first call has an effect.
func (c *conn) close(err error) {
	c.mu.Lock()
	if c.err != nil {
		c.mu.Unlock()
		return
	}
	c.err = err
	pending := c.pending
	c.pending = nil
	close(c.done)
	c.mu.Unlock()
	_ = c.nc.Close()
	for _, p := range pending {
		c.budget.release(p.weight)
		p.ch <- result{err: err}
	}
}

// roundTrip sends req and waits for its response. An Error response is
// returned as a *wire.Error response, not as an error.
func (c *conn) roundTrip(ctx context.Context, req wire.Request) (wire.Response, error) {
	id := c.nextID.Add(1) // never 0, never reused on this connection
	buf := frameBufs.Get().(*[]byte)
	frame, err := wire.AppendRequest((*buf)[:0], id, req)
	if err != nil {
		frameBufs.Put(buf)
		return nil, err
	}
	if limit := c.hello.MaxFrameBytes; limit != 0 && uint64(len(frame)) > uint64(limit) {
		// The broker would answer FrameTooLarge and close the connection.
		return nil, &straumerr.Error{Code: straumerr.FrameTooLarge,
			Message: fmt.Sprintf("client: frame of %d bytes exceeds the broker's max_frame_bytes %d", len(frame), limit)}
	}
	weight := min(len(frame), c.budget.limit)
	if err := c.budget.acquire(ctx, weight, c.failFast, c.done); err != nil {
		return nil, err
	}
	call := &pendingCall{ch: make(chan result, 1), weight: weight, want: req.Type() | wire.ResponseBit}
	c.mu.Lock()
	if c.err != nil {
		err := c.err
		c.mu.Unlock()
		c.budget.release(weight)
		return nil, err
	}
	c.pending[id] = call
	c.mu.Unlock()

	err = c.write(ctx, frame)
	recycle(buf, frame)
	if errors.Is(err, errNotSent) {
		// Cancelled before taking the write lock: nothing was sent, the
		// connection and the other calls on it are unaffected.
		c.mu.Lock()
		_, mine := c.pending[id]
		delete(c.pending, id)
		c.mu.Unlock()
		if mine {
			c.budget.release(weight)
		}
		return nil, ctx.Err()
	}
	if err != nil {
		if ctx.Err() != nil {
			// A partial frame may be on the wire: the connection is unusable.
			c.close(&connError{"write", c.addr, ctx.Err()})
			return nil, ctx.Err()
		}
		c.close(&connError{"write", c.addr, err})
		return nil, c.err
	}
	select {
	case r := <-call.ch:
		return r.resp, r.err
	case <-ctx.Done():
		// The reader releases the budget when the response or the
		// connection's failure arrives.
		return nil, ctx.Err()
	}
}

// frameBufs holds encode buffers. A frame is not referenced after write
// returns, so the next request reuses its buffer instead of growing a new
// one by appends.
var frameBufs = sync.Pool{New: func() any { return new([]byte) }}

// recycle returns a frame's buffer to frameBufs unless it is larger than
// 4 MiB, so one large request does not keep its memory in the pool.
func recycle(buf *[]byte, frame []byte) {
	if cap(frame) <= 4<<20 {
		*buf = frame[:0]
		frameBufs.Put(buf)
	}
}

// errNotSent: write gave up before taking the write lock.
var errNotSent = errors.New("straum: request not sent")

func (c *conn) write(ctx context.Context, frame []byte) error {
	select {
	case c.wlock <- struct{}{}:
	case <-c.done:
		return c.err
	case <-ctx.Done():
		return errNotSent
	}
	defer func() { <-c.wlock }()
	if ctx.Err() != nil {
		return errNotSent // select picks at random when both are ready
	}
	deadline, _ := ctx.Deadline()
	if err := c.nc.SetWriteDeadline(deadline); err != nil {
		return err
	}
	stop := context.AfterFunc(ctx, func() { _ = c.nc.SetWriteDeadline(time.Unix(1, 0)) })
	defer stop()
	_, err := c.nc.Write(frame)
	return err
}

func (c *conn) readLoop(maxResponse int) {
	r := bufio.NewReaderSize(c.nc, 64<<10)
	var head [wire.HeaderSize]byte
	for {
		if _, err := io.ReadFull(r, head[:]); err != nil {
			c.close(&connError{"read", c.addr, err})
			return
		}
		h, err := wire.ParseHeader(head[:])
		if err == nil && h.BodyLen() > maxResponse {
			err = fmt.Errorf("response frame of %d bytes exceeds MaxResponseBytes %d", h.BodyLen(), maxResponse)
		}
		if err != nil {
			c.close(&connError{"read", c.addr, fmt.Errorf("%w: %v", ErrProtocol, err)})
			return
		}
		body := make([]byte, h.BodyLen())
		if _, err := io.ReadFull(r, body); err != nil {
			c.close(&connError{"read", c.addr, err})
			return
		}
		c.mu.Lock()
		call := c.pending[h.CorrelationID]
		delete(c.pending, h.CorrelationID)
		c.mu.Unlock()
		if call == nil {
			continue // not a request of this connection (a broker error with id 0)
		}
		c.budget.release(call.weight)
		resp, err := wire.DecodeResponse(h.Type, body)
		switch {
		case err != nil:
			err = fmt.Errorf("%w: response type %#06x: %v", ErrProtocol, h.Type, err)
		case h.Type != call.want && h.Type != wire.TypeError:
			resp, err = nil, fmt.Errorf("%w: response type %#06x to a request expecting %#06x", ErrProtocol, h.Type, call.want)
		}
		call.ch <- result{resp, err}
	}
}
