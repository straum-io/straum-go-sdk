// Package client connects to Straum brokers over protocol version 0
// (docs/protocol.md). A Client is safe for concurrent use by many
// goroutines: requests are pipelined on one connection per broker node and
// matched to their responses by correlation id.
//
// Every call takes a context.Context for deadlines and cancellation. The
// client implements the normative client behavior of docs/protocol.md
// section 6: on NotLeader or FencedLeaderEpoch it refreshes Metadata and
// retries at the partition's leader; on Overloaded it backs off; on a lost
// connection it reconnects, says Hello again and retries the same request.
// Retries are bounded by RetryPolicy.MaxAttempts.
//
// The producer, consumer and admin packages build on Client.
package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/straum-io/straum-go-sdk/internal/link"
	"github.com/straum-io/straum-go-sdk/internal/wire"
	"github.com/straum-io/straum-go-sdk/straumerr"
)

func init() {
	link.Do = func(c any, ctx context.Context, req wire.Request) (wire.Response, error) {
		return c.(*Client).do(ctx, nil, func(uint32) wire.Request { return req })
	}
	link.DoPartition = func(c any, ctx context.Context, topic string, partition uint32, build func(uint32) wire.Request) (wire.Response, error) {
		return c.(*Client).do(ctx, &topicPartition{topic, partition}, build)
	}
}

// Config configures a Client. Zero fields take the defaults named.
type Config struct {
	// Addrs are "host:port" addresses of brokers to bootstrap from, tried
	// in order. Required.
	Addrs []string
	// ClientName is sent in Hello for the broker's diagnostics
	// (default "straum-go-sdk").
	ClientName string
	// Dial opens connections (default: a net.Dialer with TCP keep-alive).
	// A TLS dialer goes here once a broker version serves TLS.
	Dial func(ctx context.Context, network, addr string) (net.Conn, error)
	// Auth is the authentication sent in Hello (default: mechanism "none",
	// the only one protocol version 0 supports).
	Auth Auth
	// Retry bounds retries (defaults in RetryPolicy).
	Retry RetryPolicy
	// MaxInFlightBytes bounds the bytes of requests sent and not yet
	// answered on each connection (default 8 MiB). At the bound a request
	// waits for responses, or fails with ErrInFlightFull if FailWhenFull is
	// set; requests are never queued without bound.
	MaxInFlightBytes int
	FailWhenFull     bool
	// MaxResponseBytes refuses a response frame larger than this (default
	// 64 MiB); the connection is closed.
	MaxResponseBytes int
	// ConnectTimeout bounds dialing and Hello (default 10 s), within the
	// call's context.
	ConnectTimeout time.Duration
}

// Auth is a Hello authentication mechanism and its payload.
type Auth struct {
	Mechanism string
	Payload   []byte
}

// RetryPolicy bounds the retries of docs/protocol.md section 6.
type RetryPolicy struct {
	// MaxAttempts counts every attempt of a request, the first included
	// (default 5).
	MaxAttempts int
	// BackoffMin and BackoffMax bound the exponential backoff after
	// Overloaded or a lost connection (defaults 10 ms and 1 s). After
	// Overloaded the wait is at least the broker's retry_after_ms.
	BackoffMin time.Duration
	BackoffMax time.Duration
}

func (c *Config) setDefaults() error {
	if len(c.Addrs) == 0 {
		return errors.New("straum: Config.Addrs is empty")
	}
	if c.ClientName == "" {
		c.ClientName = "straum-go-sdk"
	}
	if c.Dial == nil {
		d := &net.Dialer{KeepAlive: 15 * time.Second}
		c.Dial = d.DialContext
	}
	if c.Auth.Mechanism == "" {
		c.Auth.Mechanism = "none"
	}
	if c.Retry.MaxAttempts <= 0 {
		c.Retry.MaxAttempts = 5
	}
	if c.Retry.BackoffMin <= 0 {
		c.Retry.BackoffMin = 10 * time.Millisecond
	}
	if c.Retry.BackoffMax < c.Retry.BackoffMin {
		c.Retry.BackoffMax = max(time.Second, c.Retry.BackoffMin)
	}
	if c.MaxInFlightBytes <= 0 {
		c.MaxInFlightBytes = 8 << 20
	}
	if c.MaxResponseBytes <= 0 {
		c.MaxResponseBytes = 64 << 20
	}
	if c.ConnectTimeout <= 0 {
		c.ConnectTimeout = 10 * time.Second
	}
	return nil
}

type topicPartition struct {
	topic     string
	partition uint32
}

type leader struct{ node, epoch uint32 }

// Client is a connection pool to the brokers of one cluster.
type Client struct {
	cfg Config

	mu      sync.Mutex
	closed  bool
	conns   map[uint32]*conn // by node id
	nodes   map[uint32]string
	leaders map[topicPartition]leader
	counts  map[string]uint32 // partition count per topic
}

// New connects to the first reachable address of cfg.Addrs and says Hello.
func New(ctx context.Context, cfg Config) (*Client, error) {
	if err := cfg.setDefaults(); err != nil {
		return nil, err
	}
	c := &Client{
		cfg:     cfg,
		conns:   make(map[uint32]*conn),
		nodes:   make(map[uint32]string),
		leaders: make(map[topicPartition]leader),
		counts:  make(map[string]uint32),
	}
	if _, err := c.anyConn(ctx); err != nil {
		return nil, err
	}
	return c, nil
}

// Close closes every connection. Calls in progress fail with ErrClosed.
func (c *Client) Close() error {
	c.mu.Lock()
	conns := c.conns
	c.conns = map[uint32]*conn{}
	c.closed = true
	c.mu.Unlock()
	for _, cn := range conns {
		cn.close(ErrClosed)
	}
	return nil
}

// Hello returns what a connected broker reported in its HelloOk: the
// cluster id, its node id and its frame and message limits.
func (c *Client) Hello(ctx context.Context) (BrokerInfo, error) {
	cn, err := c.anyConn(ctx)
	if err != nil {
		return BrokerInfo{}, err
	}
	return BrokerInfo(cn.hello), nil
}

// live returns the open connection to node, dropping a failed one.
func (c *Client) live(node uint32) *conn {
	cn := c.conns[node]
	if cn != nil && cn.dead() {
		delete(c.conns, node)
		return nil
	}
	return cn
}

// adopt registers a new connection under the node id its broker reported,
// or closes it if another goroutine connected to that node first.
// ponytail: concurrent first calls to one node may each dial once; the
// extra connections are closed here.
func (c *Client) adopt(cn *conn) (*conn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		cn.close(ErrClosed)
		return nil, ErrClosed
	}
	if old := c.live(cn.hello.NodeID); old != nil {
		cn.close(ErrClosed)
		return old, nil
	}
	c.conns[cn.hello.NodeID] = cn
	return cn, nil
}

// anyConn returns a connection to any broker, connecting to the bootstrap
// addresses, then to the nodes from Metadata, if none is open.
func (c *Client) anyConn(ctx context.Context) (*conn, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrClosed
	}
	for id := range c.conns {
		if cn := c.live(id); cn != nil {
			c.mu.Unlock()
			return cn, nil
		}
	}
	addrs := append([]string(nil), c.cfg.Addrs...)
	for _, a := range c.nodes {
		addrs = append(addrs, a)
	}
	c.mu.Unlock()
	var errs []error
	for _, a := range addrs {
		cn, err := dialConn(ctx, &c.cfg, a)
		if err == nil {
			return c.adopt(cn)
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		var se *straumerr.Error
		if errors.As(err, &se) {
			return nil, err // the broker refused the Hello; another one would too
		}
		errs = append(errs, err)
	}
	return nil, errors.Join(errs...)
}

// nodeConn returns a connection to node, dialing its address from the last
// Metadata.
func (c *Client) nodeConn(ctx context.Context, node uint32) (*conn, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrClosed
	}
	if cn := c.live(node); cn != nil {
		c.mu.Unlock()
		return cn, nil
	}
	addr, ok := c.nodes[node]
	c.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("%w: Metadata has no address for node %d", ErrProtocol, node)
	}
	cn, err := dialConn(ctx, &c.cfg, addr)
	if err != nil {
		return nil, err
	}
	return c.adopt(cn)
}

func (c *Client) route(ctx context.Context, tp *topicPartition) (*conn, error) {
	if tp != nil {
		c.mu.Lock()
		l, ok := c.leaders[*tp]
		c.mu.Unlock()
		if ok {
			return c.nodeConn(ctx, l.node)
		}
	}
	return c.anyConn(ctx)
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Client) do(ctx context.Context, tp *topicPartition, build func(uint32) wire.Request) (wire.Response, error) {
	var epoch uint32
	backoff := c.cfg.Retry.BackoffMin
	wait := func(atLeast time.Duration) error {
		err := sleep(ctx, max(backoff, atLeast))
		backoff = min(backoff*2, c.cfg.Retry.BackoffMax)
		return err
	}
	for attempt := 1; ; attempt++ {
		last := attempt >= c.cfg.Retry.MaxAttempts
		cn, err := c.route(ctx, tp)
		var resp wire.Response
		if err == nil {
			resp, err = cn.roundTrip(ctx, build(epoch))
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if !last && errors.Is(err, ErrConnection) {
				// Rule 3: reconnect on the next attempt, same request.
				if err := wait(0); err != nil {
					return nil, err
				}
				continue
			}
			return nil, err
		}
		e, ok := resp.(*wire.Error)
		if !ok {
			return resp, nil
		}
		serr := wire.ErrorFromResponse(e)
		if last {
			return nil, serr
		}
		switch e.Code.Retry() {
		case straumerr.RetryAfterMetadata:
			// Rule 1: refresh Metadata, retry at the leader with its epoch.
			if tp == nil {
				return nil, serr
			}
			epoch, err = c.refreshLeader(ctx, cn, *tp)
			if err != nil && !errors.Is(err, ErrConnection) {
				return nil, err
			}
		case straumerr.RetryAfterBackoff:
			// Rule 2: wait at least retry_after_ms, else exponential backoff.
			if err := wait(time.Duration(e.RetryAfterMs) * time.Millisecond); err != nil {
				return nil, err
			}
		default:
			return nil, serr // rule 4
		}
	}
}

// refreshLeader asks cn for the topic's Metadata and returns the
// partition's leader epoch; the next attempt goes to the leader it names.
func (c *Client) refreshLeader(ctx context.Context, cn *conn, tp topicPartition) (uint32, error) {
	resp, err := cn.roundTrip(ctx, &wire.Metadata{Topics: []string{tp.topic}})
	if err != nil {
		return 0, err
	}
	m, ok := resp.(*wire.MetadataOk)
	if !ok {
		return 0, wire.ErrorFromResponse(resp.(*wire.Error))
	}
	c.learn(m)
	c.mu.Lock()
	l, ok := c.leaders[tp]
	c.mu.Unlock()
	if !ok {
		return 0, fmt.Errorf("%w: Metadata has no leader for %s/%d", ErrProtocol, tp.topic, tp.partition)
	}
	return l.epoch, nil
}

func (c *Client) learn(m *wire.MetadataOk) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, n := range m.Nodes {
		c.nodes[n.NodeID] = net.JoinHostPort(n.Host, strconv.Itoa(int(n.Port)))
	}
	for _, t := range m.Topics {
		if t.Error != 0 {
			continue
		}
		c.counts[t.Name] = uint32(len(t.Partitions))
		for _, p := range t.Partitions {
			c.leaders[topicPartition{t.Name, p.Partition}] = leader{p.LeaderNodeID, p.LeaderEpoch}
		}
	}
}

// Metadata asks for the given topics, or every topic if none is given,
// and updates the client's view of nodes and leaders. A topic that does
// not exist has Error straumerr.UnknownTopic in the result.
func (c *Client) Metadata(ctx context.Context, topics ...string) (*Metadata, error) {
	resp, err := c.do(ctx, nil, func(uint32) wire.Request { return &wire.Metadata{Topics: topics} })
	if err != nil {
		return nil, err
	}
	m, ok := resp.(*wire.MetadataOk)
	if !ok {
		return nil, fmt.Errorf("%w: Metadata answered with %T", ErrProtocol, resp)
	}
	c.learn(m)
	return newMetadata(m), nil
}

// PartitionCount returns the topic's number of partitions, from Metadata
// the first time.
func (c *Client) PartitionCount(ctx context.Context, topic string) (uint32, error) {
	c.mu.Lock()
	n, ok := c.counts[topic]
	c.mu.Unlock()
	if ok {
		return n, nil
	}
	m, err := c.Metadata(ctx, topic)
	if err != nil {
		return 0, err
	}
	for _, t := range m.Topics {
		if t.Name == topic {
			if t.Error != 0 {
				return 0, &straumerr.Error{Code: t.Error, Message: topic}
			}
			return uint32(len(t.Partitions)), nil
		}
	}
	return 0, fmt.Errorf("%w: Metadata did not list %q", ErrProtocol, topic)
}
