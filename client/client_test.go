package client_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/straum-io/straum-go-sdk/client"
	"github.com/straum-io/straum-go-sdk/internal/fakebroker"
	"github.com/straum-io/straum-go-sdk/straumerr"
	"github.com/straum-io/straum-go-sdk/wire"
)

func connect(t *testing.T, b *fakebroker.Broker, mod func(*client.Config)) *client.Client {
	t.Helper()
	cfg := client.Config{Addrs: []string{b.Addr()}, Retry: client.RetryPolicy{BackoffMin: time.Millisecond}}
	if mod != nil {
		mod(&cfg)
	}
	c, err := client.New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func pong(c *fakebroker.Conn, corr uint64, req wire.Request) {
	if p, ok := req.(*wire.Ping); ok {
		c.Reply(corr, &wire.Pong{Nonce: p.Nonce})
	}
}

func TestHelloAndPing(t *testing.T) {
	b := fakebroker.Start(t, pong)
	c := connect(t, b, nil)
	h, err := c.Hello(context.Background())
	if err != nil || h.ClusterID != "fake" || h.NodeID != 1 {
		t.Fatalf("hello %+v, %v", h, err)
	}
	resp, err := c.Do(context.Background(), &wire.Ping{Nonce: 9})
	if err != nil || resp.(*wire.Pong).Nonce != 9 {
		t.Fatalf("%v %v", resp, err)
	}
}

// Many goroutines share one connection; the broker answers in reverse
// order of arrival, and each caller gets its own response.
func TestPipeliningOutOfOrder(t *testing.T) {
	const n = 64
	var mu sync.Mutex
	var held []func()
	b := fakebroker.Start(t, func(c *fakebroker.Conn, corr uint64, req wire.Request) {
		p := req.(*wire.Ping)
		mu.Lock()
		held = append(held, func() { c.Reply(corr, &wire.Pong{Nonce: p.Nonce}) })
		if len(held) == n {
			for i := len(held) - 1; i >= 0; i-- {
				held[i]()
			}
		}
		mu.Unlock()
	})
	c := connect(t, b, nil)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			resp, err := c.Do(context.Background(), &wire.Ping{Nonce: uint64(i)})
			if err != nil || resp.(*wire.Pong).Nonce != uint64(i) {
				t.Errorf("ping %d: %v %v", i, resp, err)
			}
		})
	}
	wg.Wait()
	if b.Accepted() != 1 {
		t.Errorf("%d connections, want 1", b.Accepted())
	}
}

func TestErrorIsTyped(t *testing.T) {
	b := fakebroker.Start(t, func(c *fakebroker.Conn, corr uint64, req wire.Request) {
		c.Reply(corr, &wire.Error{Code: straumerr.UnknownTopic, Message: "x"})
	})
	c := connect(t, b, nil)
	_, err := c.Do(context.Background(), &wire.DescribeTopic{Name: "x"})
	var se *straumerr.Error
	if !errors.Is(err, straumerr.UnknownTopic) || !errors.As(err, &se) || se.Message != "x" {
		t.Fatalf("got %v", err)
	}
}

// reconnects, says Hello again and sends the same request.
func TestReconnectRetriesSameRequest(t *testing.T) {
	var seen []uint64
	var mu sync.Mutex
	b := fakebroker.Start(t, func(c *fakebroker.Conn, corr uint64, req wire.Request) {
		p := req.(*wire.Produce)
		mu.Lock()
		seen = append(seen, p.ProducerSeq)
		mu.Unlock()
		if c.Index == 0 {
			_ = c.Close()
			return
		}
		c.Reply(corr, &wire.ProduceOk{BaseSeq: 4, Count: 1})
	})
	c := connect(t, b, nil)
	resp, err := c.DoPartition(context.Background(), "t", 0, func(epoch uint32) wire.Request {
		return &wire.Produce{Topic: "t", ProducerID: 1, ProducerSeq: 42, AckLevel: wire.AckDurable,
			ExpectedLeaderEpoch: epoch, Messages: []wire.Message{{Body: []byte("x")}}}
	})
	if err != nil || resp.(*wire.ProduceOk).BaseSeq != 4 {
		t.Fatalf("%v %v", resp, err)
	}
	if len(seen) != 2 || seen[0] != 42 || seen[1] != 42 || b.Accepted() != 2 {
		t.Fatalf("seqs %v over %d connections", seen, b.Accepted())
	}
}

// new leader (another node) with its epoch.
func TestNotLeaderMovesToLeader(t *testing.T) {
	hello2 := fakebroker.DefaultHello
	hello2.NodeID = 2
	leader := fakebroker.StartHello(t, &hello2, func(c *fakebroker.Conn, corr uint64, req wire.Request) {
		p := req.(*wire.Produce)
		if p.ExpectedLeaderEpoch != 7 {
			c.Reply(corr, &wire.Error{Code: straumerr.FencedLeaderEpoch})
			return
		}
		c.Reply(corr, &wire.ProduceOk{BaseSeq: 1, LeaderEpoch: 7})
	})
	host, port := leader.HostPort()
	first := fakebroker.Start(t, func(c *fakebroker.Conn, corr uint64, req wire.Request) {
		switch req.(type) {
		case *wire.Produce:
			c.Reply(corr, &wire.Error{Code: straumerr.NotLeader, LeaderNodeID: 2, LeaderEpoch: 7})
		case *wire.Metadata:
			c.Reply(corr, &wire.MetadataOk{Nodes: []wire.Node{{NodeID: 2, Host: host, Port: port}},
				Topics: []wire.TopicMetadata{{Name: "t", Partitions: []wire.PartitionMetadata{{LeaderNodeID: 2, LeaderEpoch: 7}}}}})
		}
	})
	c := connect(t, first, nil)
	resp, err := c.DoPartition(context.Background(), "t", 0, func(epoch uint32) wire.Request {
		return &wire.Produce{Topic: "t", ExpectedLeaderEpoch: epoch, AckLevel: wire.AckDurable, Messages: []wire.Message{{}}}
	})
	if err != nil || resp.(*wire.ProduceOk).LeaderEpoch != 7 {
		t.Fatalf("%v %v", resp, err)
	}
	// Leader errors on a request without a partition are returned.
	if _, err := c.Do(context.Background(), &wire.Produce{Topic: "t"}); err == nil {
		t.Fatal("NotLeader without a partition was not returned")
	}
}

func TestContextCancelKeepsConnection(t *testing.T) {
	b := fakebroker.Start(t, func(c *fakebroker.Conn, corr uint64, req wire.Request) {
		if p, ok := req.(*wire.Ping); ok && p.Nonce == 1 {
			return // never answered
		}
		pong(c, corr, req)
	})
	c := connect(t, b, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := c.Do(ctx, &wire.Ping{Nonce: 1}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v", err)
	}
	if _, err := c.Do(context.Background(), &wire.Ping{Nonce: 2}); err != nil {
		t.Fatal(err)
	}
	if b.Accepted() != 1 {
		t.Errorf("%d connections, want 1", b.Accepted())
	}
}

// With FailWhenFull, a request beyond the in-flight bound fails at once;
// without it, it waits until a response frees the bytes.
func TestInFlightBound(t *testing.T) {
	for _, failFast := range []bool{true, false} {
		release := make(chan struct{})
		b := fakebroker.Start(t, func(c *fakebroker.Conn, corr uint64, req wire.Request) {
			go func() {
				<-release
				pong(c, corr, req)
			}()
		})
		// A Ping frame is 22 bytes: one fits in 30, two do not.
		c := connect(t, b, func(cfg *client.Config) { cfg.MaxInFlightBytes = 30; cfg.FailWhenFull = failFast })
		first := make(chan error, 1)
		go func() { _, err := c.Do(context.Background(), &wire.Ping{Nonce: 1}); first <- err }()
		time.Sleep(50 * time.Millisecond)
		second := make(chan error, 1)
		go func() { _, err := c.Do(context.Background(), &wire.Ping{Nonce: 2}); second <- err }()
		if failFast {
			if err := <-second; !errors.Is(err, client.ErrInFlightFull) {
				t.Fatalf("fail fast: got %v", err)
			}
		} else {
			select {
			case err := <-second:
				t.Fatalf("second ping did not wait: %v", err)
			case <-time.After(50 * time.Millisecond):
			}
		}
		close(release)
		if err := <-first; err != nil {
			t.Fatal(err)
		}
		if !failFast {
			if err := <-second; err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestFrameTooLargeIsCheckedLocally(t *testing.T) {
	hello := fakebroker.DefaultHello
	hello.MaxFrameBytes = 100
	b := fakebroker.StartHello(t, &hello, func(c *fakebroker.Conn, corr uint64, req wire.Request) {
		t.Error("the broker received an oversized frame")
	})
	c := connect(t, b, nil)
	_, err := c.Do(context.Background(), &wire.Produce{Topic: "t", Messages: []wire.Message{{Body: make([]byte, 200)}}})
	if !errors.Is(err, straumerr.FrameTooLarge) {
		t.Fatalf("got %v", err)
	}
}

func TestHelloRefused(t *testing.T) {
	b := fakebroker.StartHello(t, &wire.Error{Code: straumerr.UnsupportedVersion, MinVersion: 1, MaxVersion: 2}, nil)
	_, err := client.New(context.Background(), client.Config{Addrs: []string{b.Addr()}})
	var se *straumerr.Error
	if !errors.As(err, &se) || se.Code != straumerr.UnsupportedVersion || se.MaxVersion != 2 {
		t.Fatalf("got %v", err)
	}
}

func TestDialFailure(t *testing.T) {
	_, err := client.New(context.Background(), client.Config{Addrs: []string{"127.0.0.1:1"}})
	if !errors.Is(err, client.ErrConnection) {
		t.Fatalf("got %v", err)
	}
	if _, err := client.New(context.Background(), client.Config{}); err == nil {
		t.Fatal("empty Addrs accepted")
	}
}

func TestWrongResponseType(t *testing.T) {
	b := fakebroker.Start(t, func(c *fakebroker.Conn, corr uint64, req wire.Request) {
		c.Reply(corr, &wire.ListTopicsOk{})
	})
	c := connect(t, b, nil)
	if _, err := c.Do(context.Background(), &wire.Ping{}); !errors.Is(err, client.ErrProtocol) {
		t.Fatalf("got %v", err)
	}
}

func TestOversizedResponseClosesConnection(t *testing.T) {
	b := fakebroker.Start(t, func(c *fakebroker.Conn, corr uint64, req wire.Request) {
		c.Reply(corr, &wire.DescribeTopicOk{Name: string(make([]byte, 2000))})
	})
	c := connect(t, b, func(cfg *client.Config) { cfg.MaxResponseBytes = 1000; cfg.Retry.MaxAttempts = 1 })
	if _, err := c.Do(context.Background(), &wire.DescribeTopic{}); !errors.Is(err, client.ErrConnection) {
		t.Fatalf("got %v", err)
	}
}

func TestPartitionCount(t *testing.T) {
	var metadataCalls atomic.Int32
	b := fakebroker.Start(t, func(c *fakebroker.Conn, corr uint64, req wire.Request) {
		metadataCalls.Add(1)
		m := req.(*wire.Metadata)
		resp := &wire.MetadataOk{}
		for _, name := range m.Topics {
			if name == "known" {
				resp.Topics = append(resp.Topics, wire.TopicMetadata{Name: name, Partitions: make([]wire.PartitionMetadata, 3)})
			} else {
				resp.Topics = append(resp.Topics, wire.TopicMetadata{Name: name, Error: straumerr.UnknownTopic})
			}
		}
		c.Reply(corr, resp)
	})
	c := connect(t, b, nil)
	for range 2 {
		if n, err := c.PartitionCount(context.Background(), "known"); err != nil || n != 3 {
			t.Fatalf("%d %v", n, err)
		}
	}
	if metadataCalls.Load() != 1 {
		t.Errorf("%d Metadata calls, want 1 (cached)", metadataCalls.Load())
	}
	if _, err := c.PartitionCount(context.Background(), "missing"); !errors.Is(err, straumerr.UnknownTopic) {
		t.Fatalf("got %v", err)
	}
}

func TestClosedClient(t *testing.T) {
	b := fakebroker.Start(t, pong)
	c := connect(t, b, nil)
	_ = c.Close()
	if _, err := c.Do(context.Background(), &wire.Ping{}); !errors.Is(err, client.ErrClosed) {
		t.Fatalf("got %v", err)
	}
}
