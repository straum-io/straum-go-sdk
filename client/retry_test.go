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
	"github.com/straum-io/straum-go-sdk/internal/link"
	"github.com/straum-io/straum-go-sdk/internal/wire"
	"github.com/straum-io/straum-go-sdk/straumerr"
)

func TestUnknownCodeIsNotRetried(t *testing.T) {
	var calls atomic.Int32
	b := fakebroker.Start(t, func(c *fakebroker.Conn, corr uint64, req wire.Request) {
		calls.Add(1)
		c.Reply(corr, &wire.Error{Code: 999})
	})
	c := connect(t, b, nil)
	_, err := link.Do(c, context.Background(), &wire.ListTopics{})
	if !errors.Is(err, straumerr.Code(999)) || calls.Load() != 1 {
		t.Fatalf("got %v after %d calls", err, calls.Load())
	}
}

func TestOverloadedBacksOffThenExhausts(t *testing.T) {
	var times []time.Time
	var mu sync.Mutex
	b := fakebroker.Start(t, func(c *fakebroker.Conn, corr uint64, req wire.Request) {
		mu.Lock()
		times = append(times, time.Now())
		mu.Unlock()
		c.Reply(corr, &wire.Error{Code: straumerr.Overloaded, RetryAfterMs: 30})
	})
	c := connect(t, b, func(cfg *client.Config) { cfg.Retry.MaxAttempts = 3 })
	_, err := link.Do(c, context.Background(), &wire.ListTopics{})
	if !errors.Is(err, straumerr.Overloaded) {
		t.Fatalf("got %v", err)
	}
	if len(times) != 3 {
		t.Fatalf("%d attempts, want 3", len(times))
	}
	for i := 1; i < len(times); i++ {
		if d := times[i].Sub(times[i-1]); d < 30*time.Millisecond {
			t.Errorf("attempt %d after %v, want at least 30ms", i+1, d)
		}
	}
}

// The first connection is dropped before the answer; the client
func TestConnectionLostAfterLastAttempt(t *testing.T) {
	b := fakebroker.Start(t, func(c *fakebroker.Conn, corr uint64, req wire.Request) { _ = c.Close() })
	c := connect(t, b, func(cfg *client.Config) { cfg.Retry.MaxAttempts = 2 })
	_, err := link.Do(c, context.Background(), &wire.ListTopics{})
	if !errors.Is(err, client.ErrConnection) {
		t.Fatalf("got %v", err)
	}
}

// NotLeader: Metadata on the same connection, then the retry goes to the
