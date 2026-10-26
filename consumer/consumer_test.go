package consumer_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/straum-io/straum-go-sdk/client"
	"github.com/straum-io/straum-go-sdk/consumer"
	"github.com/straum-io/straum-go-sdk/internal/fakebroker"
	"github.com/straum-io/straum-go-sdk/internal/wire"
	"github.com/straum-io/straum-go-sdk/straumerr"
)

// A partition of "t" holding seqs start … 9; each Fetch returns at most
// three messages and records its request.
func broker(t *testing.T, start uint64, fetches chan<- *wire.Fetch) *client.Client {
	b := fakebroker.Start(t, func(c *fakebroker.Conn, corr uint64, req wire.Request) {
		f := req.(*wire.Fetch)
		if fetches != nil {
			fetches <- f
		}
		if f.Topic != "t" {
			c.Reply(corr, &wire.Error{Code: straumerr.UnknownTopic})
			return
		}
		resp := &wire.FetchOk{LeaderEpoch: 1, StartSeq: start, NextSeq: 10}
		for s := max(f.FromSeq, start); s < 10 && len(resp.Messages) < 3; s++ {
			resp.Messages = append(resp.Messages, wire.FetchedMessage{Seq: s, TimestampMs: 1000 + s,
				Message: wire.Message{Key: []byte{}, Headers: []wire.Header{{Name: []byte("h"), Value: []byte("v")}}, Body: []byte(fmt.Sprint(s))}})
		}
		if len(resp.Messages) == 0 {
			time.Sleep(time.Duration(f.MaxWaitMs) * time.Millisecond)
		}
		c.Reply(corr, resp)
	})
	c, err := client.New(context.Background(), client.Config{Addrs: []string{b.Addr()}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestFetchSendsOptions(t *testing.T) {
	fetches := make(chan *wire.Fetch, 1)
	c := broker(t, 0, fetches)
	cons := consumer.New(c, consumer.Options{MaxBytes: 4096, MaxWait: 20 * time.Millisecond})
	f, err := cons.Fetch(context.Background(), "t", 0, 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(f.Messages) != 2 || f.Messages[0].Seq != 8 || f.NextSeq != 10 || f.LeaderEpoch != 1 {
		t.Fatalf("%+v", f)
	}
	if m := f.Messages[0]; m.Timestamp.UnixMilli() != 1008 || m.Key == nil || len(m.Key) != 0 ||
		len(m.Headers) != 1 || string(m.Headers[0].Name) != "h" || string(m.Headers[0].Value) != "v" || string(m.Body) != "8" {
		t.Fatalf("message %+v", m)
	}
	if r := <-fetches; r.MaxBytes != 4096 || r.MaxWaitMs != 20 || r.FromSeq != 8 || r.ExpectedLeaderEpoch != 0 {
		t.Fatalf("request %+v", r)
	}
	if _, err := cons.Fetch(context.Background(), "missing", 0, 0); !errors.Is(err, straumerr.UnknownTopic) {
		t.Fatalf("got %v", err)
	}
}

func TestIteratorKeepsItsPosition(t *testing.T) {
	c := broker(t, 0, nil)
	it := consumer.New(c, consumer.Options{MaxWait: 10 * time.Millisecond}).Iterator("t", 0, 0)
	for want := uint64(0); want < 10; want++ {
		m, err := it.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if m.Seq != want || string(m.Body) != fmt.Sprint(want) || it.Position() != want+1 {
			t.Fatalf("got seq %d body %q position %d, want %d", m.Seq, m.Body, it.Position(), want)
		}
	}
	// Nothing more: Next waits until the context ends, position unchanged.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := it.Next(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v", err)
	}
	if it.Position() != 10 {
		t.Fatalf("position %d", it.Position())
	}
}

func TestIteratorSkipsReleasedMessages(t *testing.T) {
	c := broker(t, 7, nil)
	it := consumer.New(c, consumer.Options{}).Iterator("t", 0, 2)
	m, err := it.Next(context.Background())
	if err != nil || m.Seq != 7 {
		t.Fatalf("seq %d, %v", m.Seq, err)
	}
}
