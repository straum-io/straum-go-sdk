package producer_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/straum-io/straum-go-sdk/client"
	"github.com/straum-io/straum-go-sdk/internal/fakebroker"
	"github.com/straum-io/straum-go-sdk/producer"
	"github.com/straum-io/straum-go-sdk/straumerr"
	"github.com/straum-io/straum-go-sdk/wire"
)

// broker serves a topic "t" with four partitions and records every
// Produce.
func broker(t *testing.T) (*client.Client, func() []*wire.Produce) {
	var mu sync.Mutex
	var got []*wire.Produce
	b := fakebroker.Start(t, func(c *fakebroker.Conn, corr uint64, req wire.Request) {
		switch r := req.(type) {
		case *wire.Metadata:
			resp := &wire.MetadataOk{Nodes: []wire.Node{{NodeID: 1}}}
			for _, name := range r.Topics {
				if name == "t" {
					var parts []wire.PartitionMetadata
					for i := range uint32(4) {
						parts = append(parts, wire.PartitionMetadata{Partition: i, LeaderNodeID: 1, LeaderEpoch: 1})
					}
					resp.Topics = append(resp.Topics, wire.TopicMetadata{Name: name, Partitions: parts})
				} else {
					resp.Topics = append(resp.Topics, wire.TopicMetadata{Name: name, Error: straumerr.UnknownTopic})
				}
			}
			c.Reply(corr, resp)
		case *wire.Produce:
			mu.Lock()
			got = append(got, r)
			n := uint64(len(got))
			mu.Unlock()
			c.Reply(corr, &wire.ProduceOk{BaseSeq: n * 10, Count: uint32(len(r.Messages)), LeaderEpoch: 1, TimestampMs: 1761350400000})
		}
	})
	c, err := client.New(context.Background(), client.Config{Addrs: []string{b.Addr()}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c, func() []*wire.Produce {
		mu.Lock()
		defer mu.Unlock()
		return append([]*wire.Produce(nil), got...)
	}
}

func TestProduceUsesTheDefaultPartitioner(t *testing.T) {
	c, sent := broker(t)
	p := producer.New(c, producer.Options{ProducerID: 77})
	ctx := context.Background()
	keys := [][]byte{[]byte("user-1"), []byte("user-2"), {}, []byte("user-1")}
	for _, k := range keys {
		ack, err := p.Produce(ctx, "t", wire.Message{Key: k, Body: []byte("x")})
		if err != nil {
			t.Fatal(err)
		}
		if want := wire.Partition(k, 4); ack.Partition != want {
			t.Errorf("key %q: partition %d, want %d", k, ack.Partition, want)
		}
	}
	reqs := sent()
	for i, r := range reqs {
		if r.ProducerID != 77 || r.ProducerSeq != uint64(i) || r.AckLevel != wire.AckDurable || r.ExpectedLeaderEpoch != 0 {
			t.Errorf("request %d: %+v", i, r)
		}
		if r.Partition != wire.Partition(keys[i], 4) {
			t.Errorf("request %d went to partition %d", i, r.Partition)
		}
	}
	if ack, _ := p.Produce(ctx, "t", wire.Message{Body: []byte("x")}); ack.Timestamp.UnixMilli() != 1761350400000 {
		t.Errorf("timestamp %v", ack.Timestamp)
	}
}

func TestKeylessMessagesRoundRobin(t *testing.T) {
	c, _ := broker(t)
	p := producer.New(c, producer.Options{})
	if p.ID() == 0 {
		t.Fatal("producer id 0")
	}
	seen := map[uint32]bool{}
	for range 4 {
		ack, err := p.Produce(context.Background(), "t", wire.Message{Body: []byte("x")})
		if err != nil {
			t.Fatal(err)
		}
		seen[ack.Partition] = true
	}
	if len(seen) != 4 {
		t.Errorf("four keyless messages reached partitions %v", seen)
	}
}

func TestProduceToSendsOneRequest(t *testing.T) {
	c, sent := broker(t)
	p := producer.New(c, producer.Options{})
	ack, err := p.ProduceTo(context.Background(), "t", 2,
		wire.Message{Body: []byte("a")}, wire.Message{Key: []byte("k"), Headers: []wire.Header{{Name: []byte("h"), Value: []byte("v")}}, Body: []byte("b")})
	if err != nil || ack.Count != 2 || ack.Partition != 2 {
		t.Fatalf("%+v %v", ack, err)
	}
	if r := sent(); len(r) != 1 || len(r[0].Messages) != 2 || r[0].Partition != 2 {
		t.Fatalf("sent %+v", r)
	}
	if _, err := p.ProduceTo(context.Background(), "t", 0); err == nil {
		t.Fatal("empty request accepted")
	}
}

func TestProduceToUnknownTopic(t *testing.T) {
	c, _ := broker(t)
	p := producer.New(c, producer.Options{})
	if _, err := p.Produce(context.Background(), "missing", wire.Message{}); !errors.Is(err, straumerr.UnknownTopic) {
		t.Fatalf("got %v", err)
	}
}
