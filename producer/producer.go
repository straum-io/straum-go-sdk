// Package producer publishes messages to Straum topics.
//
// A Producer has a stable producer id and gives each Produce request its
// own producer seq; a retry of the request (docs/protocol.md section 6)
// keeps both. Protocol version 0 does not check them, so a request retried
// after a lost connection may store its messages twice (section 8).
//
// Produce picks the partition with the normative default partitioner
// (crc32c of the key modulo the partition count, section 7) and sends
// keyless messages to partitions in turn. ProduceTo takes the partition
// from the caller.
package producer

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/straum-io/straum-go-sdk/client"
	"github.com/straum-io/straum-go-sdk/wire"
)

// Producer sends Produce requests through a client. It is safe for
// concurrent use.
type Producer struct {
	c          *client.Client
	id         uint64
	seq        atomic.Uint64
	roundRobin atomic.Uint32
}

// Options configure a Producer.
type Options struct {
	// ProducerID identifies the producer to the broker. 0 picks a random
	// id; set it to keep one id across restarts.
	ProducerID uint64
}

// New returns a producer that sends through c.
func New(c *client.Client, opts Options) *Producer {
	id := opts.ProducerID
	for id == 0 {
		var b [8]byte
		_, _ = rand.Read(b[:]) // never fails (crypto/rand)
		id = binary.LittleEndian.Uint64(b[:])
	}
	return &Producer{c: c, id: id}
}

// ID is the producer id sent with every request.
func (p *Producer) ID() uint64 { return p.id }

// Ack is the broker's acknowledgement: the messages of the request have
// seqs BaseSeq … BaseSeq+Count−1 in Partition, are on the leader's disk
// (ack level durable), and the first one has Timestamp.
type Ack struct {
	Partition   uint32
	BaseSeq     uint64
	Count       uint32
	LeaderEpoch uint32
	Timestamp   time.Time
}

// Produce sends one message; the default partitioner picks its partition
// from its key.
func (p *Producer) Produce(ctx context.Context, topic string, m wire.Message) (Ack, error) {
	n, err := p.c.PartitionCount(ctx, topic)
	if err != nil {
		return Ack{}, err
	}
	var partition uint32
	if m.Key != nil {
		partition = wire.Partition(m.Key, n)
	} else if n > 0 {
		partition = p.roundRobin.Add(1) % n
	}
	return p.ProduceTo(ctx, topic, partition, m)
}

// ProduceTo sends messages to one partition in one request. They get
// consecutive seqs, in order.
func (p *Producer) ProduceTo(ctx context.Context, topic string, partition uint32, messages ...wire.Message) (Ack, error) {
	if len(messages) == 0 {
		return Ack{}, fmt.Errorf("producer: no messages")
	}
	seq := p.seq.Add(1) - 1
	resp, err := p.c.DoPartition(ctx, topic, partition, func(epoch uint32) wire.Request {
		return &wire.Produce{
			Topic:               topic,
			Partition:           partition,
			ProducerID:          p.id,
			ProducerSeq:         seq,
			AckLevel:            wire.AckDurable,
			ExpectedLeaderEpoch: epoch,
			Messages:            messages,
		}
	})
	if err != nil {
		return Ack{}, err
	}
	ok, isOk := resp.(*wire.ProduceOk)
	if !isOk {
		return Ack{}, fmt.Errorf("%w: Produce answered with %T", client.ErrProtocol, resp)
	}
	return Ack{
		Partition:   partition,
		BaseSeq:     ok.BaseSeq,
		Count:       ok.Count,
		LeaderEpoch: ok.LeaderEpoch,
		Timestamp:   time.UnixMilli(int64(ok.TimestampMs)),
	}, nil
}
