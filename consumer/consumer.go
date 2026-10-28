// Package consumer reads messages from Straum partitions.
//
// Protocol version 0 stores no consumer offsets: the caller keeps its
// position, the seq of the next message to read. Fetch reads from a given
// seq; Iterator keeps the position itself and long-polls for new
// messages.
package consumer

import (
	"context"
	"fmt"
	"time"

	"github.com/straum-io/straum-go-sdk/client"
	"github.com/straum-io/straum-go-sdk/internal/link"
	"github.com/straum-io/straum-go-sdk/internal/wire"
)

// Options configure fetches.
type Options struct {
	// MaxBytes bounds the encoded size of the messages in one response:
	// per message its seq, timestamp, flags, key and headers with their
	// lengths, and body with its length (default 1 MiB); the broker caps it
	// at its max_fetch_bytes and also returns at most its
	// max_fetch_messages messages. The first message is returned even if it
	// alone is larger.
	MaxBytes uint32
	// MaxWait is how long the broker waits for a message when none is
	// available (default 500 ms; a negative value: no wait); the broker
	// caps it at its max_fetch_wait_ms.
	MaxWait time.Duration
}

func (o Options) withDefaults() Options {
	if o.MaxBytes == 0 {
		o.MaxBytes = 1 << 20
	}
	if o.MaxWait == 0 {
		o.MaxWait = 500 * time.Millisecond
	}
	return o
}

// Consumer fetches through a client. It is safe for concurrent use.
type Consumer struct {
	c    *client.Client
	opts Options
}

// New returns a consumer that fetches through c.
func New(c *client.Client, opts Options) *Consumer {
	return &Consumer{c: c, opts: opts.withDefaults()}
}

// Message is a fetched message: its seq, the broker's time for it, and
// its key, headers and body.
type Message struct {
	Seq       uint64
	Timestamp time.Time
	client.Message
}

// Batch is one Fetch response.
type Batch struct {
	LeaderEpoch uint32
	StartSeq    uint64 // first seq still readable
	NextSeq     uint64 // seq of the next message the partition will store
	Messages    []Message
}

// Fetch returns the messages of a partition from seq fromSeq on, in seq
// order. If none is available it waits up to MaxWait and may return none.
// A fromSeq below the partition's StartSeq skips released messages. The
// next position is the last message's Seq + 1.
func (c *Consumer) Fetch(ctx context.Context, topic string, partition uint32, fromSeq uint64) (*Batch, error) {
	maxWait := min(max(c.opts.MaxWait.Milliseconds(), 0), 1<<32-1)
	resp, err := link.DoPartition(c.c, ctx, topic, partition, func(epoch uint32) wire.Request {
		return &wire.Fetch{
			Topic:               topic,
			Partition:           partition,
			FromSeq:             fromSeq,
			MaxBytes:            c.opts.MaxBytes,
			MaxWaitMs:           uint32(maxWait),
			ExpectedLeaderEpoch: epoch,
		}
	})
	if err != nil {
		return nil, err
	}
	f, ok := resp.(*wire.FetchOk)
	if !ok {
		return nil, fmt.Errorf("%w: Fetch answered with %T", client.ErrProtocol, resp)
	}
	b := &Batch{LeaderEpoch: f.LeaderEpoch, StartSeq: f.StartSeq, NextSeq: f.NextSeq, Messages: make([]Message, len(f.Messages))}
	for i, m := range f.Messages {
		b.Messages[i] = Message{Seq: m.Seq, Timestamp: time.UnixMilli(int64(m.TimestampMs)),
			Message: client.Message{Key: m.Key, Body: m.Body}}
		if len(m.Headers) > 0 {
			hs := make([]client.Header, len(m.Headers))
			for j, h := range m.Headers {
				hs[j] = client.Header(h)
			}
			b.Messages[i].Headers = hs
		}
	}
	return b, nil
}

// Iterator reads one partition message by message from a position it
// keeps. It is not safe for concurrent use.
type Iterator struct {
	c         *Consumer
	topic     string
	partition uint32
	next      uint64
	buf       []Message
}

// Iterator returns an iterator over a partition starting at seq fromSeq.
func (c *Consumer) Iterator(topic string, partition uint32, fromSeq uint64) *Iterator {
	return &Iterator{c: c, topic: topic, partition: partition, next: fromSeq}
}

// Next returns the next message, fetching (and waiting for new messages)
// as needed until ctx ends. After an error the position is unchanged and
// Next may be called again.
func (it *Iterator) Next(ctx context.Context) (Message, error) {
	for len(it.buf) == 0 {
		f, err := it.c.Fetch(ctx, it.topic, it.partition, it.next)
		if err != nil {
			return Message{}, err
		}
		it.buf = f.Messages
		if len(it.buf) == 0 && f.StartSeq > it.next {
			it.next = f.StartSeq // the messages before StartSeq were released
		}
	}
	m := it.buf[0]
	it.buf = it.buf[1:]
	it.next = m.Seq + 1
	return m, nil
}

// Position is the seq of the next message Next returns.
func (it *Iterator) Position() uint64 { return it.next }
