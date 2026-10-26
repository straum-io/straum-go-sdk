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
	"github.com/straum-io/straum-go-sdk/wire"
)

// Options configure fetches.
type Options struct {
	// MaxBytes bounds the key, header and body bytes of one response
	// (default 1 MiB); the broker caps it at its max_fetch_bytes. The first
	// message is returned even if it alone is larger.
	MaxBytes uint32
	// MaxWait is how long the broker waits for a message when none is
	// available (default 500 ms); the broker caps it at its
	// max_fetch_wait_ms.
	MaxWait time.Duration
}

func (o Options) withDefaults() Options {
	if o.MaxBytes == 0 {
		o.MaxBytes = 1 << 20
	}
	if o.MaxWait <= 0 {
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

// Fetch returns the messages of a partition from seq fromSeq on, in seq
// order. If none is available it waits up to MaxWait and may return none.
// A fromSeq below the partition's StartSeq skips released messages. The
// next position is the last message's Seq + 1.
func (c *Consumer) Fetch(ctx context.Context, topic string, partition uint32, fromSeq uint64) (*wire.FetchOk, error) {
	maxWait := min(c.opts.MaxWait.Milliseconds(), 1<<32-1)
	resp, err := c.c.DoPartition(ctx, topic, partition, func(epoch uint32) wire.Request {
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
	return f, nil
}

// Iterator reads one partition message by message from a position it
// keeps. It is not safe for concurrent use.
type Iterator struct {
	c         *Consumer
	topic     string
	partition uint32
	next      uint64
	buf       []wire.FetchedMessage
}

// Iterator returns an iterator over a partition starting at seq fromSeq.
func (c *Consumer) Iterator(topic string, partition uint32, fromSeq uint64) *Iterator {
	return &Iterator{c: c, topic: topic, partition: partition, next: fromSeq}
}

// Next returns the next message, fetching (and waiting for new messages)
// as needed until ctx ends. After an error the position is unchanged and
// Next may be called again.
func (it *Iterator) Next(ctx context.Context) (wire.FetchedMessage, error) {
	for len(it.buf) == 0 {
		f, err := it.c.Fetch(ctx, it.topic, it.partition, it.next)
		if err != nil {
			return wire.FetchedMessage{}, err
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
