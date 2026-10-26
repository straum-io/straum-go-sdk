// Package admin holds the administrative requests of protocol version 0:
// CreateTopic, Metadata, ListTopics, DescribeTopic, BrokerStats and Ping.
// There is no separate administrative API; these go over the same
// connections as produce and fetch.
package admin

import (
	"context"
	"fmt"
	"time"

	"github.com/straum-io/straum-go-sdk/client"
	"github.com/straum-io/straum-go-sdk/internal/link"
	"github.com/straum-io/straum-go-sdk/internal/wire"
)

// Admin sends administrative requests through a client. It is safe for
// concurrent use.
type Admin struct{ c *client.Client }

// New returns an Admin that sends through c.
func New(c *client.Client) *Admin { return &Admin{c: c} }

// TopicConfig is the configuration of a new topic.
type TopicConfig struct {
	// Partitions defaults to 1, the only count version 0 brokers accept.
	Partitions uint32
	// ReplicationFactor defaults to 1, the only factor version 0 accepts.
	ReplicationFactor uint16
	// Retention is recorded by the broker; 0 keeps everything. Version 0
	// brokers do not release messages by age.
	Retention time.Duration
}

func do[T wire.Response](ctx context.Context, a *Admin, req wire.Request) (T, error) {
	var zero T
	resp, err := link.Do(a.c, ctx, req)
	if err != nil {
		return zero, err
	}
	r, ok := resp.(T)
	if !ok {
		return zero, fmt.Errorf("%w: answered with %T", client.ErrProtocol, resp)
	}
	return r, nil
}

// CreateTopic creates a topic. An existing name gives
// straumerr.TopicExists; a name starting with "__" gives
// straumerr.Forbidden.
func (a *Admin) CreateTopic(ctx context.Context, name string, cfg TopicConfig) error {
	if cfg.Partitions == 0 {
		cfg.Partitions = 1
	}
	if cfg.ReplicationFactor == 0 {
		cfg.ReplicationFactor = 1
	}
	_, err := do[*wire.CreateTopicOk](ctx, a, &wire.CreateTopic{
		Name:              name,
		Partitions:        cfg.Partitions,
		ReplicationFactor: cfg.ReplicationFactor,
		RetentionMs:       uint64(cfg.Retention.Milliseconds()),
	})
	return err
}

// Metadata returns the nodes and the given topics' partitions, or every
// topic if none is given. A missing topic is listed with Error
// straumerr.UnknownTopic.
func (a *Admin) Metadata(ctx context.Context, topics ...string) (*client.Metadata, error) {
	return a.c.Metadata(ctx, topics...)
}

// TopicSummary is a topic's name and partition count.
type TopicSummary struct {
	Name       string
	Partitions uint32
}

// ListTopics returns every topic by name.
func (a *Admin) ListTopics(ctx context.Context) ([]TopicSummary, error) {
	r, err := do[*wire.ListTopicsOk](ctx, a, &wire.ListTopics{})
	if err != nil {
		return nil, err
	}
	out := make([]TopicSummary, len(r.Topics))
	for i, t := range r.Topics {
		out[i] = TopicSummary(t)
	}
	return out, nil
}

// PartitionDescription is a partition's seq range and storage.
type PartitionDescription struct {
	Partition    uint32
	StartSeq     uint64
	NextSeq      uint64
	MessageCount uint64
	IndexBytes   uint64
}

// TopicDescription is a topic's configuration and partitions.
type TopicDescription struct {
	Name              string
	ReplicationFactor uint16
	Retention         time.Duration // 0: everything is kept
	Partitions        []PartitionDescription
}

// DescribeTopic returns a topic's configuration and partitions.
func (a *Admin) DescribeTopic(ctx context.Context, name string) (*TopicDescription, error) {
	r, err := do[*wire.DescribeTopicOk](ctx, a, &wire.DescribeTopic{Name: name})
	if err != nil {
		return nil, err
	}
	d := &TopicDescription{Name: r.Name, ReplicationFactor: r.ReplicationFactor,
		Retention: time.Duration(r.RetentionMs) * time.Millisecond, Partitions: make([]PartitionDescription, len(r.Partitions))}
	for i, p := range r.Partitions {
		d.Partitions[i] = PartitionDescription(p)
	}
	return d, nil
}

// BrokerStats are the counters of one broker (docs/protocol.md, BrokerStats).
type BrokerStats struct {
	NodeID               uint32
	OpenConnections      uint32
	PendingBytes         uint64
	PendingBytesPeak     uint64
	MaxPendingBytes      uint64
	FetchWaiters         uint32
	ProduceRequests      uint64
	GroupsCommitted      uint64
	Fsyncs               uint64
	MessagesAccepted     uint64
	MessagesDeduplicated uint64
	BodyBytesReceived    uint64
	BodyBytesNew         uint64
	WalBytes             uint64
	SegmentBytes         uint64
	IndexBytes           uint64
}

// BrokerStats returns the counters of the broker the client is connected
// to.
func (a *Admin) BrokerStats(ctx context.Context) (*BrokerStats, error) {
	r, err := do[*wire.BrokerStatsOk](ctx, a, &wire.BrokerStats{})
	if err != nil {
		return nil, err
	}
	s := BrokerStats(*r)
	return &s, nil
}

// Ping checks that a broker answers and returns the round-trip time.
func (a *Admin) Ping(ctx context.Context) (time.Duration, error) {
	start := time.Now()
	nonce := uint64(start.UnixNano())
	p, err := do[*wire.Pong](ctx, a, &wire.Ping{Nonce: nonce})
	if err != nil {
		return 0, err
	}
	if p.Nonce != nonce {
		return 0, fmt.Errorf("%w: Pong nonce %d, sent %d", client.ErrProtocol, p.Nonce, nonce)
	}
	return time.Since(start), nil
}
