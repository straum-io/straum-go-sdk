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
	"github.com/straum-io/straum-go-sdk/wire"
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
	resp, err := a.c.Do(ctx, req)
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
func (a *Admin) Metadata(ctx context.Context, topics ...string) (*wire.MetadataOk, error) {
	return a.c.Metadata(ctx, topics...)
}

// ListTopics returns every topic by name.
func (a *Admin) ListTopics(ctx context.Context) ([]wire.TopicSummary, error) {
	r, err := do[*wire.ListTopicsOk](ctx, a, &wire.ListTopics{})
	if err != nil {
		return nil, err
	}
	return r.Topics, nil
}

// DescribeTopic returns a topic's configuration and partitions.
func (a *Admin) DescribeTopic(ctx context.Context, name string) (*wire.DescribeTopicOk, error) {
	return do[*wire.DescribeTopicOk](ctx, a, &wire.DescribeTopic{Name: name})
}

// BrokerStats returns the counters of the broker the client is connected
// to.
func (a *Admin) BrokerStats(ctx context.Context) (*wire.BrokerStatsOk, error) {
	return do[*wire.BrokerStatsOk](ctx, a, &wire.BrokerStats{})
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
