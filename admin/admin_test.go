package admin_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/straum-io/straum-go-sdk/admin"
	"github.com/straum-io/straum-go-sdk/client"
	"github.com/straum-io/straum-go-sdk/internal/fakebroker"
	"github.com/straum-io/straum-go-sdk/internal/wire"
	"github.com/straum-io/straum-go-sdk/straumerr"
)

func TestAdminRequests(t *testing.T) {
	var created atomic.Pointer[wire.CreateTopic]
	b := fakebroker.Start(t, func(c *fakebroker.Conn, corr uint64, req wire.Request) {
		switch r := req.(type) {
		case *wire.CreateTopic:
			if r.Name == "dup" {
				c.Reply(corr, &wire.Error{Code: straumerr.TopicExists})
				return
			}
			created.Store(r)
			c.Reply(corr, &wire.CreateTopicOk{})
		case *wire.Metadata:
			c.Reply(corr, &wire.MetadataOk{ClusterID: "fake", Topics: []wire.TopicMetadata{{Name: "gone", Error: straumerr.UnknownTopic}}})
		case *wire.ListTopics:
			// The broker echoes the request: one topic named after the
			// cursor and limit, and a next cursor unless the cursor is "z".
			next := "z"
			if r.Cursor == "z" {
				next = ""
			}
			c.Reply(corr, &wire.ListTopicsOk{Topics: []wire.TopicSummary{{Name: fmt.Sprintf("%s/%d", r.Cursor, r.Limit), Partitions: 1}}, NextCursor: next})
		case *wire.DescribeTopic:
			c.Reply(corr, &wire.DescribeTopicOk{Name: r.Name, ReplicationFactor: 1,
				Partitions: []wire.PartitionDescription{{NextSeq: 5, MessageCount: 5}}})
		case *wire.BrokerStats:
			c.Reply(corr, &wire.BrokerStatsOk{NodeID: 1, MessagesAccepted: 5})
		case *wire.Ping:
			c.Reply(corr, &wire.Pong{Nonce: r.Nonce})
		}
	})
	c, err := client.New(context.Background(), client.Config{Addrs: []string{b.Addr()}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	a := admin.New(c)
	ctx := context.Background()

	if err := a.CreateTopic(ctx, "orders", admin.TopicConfig{Retention: time.Hour}); err != nil {
		t.Fatal(err)
	}
	if r := created.Load(); r.Partitions != 1 || r.ReplicationFactor != 1 || r.RetentionMs != 3600000 {
		t.Errorf("CreateTopic sent %+v", r)
	}
	if err := a.CreateTopic(ctx, "dup", admin.TopicConfig{}); !errors.Is(err, straumerr.TopicExists) {
		t.Errorf("duplicate: %v", err)
	}
	if m, err := a.Metadata(ctx, "gone"); err != nil || m.Topics[0].Error != straumerr.UnknownTopic {
		t.Errorf("metadata %+v %v", m, err)
	}
	if l, next, err := a.ListTopics(ctx, "", 0); err != nil || len(l) != 1 || l[0].Name != "/1000" || next != "z" {
		t.Errorf("list %+v %q %v", l, next, err)
	}
	if l, next, err := a.ListTopics(ctx, "z", 5000); err != nil || len(l) != 1 || l[0].Name != "z/1000" || next != "" {
		t.Errorf("list %+v %q %v", l, next, err)
	}
	if l, _, err := a.ListTopics(ctx, "m", 7); err != nil || l[0].Name != "m/7" {
		t.Errorf("list %+v %v", l, err)
	}
	if d, err := a.DescribeTopic(ctx, "orders"); err != nil || d.Name != "orders" || d.Partitions[0].MessageCount != 5 {
		t.Errorf("describe %+v %v", d, err)
	}
	if s, err := a.BrokerStats(ctx); err != nil || s.MessagesAccepted != 5 {
		t.Errorf("stats %+v %v", s, err)
	}
	if rtt, err := a.Ping(ctx); err != nil || rtt <= 0 {
		t.Errorf("ping %v %v", rtt, err)
	}
}

func TestPingNonceMismatch(t *testing.T) {
	b := fakebroker.Start(t, func(c *fakebroker.Conn, corr uint64, req wire.Request) {
		c.Reply(corr, &wire.Pong{Nonce: 1})
	})
	c, err := client.New(context.Background(), client.Config{Addrs: []string{b.Addr()}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := admin.New(c).Ping(context.Background()); !errors.Is(err, client.ErrProtocol) {
		t.Fatalf("got %v", err)
	}
}
