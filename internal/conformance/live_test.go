//go:build live

// Live conformance: needs binaries built from the Straum commit in
// CONTRACT (scripts/live.sh builds them and runs these tests).
//
//	STRAUM_BROKER_BIN       target/release/straum-broker
//	STRAUM_FAKE_SERVER_BIN  target/release/straum-fake-server
package conformance_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/straum-io/straum-go-sdk/admin"
	"github.com/straum-io/straum-go-sdk/client"
	"github.com/straum-io/straum-go-sdk/consumer"
	"github.com/straum-io/straum-go-sdk/internal/conformance"
	"github.com/straum-io/straum-go-sdk/producer"
	"github.com/straum-io/straum-go-sdk/straumerr"
	"github.com/straum-io/straum-go-sdk/wire"
)

const suite = "../../testdata/conformance"

func bin(t *testing.T, env string) string {
	t.Helper()
	p := os.Getenv(env)
	if p == "" {
		t.Fatalf("%s is not set (scripts/live.sh sets it)", env)
	}
	return p
}

func scenarioFiles(t *testing.T, dir string) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(suite, dir, "*.json"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no scenarios in %s: %v", dir, err)
	}
	sort.Strings(files)
	return files
}

// Every scenario of conformance/scenarios against a fresh broker.
func TestBrokerScenarios(t *testing.T) {
	broker := bin(t, "STRAUM_BROKER_BIN")
	for _, f := range scenarioFiles(t, "scenarios") {
		t.Run(filepath.Base(f), func(t *testing.T) {
			data, err := os.ReadFile(f)
			if err != nil {
				t.Fatal(err)
			}
			v, err := conformance.Parse(data)
			if err != nil {
				t.Fatal(err)
			}
			if err := conformance.RunBrokerScenario(broker, v.(conformance.Obj)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Every scenario of conformance/client-scenarios: the SDK's produce and
// fetch against Straum's scripted fake server.
func TestClientScenarios(t *testing.T) {
	fake := bin(t, "STRAUM_FAKE_SERVER_BIN")
	for _, f := range scenarioFiles(t, "client-scenarios") {
		t.Run(filepath.Base(f), func(t *testing.T) {
			if err := conformance.RunClientScenario(fake, f); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// The SDK's public API end to end against a live broker.
func TestLiveSDK(t *testing.T) {
	b, err := conformance.StartBroker(bin(t, "STRAUM_BROKER_BIN"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	c, err := client.New(ctx, client.Config{Addrs: []string{b.Addr}, ClientName: "live-test"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	a := admin.New(c)

	if err := a.CreateTopic(ctx, "orders", admin.TopicConfig{Retention: time.Hour}); err != nil {
		t.Fatal(err)
	}
	if err := a.CreateTopic(ctx, "orders", admin.TopicConfig{}); !errors.Is(err, straumerr.TopicExists) {
		t.Fatalf("second create: %v", err)
	}
	if err := a.CreateTopic(ctx, "__internal", admin.TopicConfig{}); !errors.Is(err, straumerr.Forbidden) {
		t.Fatalf("reserved name: %v", err)
	}
	if err := a.CreateTopic(ctx, "wide", admin.TopicConfig{Partitions: 2}); !errors.Is(err, straumerr.Unsupported) {
		t.Fatalf("two partitions: %v", err)
	}

	// Many goroutines produce on one client at once (pipelined requests).
	p := producer.New(c, producer.Options{})
	const writers, each = 8, 25
	var wg sync.WaitGroup
	for w := range writers {
		wg.Go(func() {
			for i := range each {
				m := wire.Message{Key: []byte(fmt.Sprintf("k%d", w)), Body: []byte(fmt.Sprintf("w%d-%d", w, i)),
					Headers: []wire.Header{{Name: []byte("writer"), Value: []byte{byte(w)}}}}
				if _, err := p.Produce(ctx, "orders", m); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
	// The same body with and without a key: stored once, fetched twice
	// with each publication's own key.
	ack, err := p.ProduceTo(ctx, "orders", 0, wire.Message{Body: []byte("same")}, wire.Message{Key: []byte{}, Body: []byte("same")})
	if err != nil || ack.Count != 2 || ack.BaseSeq != writers*each {
		t.Fatalf("batch: %+v %v", ack, err)
	}

	it := consumer.New(c, consumer.Options{MaxBytes: 300}).Iterator("orders", 0, 0)
	perWriter := map[byte]int{}
	for seq := uint64(0); seq < writers*each+2; seq++ {
		m, err := it.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if m.Seq != seq || m.TimestampMs == 0 {
			t.Fatalf("message %d: seq %d ts %d", seq, m.Seq, m.TimestampMs)
		}
		if seq < writers*each {
			w := m.Headers[0].Value[0]
			if want := fmt.Sprintf("w%d-%d", w, perWriter[w]); string(m.Body) != want || string(m.Key) != fmt.Sprintf("k%d", w) {
				t.Fatalf("seq %d: key %q body %q, want %q (per-writer order)", seq, m.Key, m.Body, want)
			}
			perWriter[w]++
		}
	}
	last2, err := consumer.New(c, consumer.Options{}).Fetch(ctx, "orders", 0, writers*each)
	if err != nil || len(last2.Messages) != 2 || last2.Messages[0].Key != nil || last2.Messages[1].Key == nil ||
		!bytes.Equal(last2.Messages[0].Body, last2.Messages[1].Body) {
		t.Fatalf("dedup pair: %+v %v", last2, err)
	}

	// A fetch past the end waits about max_wait, then returns nothing.
	start := time.Now()
	f, err := consumer.New(c, consumer.Options{MaxWait: 100 * time.Millisecond}).Fetch(ctx, "orders", 0, 10_000)
	if err != nil || len(f.Messages) != 0 || time.Since(start) < 90*time.Millisecond {
		t.Fatalf("fetch past the end: %+v %v after %v", f, err, time.Since(start))
	}

	if _, err := p.Produce(ctx, "missing", wire.Message{}); !errors.Is(err, straumerr.UnknownTopic) {
		t.Fatalf("unknown topic: %v", err)
	}
	if _, err := p.ProduceTo(ctx, "orders", 3, wire.Message{}); !errors.Is(err, straumerr.UnknownPartition) {
		t.Fatalf("unknown partition: %v", err)
	}
	if _, err := p.ProduceTo(ctx, "orders", 0, wire.Message{Body: make([]byte, 1<<20+1)}); !errors.Is(err, straumerr.MessageTooLarge) {
		t.Fatalf("large body: %v", err)
	}

	topics, err := a.ListTopics(ctx)
	if err != nil || len(topics) != 1 || topics[0].Name != "orders" {
		t.Fatalf("list: %+v %v", topics, err)
	}
	d, err := a.DescribeTopic(ctx, "orders")
	if err != nil || d.RetentionMs != 3600000 || d.Partitions[0].MessageCount != writers*each+2 {
		t.Fatalf("describe: %+v %v", d, err)
	}
	m, err := a.Metadata(ctx, "orders", "missing")
	if err != nil || len(m.Topics) != 2 || m.Topics[0].Partitions[0].NextSeq != writers*each+2 || m.Topics[1].Error != straumerr.UnknownTopic {
		t.Fatalf("metadata: %+v %v", m, err)
	}
	// The store counters include the broker's own records: one per
	// CreateTopic (docs/protocol.md 4.11).
	s, err := a.BrokerStats(ctx)
	if err != nil || s.MessagesAccepted != writers*each+2+1 || s.MessagesDeduplicated != 1 {
		t.Fatalf("stats: %+v %v", s, err)
	}
	if _, err := a.Ping(ctx); err != nil {
		t.Fatal(err)
	}
}
