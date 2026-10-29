package consumer_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/straum-io/straum-go-sdk/client"
	"github.com/straum-io/straum-go-sdk/consumer"
)

// Reads partition 0 of "orders" from the beginning for a minute, from a
// broker on 127.0.0.1:7171. The position to resume from is
// it.Position(); the broker keeps no offsets.
func Example() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	c, err := client.New(ctx, client.Config{Addrs: []string{"127.0.0.1:7171"}})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	it := consumer.New(c, consumer.Options{}).Iterator("orders", 0, 0)
	for {
		m, err := it.Next(ctx)
		if errors.Is(err, context.DeadlineExceeded) {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("seq %d key %q body %q\n", m.Seq, m.Key, m.Body)
	}
	fmt.Println("resume from", it.Position())
}

// Fetches partition 0 of "orders" batch by batch until it is caught up,
// from a broker on 127.0.0.1:7171.
func ExampleConsumer_Fetch() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := client.New(ctx, client.Config{Addrs: []string{"127.0.0.1:7171"}})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	cons := consumer.New(c, consumer.Options{MaxBytes: 64 << 10, MaxWait: -1})
	var next uint64
	for {
		b, err := cons.Fetch(ctx, "orders", 0, next)
		if err != nil {
			log.Fatal(err)
		}
		for _, m := range b.Messages {
			fmt.Printf("seq %d at %s: %s\n", m.Seq, m.Timestamp.Format(time.RFC3339), m.Body)
			next = m.Seq + 1
		}
		if next < b.StartSeq {
			next = b.StartSeq
		}
		if next >= b.NextSeq {
			break
		}
	}
}
