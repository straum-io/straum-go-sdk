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
