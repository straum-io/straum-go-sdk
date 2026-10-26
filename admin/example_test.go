package admin_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/straum-io/straum-go-sdk/admin"
	"github.com/straum-io/straum-go-sdk/client"
	"github.com/straum-io/straum-go-sdk/straumerr"
)

// Creates a topic unless it exists and prints the broker's counters, on a
// broker on 127.0.0.1:7171.
func Example() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := client.New(ctx, client.Config{Addrs: []string{"127.0.0.1:7171"}})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	a := admin.New(c)
	err = a.CreateTopic(ctx, "orders", admin.TopicConfig{Retention: 7 * 24 * time.Hour})
	if err != nil && !errors.Is(err, straumerr.TopicExists) {
		log.Fatal(err)
	}
	d, err := a.DescribeTopic(ctx, "orders")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s: %d messages\n", d.Name, d.Partitions[0].MessageCount)
	s, err := a.BrokerStats(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("accepted %d, deduplicated %d\n", s.MessagesAccepted, s.MessagesDeduplicated)
}
