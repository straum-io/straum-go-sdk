package producer_test

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/straum-io/straum-go-sdk/client"
	"github.com/straum-io/straum-go-sdk/producer"
)

// Publishes a keyed message with a header to a broker on 127.0.0.1:7171.
func Example() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := client.New(ctx, client.Config{Addrs: []string{"127.0.0.1:7171"}})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	p := producer.New(c, producer.Options{})
	ack, err := p.Produce(ctx, "orders", client.Message{
		Key:     []byte("customer-42"),
		Headers: []client.Header{{Name: []byte("source"), Value: []byte("web")}},
		Body:    []byte(`{"order": 1001}`),
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("partition %d, seq %d\n", ack.Partition, ack.BaseSeq)
}

// Publishes three messages to partition 0 in one request; they get
// consecutive seqs.
func ExampleProducer_ProduceTo() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := client.New(ctx, client.Config{Addrs: []string{"127.0.0.1:7171"}})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	p := producer.New(c, producer.Options{})
	ack, err := p.ProduceTo(ctx, "orders", 0,
		client.Message{Body: []byte(`{"order": 1002}`)},
		client.Message{Body: []byte(`{"order": 1003}`)},
		client.Message{Body: []byte(`{"order": 1004}`)},
	)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("seqs %d to %d\n", ack.BaseSeq, ack.BaseSeq+uint64(ack.Count)-1)
}
