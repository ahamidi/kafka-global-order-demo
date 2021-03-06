package main

import (
	"log"
	"os"
	"time"

	"github.com/ahamidi/kafka-global-order-demo/config"
	"github.com/ahamidi/kafka-global-order-demo/consumer"
)

// Global Ordering Kafka Demo
//
// This app aims to provide an example implmentation whereby global ordering is
// mainained across partitions.
//
// In order to achieve this a number of trade-offs have been made, in particular
// we've introduced a delay in processing and made the explicit decision to discard
// any messages that are delayed beyond our chosen time window.

const (
	TimeWindow = 10 * time.Second
	Brokers    = "kafka://localhost:9092" // Comma seperated list fo Kafka broker URLs
)

func main() {

	// Create the config
	cfg := config.NewConsumerConfig(Brokers)

	// Create the consumer
	c, err := consumer.New(cfg, "messages", "demo")
	if err != nil {
		log.Fatal(err)
	}

	if os.Getenv("IN_ORDER") != "" {
		tw := TimeWindow
		c.Consume(true, &tw)
	} else {
		c.Consume(false, nil)
	}
}
