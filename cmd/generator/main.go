package main

import (
	"log"

	"github.com/ahamidi/kafka-global-order-demo/config"
	"github.com/ahamidi/kafka-global-order-demo/producer"
)

const Brokers = "kafka://localhost:9092"

func main() {
	// Create producer config
	cfg := config.NewProducerConfig(Brokers)

	// Create the producer
	p, err := producer.New(cfg, "messages")
	if err != nil {
		log.Fatal(err)
	}

	p.Run()

}
