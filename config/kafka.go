package config

import (
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func NewConsumerConfig(brokers string) *kafka.ConfigMap {
	return &kafka.ConfigMap{
		"bootstrap.servers":               stripSchemaFromBrokerURL(brokers),
		"session.timeout.ms":              10000,
		"go.events.channel.enable":        true,
		"go.application.rebalance.enable": true,
		"default.topic.config":            kafka.ConfigMap{"auto.offset.reset": "earliest"},
	}
}

func NewProducerConfig(brokers string) *kafka.ConfigMap {
	return &kafka.ConfigMap{
		"bootstrap.servers": stripSchemaFromBrokerURL(brokers),
	}
}

func stripSchemaFromBrokerURL(s string) string {
	brokers := strings.Split(s, ",")

	for i, b := range brokers {
		brokers[i] = strings.Split(b, "://")[1]
	}

	return strings.Join(brokers, ",")
}
