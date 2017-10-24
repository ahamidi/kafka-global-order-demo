package producer

import (
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Producer wraps kafka.Producer and embeds Topic
type Producer struct {
	Topic string
	*kafka.Producer
}

// New returns a new Producer
func New(c *kafka.ConfigMap, topic string) (*Producer, error) {
	p, err := kafka.NewProducer(c)
	if err != nil {
		return nil, err
	}

	return &Producer{topic, p}, nil
}

// Run blocking producer
func (p *Producer) Run() {
	for {
		produceMessage(p, "", "test", nil)
		time.Sleep(3 * time.Second)
	}

}

func produceMessage(p *Producer, key, message string, timestamp *time.Time) error {
	pm := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.Topic, Partition: kafka.PartitionAny},
		Value:          []byte(message),
		Key:            []byte(key),
	}

	if timestamp != nil {
		pm.Timestamp = *timestamp
	}

	return p.Produce(pm, nil)
}
