package producer

import (
	"math/rand"
	"strconv"
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
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		produceMessage(p, strconv.Itoa(rand.Intn(32)), "test", oldTimestamp(6))
	}
}

func produceMessage(p *Producer, key, message string, timestamp time.Time) error {
	pm := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.Topic, Partition: kafka.PartitionAny},
		Value:          []byte(message),
		Key:            []byte(key),
		Timestamp:      timestamp,
	}

	return p.Produce(pm, nil)
}

// returns timestamp that is up to max seconds ago
func oldTimestamp(maxSeconds int) time.Time {
	goBack := rand.Intn(maxSeconds)
	ts := time.Now().UTC().Add(-(time.Duration(goBack) * time.Second))

	return ts
}
