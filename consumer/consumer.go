package consumer

import (
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Consumer wraps kafka.Consumer and embeds Topic and Consumer Group ID
type Consumer struct {
	Topic string
	*kafka.Consumer
}

// New returns a new Consumer.
func New(c *kafka.ConfigMap, topic string, clientID string) (*Consumer, error) {
	if err := c.SetKey("group.id", clientID); err != nil {
		return nil, err
	}
	consumer, err := kafka.NewConsumer(c)
	if err != nil {
		return nil, err
	}

	return &Consumer{topic, consumer}, nil
}

// Consume message from Topic. This is blocking.
func (c *Consumer) Consume(inOrder bool, timeWindow *time.Duration) {
	if err := c.Subscribe(c.Topic, nil); err != nil {
		log.Fatal(err)
	}

	// Only messages will be pushed to this channel
	msgChan := make(chan kafka.Message)
	defer close(msgChan)
	go c.filterMessages(msgChan)

	// This will block
	if inOrder {
		processMessagesInOrder(msgChan, *timeWindow)
	} else {
		processMessages(msgChan)
	}

	c.Close()
}

// discard everything but message events
func (c *Consumer) filterMessages(messages chan kafka.Message) {
	for ev := range c.Events() {
		switch e := ev.(type) {
		case kafka.AssignedPartitions:
			c.Assign(e.Partitions)
		case kafka.RevokedPartitions:
			c.Unassign()
		case *kafka.Message:
			messages <- *e
		case kafka.Error:
			log.Println("Error:", e)
			break
		}
	}
}
