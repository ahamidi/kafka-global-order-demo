package consumer

import (
	"fmt"
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
	err := c.SetKey("group.id", clientID)
	if err != nil {
		return nil, err
	}
	consumer, err := kafka.NewConsumer(c)
	if err != nil {
		return nil, err
	}

	return &Consumer{topic, consumer}, nil
}

// Consume message from Topic. This is blocking.
func (c *Consumer) Consume(inOrder bool, timeWindow *time.Time) {
	err := c.Subscribe(c.Topic, nil)
	if err != nil {
		log.Fatal(err)
	}

	// Only messages will be pushed to this channel
	msgChan := make(chan kafka.Message)
	go c.filterMessages(msgChan)

	// This will block
	if inOrder {
		processMessagesInOrder(msgChan, timeWindow)
	} else {
		processMessages(msgChan)
	}

	c.Close()
}

func (c *Consumer) filterMessages(messages chan kafka.Message) {
	for {
		select {
		case ev := <-c.Events():
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				c.Assign(e.Partitions)
			case kafka.RevokedPartitions:
				c.Unassign()
			case *kafka.Message:
				fmt.Printf("%% Message on %s:\n%s\n",
					e.TopicPartition, string(e.Value))
			case kafka.Error:
				log.Println("Error:", e)
				break
			}
		}
	}
}
