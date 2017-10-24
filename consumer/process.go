package consumer

import (
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/fatih/color"
)

// Outputs messages as soon they are received
func processMessages(in chan kafka.Message) {
	var lastMessage kafka.Message
	for m := range in {
		printMessage(&lastMessage, m)
		lastMessage = m
	}
}

// Reorders messages based on timestamp and discards any that arrive outside of
// the time window
func processMessagesInOrder(in chan kafka.Message, timeWindow *time.Time) {

}

func printMessage(lastMessage *kafka.Message, currentMessage kafka.Message) {
	mts := currentMessage.Timestamp
	var tsString string
	if lastMessage != nil && lastMessage.Timestamp.After(mts) {
		tsString = color.RedString(currentMessage.Timestamp.String())
	} else {
		tsString = color.GreenString(currentMessage.Timestamp.String())
	}
	log.Printf("Received Message: %s:%s @ %s on partition %d", string(currentMessage.Key), string(currentMessage.Value), tsString, currentMessage.TopicPartition.Partition)
}
