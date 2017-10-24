package consumer

import (
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Outputs messages as soon they are received
func processMessages(in chan kafka.Message) {
	for m := range in {
		log.Printf("Received Message: %s", m.String())
	}
}

// Reorders messages based on timestamp and discards any that arrive outside of
// the time window
func processMessagesInOrder(in chan kafka.Message, timeWindow *time.Time) {

}
