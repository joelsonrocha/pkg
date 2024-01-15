package pkg

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	handler func(kafka.Message)
}

func NewConsumer(ctx context.Context, topics []string, groupID string, handler func(kafka.Message)) {
	brokers := []string{os.Getenv("KAFKA_URL")}

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  groupID,
		Topic:    topics[0], // Assuming a single topic for simplicity
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})

	consumer := &Consumer{handler: handler}

	var wg sync.WaitGroup

	for {
		select {
		case <-ctx.Done():
			// Context done, exit the loop
			wg.Wait()
			return
		default:
			// Continue processing messages
			msg, err := r.ReadMessage(ctx)
			if err != nil {
				fmt.Printf("Consumer error: %v\n", err)
				break
			}

			wg.Add(1)
			go func(msg kafka.Message) {
				defer wg.Done()
				consumer.handleFunction(msg)
			}(msg)
		}
	}
}

func (c *Consumer) handleFunction(msg kafka.Message) {
	c.handler(msg)
}
