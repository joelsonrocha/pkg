package pkg

import (
	"context"
	"encoding/json"
	"os"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
}

func NewProducer(topic string) *Producer {
	brokers := []string{os.Getenv("KAFKA_URL")}

	return &Producer{
		writer: kafka.NewWriter(kafka.WriterConfig{
			Brokers:  brokers,
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
		}),
	}
}

func (producer *Producer) ProduceMessage(ctx context.Context, key string, value interface{}) error {
	var messageValue []byte

	switch v := value.(type) {
	case string:
		messageValue = []byte(v)
	default:
		jsonString, err := json.Marshal(value)
		if err != nil {
			return err
		}
		messageValue = jsonString
	}

	message := kafka.Message{
		Key:   []byte(key),
		Value: messageValue,
	}

	return producer.writer.WriteMessages(ctx, message)
}

func (producer *Producer) Close() error {
	return producer.writer.Close()
}
