package consumer

import (
	"fmt"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type MessageHandler interface {
	HandleMessage(*Message)
}

type Message struct {
	TopicPartition kafka.TopicPartition
	Value          []byte
	Key            []byte
	Timestamp      time.Time
	TimestampType  int
	Opaque         interface{}
	Headers        []kafka.Header
	LeaderEpoch    *int32
}

func AdaptKafkaMessage(original *kafka.Message) *Message {
	return &Message{
		TopicPartition: original.TopicPartition,
		Value:          original.Value,
		Key:            original.Key,
		Timestamp:      original.Timestamp,
		TimestampType:  int(original.TimestampType),
		Opaque:         original.Opaque,
		Headers:        original.Headers,
		LeaderEpoch:    original.LeaderEpoch,
	}
}

func (m *Message) AdaptToKafkaMessage() *kafka.Message {
	return &kafka.Message{
		TopicPartition: m.TopicPartition,
		Value:          m.Value,
		Key:            m.Key,
		Timestamp:      m.Timestamp,
		TimestampType:  kafka.TimestampType(m.TimestampType),
		Opaque:         m.Opaque,
		Headers:        m.Headers,
		LeaderEpoch:    m.LeaderEpoch,
	}
}

type Consumer struct {
	handler MessageHandler
}

func NewConsumer(topics []string, groupID string, handler MessageHandler) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": os.Getenv("KAFKA_URL"),
		"group.id":          groupID,
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}

	c.SubscribeTopics(topics, nil)

	consumer := &Consumer{handler: handler}

	// A signal handler or similar could be used to set this to false to break the loop.
	run := true

	for run {
		msg, err := c.ReadMessage(time.Second)
		if err == nil {
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
			go consumer.handleFuntion(msg)
		} else if !err.(kafka.Error).IsTimeout() {
			// The client will automatically try to recover from all errors.
			// Timeout is not considered an error because it is raised by
			// ReadMessage in absence of messages.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		} else {
			fmt.Printf("Consumer error timeout: %v (%v)\n", err, msg)
		}
	}

	c.Close()
}

func (c *Consumer) handleFuntion(msg *kafka.Message) {
	c.handler.HandleMessage(AdaptKafkaMessage(msg))
}
