package producer

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
)

type Message struct {
	Key   []byte
	Value []byte
}

type Producer interface {
	Produce(context.Context, Message) error
	Close() error
}

type KafkaConfig struct {
	Brokers []string
	Topic   string
}

type Kafka struct {
	writer *kafka.Writer
}

func NewKafka(cfg KafkaConfig) *Kafka {
	return &Kafka{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(cfg.Brokers...),
			Topic:        cfg.Topic,
			Balancer:     &kafka.LeastBytes{},
			RequiredAcks: kafka.RequireAll,
			Async:        false,
			BatchSize:    100,
			BatchBytes:   1 << 20,
			BatchTimeout: 10 * time.Millisecond,
			WriteTimeout: 10 * time.Second,
			ReadTimeout:  10 * time.Second,
			MaxAttempts:  3,
		},
	}
}

func (k *Kafka) Produce(ctx context.Context, message Message) error {
	return k.writer.WriteMessages(ctx, kafka.Message{
		Key:   message.Key,
		Value: message.Value,
		Time:  time.Now().UTC(),
	})
}

func (k *Kafka) Close() error {
	return k.writer.Close()
}
