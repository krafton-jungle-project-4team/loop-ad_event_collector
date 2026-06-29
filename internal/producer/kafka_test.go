package producer

import (
	"testing"

	"github.com/segmentio/kafka-go"
)

func TestNewKafkaWithSCRAMConfigSetsSASLMechanism(t *testing.T) {
	kafkaProducer, err := NewKafka(KafkaConfig{
		Brokers:  []string{"ip-10-0-1-10:9094"},
		Topic:    "loop-ad.events.raw",
		Username: "event-collector",
		Password: "test-password",
	})
	if err != nil {
		t.Fatalf("NewKafka() error = %v", err)
	}
	t.Cleanup(func() {
		if err := kafkaProducer.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})

	transport, ok := kafkaProducer.writer.Transport.(*kafka.Transport)
	if !ok {
		t.Fatalf("writer.Transport = %T, want *kafka.Transport", kafkaProducer.writer.Transport)
	}
	if transport.SASL == nil {
		t.Fatal("writer.Transport.SASL = nil")
	}
	if got := transport.SASL.Name(); got != "SCRAM-SHA-512" {
		t.Fatalf("writer.Transport.SASL.Name() = %q", got)
	}
}
