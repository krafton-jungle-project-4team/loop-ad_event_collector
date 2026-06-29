package producer

import (
	"testing"

	"github.com/segmentio/kafka-go"
)

func TestNewKafkaWithSCRAMConfigSetsDialerAndSASL(t *testing.T) {
	kafkaProducer, err := NewKafka(KafkaConfig{
		Brokers:          []string{"ip-10-0-1-10:9094"},
		Topic:            "loop-ad.events.raw",
		SecurityProtocol: "SASL_PLAINTEXT",
		SASLMechanism:    "SCRAM-SHA-512",
		Username:         "event-collector",
		Password:         "test-password",
	})
	if err != nil {
		t.Fatalf("NewKafka() error = %v", err)
	}
	t.Cleanup(func() {
		if err := kafkaProducer.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})

	if kafkaProducer.dialer == nil {
		t.Fatal("dialer = nil, want SASL dialer")
	}
	if kafkaProducer.dialer.SASLMechanism == nil {
		t.Fatal("dialer.SASLMechanism = nil")
	}
	if got := kafkaProducer.dialer.SASLMechanism.Name(); got != "SCRAM-SHA-512" {
		t.Fatalf("dialer.SASLMechanism.Name() = %q", got)
	}

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

func TestNewKafkaWithPlaintextConfigUsesDefaultTCPWriter(t *testing.T) {
	kafkaProducer, err := NewKafka(KafkaConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "loop-ad.events.raw",
	})
	if err != nil {
		t.Fatalf("NewKafka() error = %v", err)
	}
	t.Cleanup(func() {
		if err := kafkaProducer.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})

	if kafkaProducer.dialer != nil {
		t.Fatal("dialer was configured for plaintext Kafka")
	}
	if kafkaProducer.writer.Transport != nil {
		t.Fatalf("writer.Transport = %T, want nil for default plaintext transport", kafkaProducer.writer.Transport)
	}
}
