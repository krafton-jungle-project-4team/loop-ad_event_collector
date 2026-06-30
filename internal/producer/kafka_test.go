package producer

import (
	"strings"
	"testing"

	"github.com/segmentio/kafka-go"
)

func TestNewKafkaConfiguresSASLPlaintextSCRAMSHA512(t *testing.T) {
	cfg := validKafkaConfig()

	producer, err := NewKafka(cfg)
	if err != nil {
		t.Fatalf("NewKafka() error = %v", err)
	}
	t.Cleanup(func() {
		_ = producer.Close()
	})

	if producer.writer.Topic != cfg.Topic {
		t.Fatalf("topic = %q, want %q", producer.writer.Topic, cfg.Topic)
	}
	transport, ok := producer.writer.Transport.(*kafka.Transport)
	if !ok {
		t.Fatalf("Transport = %T, want *kafka.Transport", producer.writer.Transport)
	}
	if transport.SASL == nil {
		t.Fatal("Transport.SASL = nil, want SCRAM mechanism")
	}
	if transport.SASL.Name() != SASLMechanismSCRAMSHA512 {
		t.Fatalf("SASL.Name() = %q, want %q", transport.SASL.Name(), SASLMechanismSCRAMSHA512)
	}
}

func TestNewKafkaRejectsUnsupportedSecurityProtocol(t *testing.T) {
	cfg := validKafkaConfig()
	cfg.SecurityProtocol = "PLAINTEXT"

	_, err := NewKafka(cfg)
	if err == nil {
		t.Fatal("NewKafka() error = nil, want unsupported protocol error")
	}
	if !strings.Contains(err.Error(), `unsupported kafka security protocol "PLAINTEXT"`) {
		t.Fatalf("NewKafka() error = %q", err.Error())
	}
}

func TestNewKafkaRejectsUnsupportedSASLMechanism(t *testing.T) {
	cfg := validKafkaConfig()
	cfg.SASLMechanism = "PLAIN"

	_, err := NewKafka(cfg)
	if err == nil {
		t.Fatal("NewKafka() error = nil, want unsupported mechanism error")
	}
	if !strings.Contains(err.Error(), `unsupported kafka SASL mechanism "PLAIN"`) {
		t.Fatalf("NewKafka() error = %q", err.Error())
	}
}

func TestNewKafkaRejectsMissingSASLCredentials(t *testing.T) {
	cfg := validKafkaConfig()
	cfg.Password = ""

	_, err := NewKafka(cfg)
	if err == nil {
		t.Fatal("NewKafka() error = nil, want missing credentials error")
	}
	if strings.Contains(err.Error(), "test-password") {
		t.Fatal("NewKafka() error exposed Kafka password")
	}
}

func validKafkaConfig() KafkaConfig {
	return KafkaConfig{
		Brokers:          []string{"kafka-1:9094", "kafka-2:9094"},
		Topic:            "loop-ad.events.raw",
		SecurityProtocol: SecurityProtocolSASLPlaintext,
		SASLMechanism:    SASLMechanismSCRAMSHA512,
		Username:         "event-collector",
		Password:         "test-password",
	}
}
