package producer

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
)

// Message는 collector가 Kafka에 발행할 key/value 바이트입니다.
type Message struct {
	Key   []byte
	Value []byte
}

// Producer는 HTTP 서버가 Kafka 구현에 의존하지 않도록 분리한 발행 인터페이스입니다.
type Producer interface {
	Produce(context.Context, Message) error
	Close() error
}

// KafkaConfig는 Kafka writer 생성에 필요한 브로커와 토픽 설정입니다.
type KafkaConfig struct {
	Brokers          []string
	Topic            string
	SecurityProtocol string
	SASLMechanism    string
	Username         string
	Password         string
}

// Kafka는 segmentio/kafka-go writer를 감싼 프로듀서 구현입니다.
type Kafka struct {
	writer *kafka.Writer
	dialer *kafka.Dialer
}

// NewKafka는 ack를 기다리는 동기 발행 설정으로 Kafka 프로듀서를 생성합니다.
func NewKafka(cfg KafkaConfig) (*Kafka, error) {
	dialer, err := newKafkaDialer(cfg)
	if err != nil {
		return nil, err
	}

	return &Kafka{
		writer: newKafkaWriter(cfg, dialer),
		dialer: dialer,
	}, nil
}

func newKafkaWriter(cfg KafkaConfig, dialer *kafka.Dialer) *kafka.Writer {
	writer := &kafka.Writer{
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
	}
	if dialer != nil {
		writer.Transport = newKafkaTransport(dialer)
	}
	return writer
}

func newKafkaDialer(cfg KafkaConfig) (*kafka.Dialer, error) {
	securityProtocol := strings.ToUpper(strings.TrimSpace(cfg.SecurityProtocol))
	saslMechanism := strings.ToUpper(strings.TrimSpace(cfg.SASLMechanism))

	switch securityProtocol {
	case "", "PLAINTEXT":
		return nil, nil
	case "SASL_PLAINTEXT":
		if saslMechanism != "SCRAM-SHA-512" {
			return nil, fmt.Errorf("unsupported kafka SASL mechanism %q", saslMechanism)
		}
		if strings.TrimSpace(cfg.Username) == "" || strings.TrimSpace(cfg.Password) == "" {
			return nil, errors.New("kafka SASL username and password are required")
		}
		mechanism, err := scram.Mechanism(scram.SHA512, cfg.Username, cfg.Password)
		if err != nil {
			return nil, fmt.Errorf("create kafka SASL mechanism: %w", err)
		}
		return &kafka.Dialer{
			Timeout:       kafka.DefaultDialer.Timeout,
			DualStack:     kafka.DefaultDialer.DualStack,
			SASLMechanism: mechanism,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported kafka security protocol %q", securityProtocol)
	}
}

func newKafkaTransport(dialer *kafka.Dialer) *kafka.Transport {
	netDialer := &net.Dialer{
		Timeout:       dialer.Timeout,
		Deadline:      dialer.Deadline,
		LocalAddr:     dialer.LocalAddr,
		DualStack:     dialer.DualStack,
		FallbackDelay: dialer.FallbackDelay,
		KeepAlive:     dialer.KeepAlive,
	}
	return &kafka.Transport{
		Dial:     netDialer.DialContext,
		SASL:     dialer.SASLMechanism,
		TLS:      dialer.TLS,
		ClientID: dialer.ClientID,
	}
}

// Produce는 메시지가 Kafka에 ack될 때까지 동기적으로 발행합니다.
func (k *Kafka) Produce(ctx context.Context, message Message) error {
	return k.writer.WriteMessages(ctx, kafka.Message{
		Key:   message.Key,
		Value: message.Value,
		Time:  time.Now().UTC(),
	})
}

// Close는 내부 Kafka writer 리소스를 정리합니다.
func (k *Kafka) Close() error {
	return k.writer.Close()
}
