package config

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/caarlos0/env/v11"
	"github.com/go-playground/validator/v10"
	"github.com/joho/godotenv"
)

var validate = validator.New(validator.WithRequiredStructEnabled())

// Config는 collector가 시작 시점에 검증해야 하는 실행 환경 설정입니다.
type Config struct {
	Env                   string   `env:"LOOPAD_ENV" validate:"required"`
	ServiceID             string   `env:"LOOPAD_SERVICE_ID" validate:"required,eq=event-collector"`
	Port                  int      `env:"PORT" validate:"min=1,max=65535"`
	KafkaBootstrapBrokers []string `env:"LOOPAD_KAFKA_BOOTSTRAP_BROKERS" envSeparator:"," validate:"required,min=1,dive,required"`
	KafkaSecurityProtocol string   `env:"LOOPAD_KAFKA_SECURITY_PROTOCOL" envDefault:"" validate:"omitempty,oneof=PLAINTEXT SASL_PLAINTEXT"`
	KafkaSASLMechanism    string   `env:"LOOPAD_KAFKA_SASL_MECHANISM" envDefault:"" validate:"omitempty,oneof=SCRAM-SHA-512"`
	KafkaUsername         string   `env:"LOOPAD_KAFKA_USERNAME" envDefault:""`
	KafkaPassword         string   `env:"LOOPAD_KAFKA_PASSWORD" envDefault:""`
	EventTopic            string   `env:"LOOPAD_EVENT_TOPIC" validate:"required"`
}

// Load는 .env를 먼저 반영한 뒤 환경변수를 파싱하고 필수 설정을 검증합니다.
func Load() (Config, error) {
	if err := loadDotenv(); err != nil {
		return Config{}, err
	}

	cfg, err := env.ParseAsWithOptions[Config](env.Options{
		RequiredIfNoDef: true,
	})
	if err != nil {
		return Config{}, err
	}
	cfg.normalizeKafkaAuth()

	if err := validate.Struct(cfg); err != nil {
		return Config{}, fmt.Errorf("config validation failed: %w", err)
	}
	if err := cfg.validateKafkaAuth(); err != nil {
		return Config{}, fmt.Errorf("config validation failed: %w", err)
	}

	return cfg, nil
}

// ListenAddr은 HTTP 서버가 바인딩할 주소를 반환합니다.
func (c Config) ListenAddr() string {
	return fmt.Sprintf("0.0.0.0:%d", c.Port)
}

func (c *Config) normalizeKafkaAuth() {
	c.KafkaSecurityProtocol = strings.ToUpper(strings.TrimSpace(c.KafkaSecurityProtocol))
	c.KafkaSASLMechanism = strings.ToUpper(strings.TrimSpace(c.KafkaSASLMechanism))
}

func (c Config) validateKafkaAuth() error {
	switch c.KafkaSecurityProtocol {
	case "", "PLAINTEXT":
		if c.KafkaSASLMechanism != "" {
			return errors.New("LOOPAD_KAFKA_SASL_MECHANISM requires LOOPAD_KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT")
		}
		return nil
	case "SASL_PLAINTEXT":
		if c.KafkaSASLMechanism == "" {
			return errors.New("LOOPAD_KAFKA_SASL_MECHANISM is required when LOOPAD_KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT")
		}
		if strings.TrimSpace(c.KafkaUsername) == "" || strings.TrimSpace(c.KafkaPassword) == "" {
			return errors.New("LOOPAD_KAFKA_USERNAME and LOOPAD_KAFKA_PASSWORD are required when LOOPAD_KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT")
		}
		return nil
	default:
		return nil
	}
}

// loadDotenv는 로컬 개발용 .env 파일이 있으면 로드하고 없으면 조용히 넘어갑니다.
func loadDotenv() error {
	if err := godotenv.Load(); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("load .env: %w", err)
	}
	return nil
}
