package config

import (
	"errors"
	"fmt"
	"os"

	"github.com/caarlos0/env/v11"
	"github.com/go-playground/validator/v10"
	"github.com/joho/godotenv"
)

var validate = validator.New(validator.WithRequiredStructEnabled())

type Config struct {
	Env                   string   `env:"LOOPAD_ENV" validate:"required"`
	ServiceID             string   `env:"LOOPAD_SERVICE_ID" validate:"required,eq=event-collector"`
	Port                  int      `env:"PORT" validate:"min=1,max=65535"`
	KafkaBootstrapBrokers []string `env:"LOOPAD_KAFKA_BOOTSTRAP_BROKERS" envSeparator:"," validate:"required,min=1,dive,required"`
	EventTopic            string   `env:"LOOPAD_EVENT_TOPIC" validate:"required"`
}

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

	if err := validate.Struct(cfg); err != nil {
		return Config{}, fmt.Errorf("config validation failed: %w", err)
	}

	return cfg, nil
}

func (c Config) ListenAddr() string {
	return fmt.Sprintf("0.0.0.0:%d", c.Port)
}

func loadDotenv() error {
	if err := godotenv.Load(); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("load .env: %w", err)
	}
	return nil
}
