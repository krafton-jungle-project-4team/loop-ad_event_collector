package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

const expectedServiceID = "event-collector"

type Config struct {
	Env                   string
	ServiceID             string
	Port                  int
	KafkaBootstrapBrokers []string
	EventTopic            string
}

func Load() (Config, error) {
	envName, err := requiredEnv("LOOPAD_ENV")
	if err != nil {
		return Config{}, err
	}
	serviceID, err := requiredEnv("LOOPAD_SERVICE_ID")
	if err != nil {
		return Config{}, err
	}
	eventTopic, err := requiredEnv("LOOPAD_EVENT_TOPIC")
	if err != nil {
		return Config{}, err
	}
	portValue, err := requiredEnv("PORT")
	if err != nil {
		return Config{}, err
	}
	brokerValue, err := requiredEnv("LOOPAD_KAFKA_BOOTSTRAP_BROKERS")
	if err != nil {
		return Config{}, err
	}

	cfg := Config{
		Env:        envName,
		ServiceID:  serviceID,
		EventTopic: eventTopic,
	}

	port, err := parsePort(portValue)
	if err != nil {
		return Config{}, err
	}
	cfg.Port = port

	brokers, err := parseCSV(brokerValue)
	if err != nil {
		return Config{}, fmt.Errorf("LOOPAD_KAFKA_BOOTSTRAP_BROKERS is invalid: %w", err)
	}
	cfg.KafkaBootstrapBrokers = brokers

	if cfg.ServiceID != expectedServiceID {
		return Config{}, fmt.Errorf("LOOPAD_SERVICE_ID must be %q, got %q", expectedServiceID, cfg.ServiceID)
	}

	return cfg, nil
}

func (c Config) ListenAddr() string {
	return fmt.Sprintf("0.0.0.0:%d", c.Port)
}

func requiredEnv(name string) (string, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return "", fmt.Errorf("%s is required", name)
	}
	return value, nil
}

func parsePort(value string) (int, error) {
	port, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("PORT must be a number: %w", err)
	}
	if port < 1 || port > 65535 {
		return 0, fmt.Errorf("PORT must be between 1 and 65535")
	}
	return port, nil
}

func parseCSV(value string) ([]string, error) {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		item := strings.TrimSpace(part)
		if item == "" {
			return nil, fmt.Errorf("empty broker")
		}
		out = append(out, item)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("at least one broker is required")
	}
	return out, nil
}
