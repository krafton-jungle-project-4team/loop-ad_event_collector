package config

import (
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadParsesRequiredEnv(t *testing.T) {
	chdirTemp(t)
	setConfigEnv(t, map[string]string{})

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.ListenAddr() != "0.0.0.0:8080" {
		t.Fatalf("ListenAddr() = %q", cfg.ListenAddr())
	}
	if len(cfg.KafkaBootstrapBrokers) != 2 {
		t.Fatalf("brokers = %#v", cfg.KafkaBootstrapBrokers)
	}
	if cfg.KafkaSecurityProtocol != "SASL_PLAINTEXT" {
		t.Fatalf("KafkaSecurityProtocol = %q", cfg.KafkaSecurityProtocol)
	}
	if cfg.KafkaSASLMechanism != "SCRAM-SHA-512" {
		t.Fatalf("KafkaSASLMechanism = %q", cfg.KafkaSASLMechanism)
	}
	if cfg.KafkaUsername != "event-collector" {
		t.Fatal("KafkaUsername was not parsed")
	}
	if cfg.KafkaPassword == "" {
		t.Fatal("KafkaPassword was not parsed")
	}
}

func TestLoadReadsDotenvFile(t *testing.T) {
	dir := chdirTemp(t)
	for _, key := range configEnvKeys {
		unsetEnv(t, key)
	}
	dotenv := []byte(`
LOOPAD_ENV=dev
LOOPAD_SERVICE_ID=event-collector
PORT=9090
LOOPAD_KAFKA_BOOTSTRAP_BROKERS=kafka:9094
LOOPAD_KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT
LOOPAD_KAFKA_SASL_MECHANISM=SCRAM-SHA-512
LOOPAD_KAFKA_USERNAME=event-collector
LOOPAD_KAFKA_PASSWORD=test-password
LOOPAD_EVENT_TOPIC=loop-ad.events.raw
`)
	if err := os.WriteFile(filepath.Join(dir, ".env"), dotenv, 0o600); err != nil {
		t.Fatalf("write .env: %v", err)
	}

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.ListenAddr() != "0.0.0.0:9090" {
		t.Fatalf("ListenAddr() = %q", cfg.ListenAddr())
	}
}

func TestLoadRejectsWrongServiceID(t *testing.T) {
	chdirTemp(t)
	setConfigEnv(t, map[string]string{"LOOPAD_SERVICE_ID": "dashboard-api"})

	_, err := Load()
	if err == nil {
		t.Fatal("Load() error = nil, want service id error")
	}
}

func TestLoadRejectsMissingEnv(t *testing.T) {
	chdirTemp(t)
	setConfigEnv(t, map[string]string{})
	t.Setenv("LOOPAD_EVENT_TOPIC", "")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() error = nil, want missing env error")
	}
}

func TestLoadRejectsMissingSCRAMCredentials(t *testing.T) {
	chdirTemp(t)
	setConfigEnv(t, map[string]string{
		"LOOPAD_KAFKA_SECURITY_PROTOCOL": "SASL_PLAINTEXT",
		"LOOPAD_KAFKA_SASL_MECHANISM":    "SCRAM-SHA-512",
		"LOOPAD_KAFKA_USERNAME":          "",
		"LOOPAD_KAFKA_PASSWORD":          "test-password",
	})

	_, err := Load()
	if err == nil {
		t.Fatal("Load() error = nil, want missing SCRAM credential error")
	}
	if strings.Contains(err.Error(), "test-password") {
		t.Fatal("Load() error exposed Kafka password")
	}
}

func TestLoadRejectsOutOfRangePort(t *testing.T) {
	chdirTemp(t)
	setConfigEnv(t, map[string]string{"PORT": "70000"})

	_, err := Load()
	if err == nil {
		t.Fatal("Load() error = nil, want out-of-range port error")
	}
}

func setConfigEnv(t *testing.T, overrides map[string]string) {
	t.Helper()

	values := map[string]string{
		"LOOPAD_ENV":                     "dev",
		"LOOPAD_SERVICE_ID":              "event-collector",
		"PORT":                           "8080",
		"LOOPAD_KAFKA_BOOTSTRAP_BROKERS": "kafka-1:9094,kafka-2:9094",
		"LOOPAD_KAFKA_SECURITY_PROTOCOL": "SASL_PLAINTEXT",
		"LOOPAD_KAFKA_SASL_MECHANISM":    "SCRAM-SHA-512",
		"LOOPAD_KAFKA_USERNAME":          "event-collector",
		"LOOPAD_KAFKA_PASSWORD":          "test-password",
		"LOOPAD_EVENT_TOPIC":             "loop-ad.events.raw",
	}
	maps.Copy(values, overrides)
	for key, value := range values {
		t.Setenv(key, value)
	}
}

var configEnvKeys = []string{
	"LOOPAD_ENV",
	"LOOPAD_SERVICE_ID",
	"PORT",
	"LOOPAD_KAFKA_BOOTSTRAP_BROKERS",
	"LOOPAD_KAFKA_SECURITY_PROTOCOL",
	"LOOPAD_KAFKA_SASL_MECHANISM",
	"LOOPAD_KAFKA_USERNAME",
	"LOOPAD_KAFKA_PASSWORD",
	"LOOPAD_EVENT_TOPIC",
}

func chdirTemp(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()
	t.Chdir(dir)
	return dir
}

func unsetEnv(t *testing.T, key string) {
	t.Helper()

	value, ok := os.LookupEnv(key)
	if err := os.Unsetenv(key); err != nil {
		t.Fatalf("unset %s: %v", key, err)
	}
	t.Cleanup(func() {
		if ok {
			_ = os.Setenv(key, value)
			return
		}
		_ = os.Unsetenv(key)
	})
}
