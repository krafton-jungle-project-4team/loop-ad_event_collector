package config

import "testing"

func TestLoadParsesRequiredEnv(t *testing.T) {
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
}

func TestLoadRejectsWrongServiceID(t *testing.T) {
	setConfigEnv(t, map[string]string{"LOOPAD_SERVICE_ID": "dashboard-api"})

	_, err := Load()
	if err == nil {
		t.Fatal("Load() error = nil, want service id error")
	}
}

func TestLoadRejectsMissingEnv(t *testing.T) {
	setConfigEnv(t, map[string]string{})
	t.Setenv("LOOPAD_EVENT_TOPIC", "")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() error = nil, want missing env error")
	}
}

func TestParsePortRejectsOutOfRangeValue(t *testing.T) {
	if _, err := parsePort("70000"); err == nil {
		t.Fatal("parsePort() error = nil, want out-of-range error")
	}
}

func TestParseCSVRejectsEmptyBroker(t *testing.T) {
	if _, err := parseCSV("kafka:9092,"); err == nil {
		t.Fatal("parseCSV() error = nil, want empty broker error")
	}
}

func setConfigEnv(t *testing.T, overrides map[string]string) {
	t.Helper()

	values := map[string]string{
		"LOOPAD_ENV":                     "dev",
		"LOOPAD_SERVICE_ID":              "event-collector",
		"PORT":                           "8080",
		"LOOPAD_KAFKA_BOOTSTRAP_BROKERS": "kafka-1:9092,kafka-2:9092",
		"LOOPAD_EVENT_TOPIC":             "loop-ad.events.raw",
	}
	for key, value := range overrides {
		values[key] = value
	}
	for key, value := range values {
		t.Setenv(key, value)
	}
}
