package event

import (
	"encoding/json"
	"testing"
)

func TestNormalizeForClickHouseMapsSDKPayload(t *testing.T) {
	body := []byte(`{
		"project_id":"demo-shoppingmall",
		"event_id":"evt_001",
		"user_id":"u_001",
		"session_id":"s_001",
		"event_time":"2026-06-27T10:00:00.123+09:00",
		"event_name":"product_view",
		"campaign_id":"cmp_001",
		"creative_id":"cr_001",
		"properties_json":"{\"page\":{\"path\":\"/products/sku-1\"}}"
	}`)

	row, value, err := NormalizeForClickHouse(body, "req_test")
	if err != nil {
		t.Fatalf("NormalizeForClickHouse() error = %v", err)
	}
	if row.EventType != "product_view" {
		t.Fatalf("EventType = %q", row.EventType)
	}
	if row.OccurredAt != "2026-06-27 01:00:00.123" {
		t.Fatalf("OccurredAt = %q", row.OccurredAt)
	}
	if row.RequestID != "req_test" {
		t.Fatalf("RequestID = %q", row.RequestID)
	}
	if row.Payload == "" {
		t.Fatal("Payload is empty")
	}

	var marshaled RawEvent
	if err := json.Unmarshal(value, &marshaled); err != nil {
		t.Fatalf("normalized value is not JSON: %v", err)
	}
	if marshaled.EventID != "evt_001" || marshaled.UserID != "u_001" {
		t.Fatalf("marshaled row = %+v", marshaled)
	}
}

func TestNormalizeForClickHouseAcceptsRawContractPayload(t *testing.T) {
	body := []byte(`{
		"event_id":"evt_002",
		"user_id":"u_001",
		"campaign_id":"cmp_001",
		"creative_id":"cr_001",
		"event_type":"click",
		"occurred_at":"2026-06-25 03:01:00.000",
		"request_id":"req_001",
		"payload":"{\"slot\":\"main\"}"
	}`)

	row, _, err := NormalizeForClickHouse(body, "req_fallback")
	if err != nil {
		t.Fatalf("NormalizeForClickHouse() error = %v", err)
	}
	if row.Payload != `{"slot":"main"}` {
		t.Fatalf("Payload = %q", row.Payload)
	}
	if row.RequestID != "req_001" {
		t.Fatalf("RequestID = %q", row.RequestID)
	}
}

func TestNormalizeForClickHouseCompactsPayloadObject(t *testing.T) {
	body := []byte(`{
		"event_id":"evt_003",
		"user_id":"u_002",
		"event_type":"impression",
		"occurred_at":"2026-06-25 03:05:00.000",
		"payload":{"slot":"sidebar"}
	}`)

	row, _, err := NormalizeForClickHouse(body, "req_002")
	if err != nil {
		t.Fatalf("NormalizeForClickHouse() error = %v", err)
	}
	if row.Payload != `{"slot":"sidebar"}` {
		t.Fatalf("Payload = %q", row.Payload)
	}
}

func TestNormalizeForClickHouseCompactsPayloadArray(t *testing.T) {
	body := []byte(`{
		"event_id":"evt_004",
		"user_id":"u_002",
		"event_type":"impression",
		"occurred_at":"2026-06-25 03:05:00.000",
		"payload":[{"slot":"main"}]
	}`)

	row, _, err := NormalizeForClickHouse(body, "req_002")
	if err != nil {
		t.Fatalf("NormalizeForClickHouse() error = %v", err)
	}
	if row.Payload != `[{"slot":"main"}]` {
		t.Fatalf("Payload = %q", row.Payload)
	}
}

func TestNormalizeForClickHouseRequiresEventID(t *testing.T) {
	body := []byte(`{"user_id":"u_001","event_name":"page_view","event_time":"2026-06-27T01:00:00Z"}`)

	if _, _, err := NormalizeForClickHouse(body, "req_test"); err == nil {
		t.Fatal("NormalizeForClickHouse() error = nil, want event_id error")
	}
}

func TestNormalizeForClickHouseRejectsNonObject(t *testing.T) {
	if _, _, err := NormalizeForClickHouse([]byte(`[{"event_id":"evt_1"}]`), "req_test"); err == nil {
		t.Fatal("NormalizeForClickHouse() error = nil, want object error")
	}
}
