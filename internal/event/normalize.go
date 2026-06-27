package event

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type RawEvent struct {
	EventID    string `json:"event_id"`
	UserID     string `json:"user_id"`
	CampaignID string `json:"campaign_id"`
	CreativeID string `json:"creative_id"`
	EventType  string `json:"event_type"`
	OccurredAt string `json:"occurred_at"`
	RequestID  string `json:"request_id"`
	Payload    string `json:"payload"`
}

type incomingEvent struct {
	EventID        string          `json:"event_id"`
	UserID         string          `json:"user_id"`
	CampaignID     string          `json:"campaign_id"`
	CreativeID     string          `json:"creative_id"`
	EventType      string          `json:"event_type"`
	EventName      string          `json:"event_name"`
	OccurredAt     string          `json:"occurred_at"`
	EventTime      string          `json:"event_time"`
	RequestID      string          `json:"request_id"`
	Payload        json.RawMessage `json:"payload"`
	PropertiesJSON string          `json:"properties_json"`
}

func NormalizeForClickHouse(body []byte, fallbackRequestID string) (RawEvent, []byte, error) {
	compacted, err := compactJSON(body)
	if err != nil {
		return RawEvent{}, nil, err
	}

	var incoming incomingEvent
	decoder := json.NewDecoder(bytes.NewReader(compacted))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&incoming); err != nil {
		var loose incomingEvent
		if looseErr := json.Unmarshal(compacted, &loose); looseErr != nil {
			return RawEvent{}, nil, fmt.Errorf("event body must be a JSON object")
		}
		incoming = loose
	}

	eventType := firstNonEmpty(incoming.EventType, incoming.EventName)
	occurredAt, err := normalizeOccurredAt(firstNonEmpty(incoming.OccurredAt, incoming.EventTime))
	if err != nil {
		return RawEvent{}, nil, err
	}

	row := RawEvent{
		EventID:    strings.TrimSpace(incoming.EventID),
		UserID:     strings.TrimSpace(incoming.UserID),
		CampaignID: strings.TrimSpace(incoming.CampaignID),
		CreativeID: strings.TrimSpace(incoming.CreativeID),
		EventType:  eventType,
		OccurredAt: occurredAt,
		RequestID:  firstNonEmpty(incoming.RequestID, fallbackRequestID),
	}

	if row.EventID == "" {
		return RawEvent{}, nil, fmt.Errorf("event_id is required")
	}
	if row.UserID == "" {
		return RawEvent{}, nil, fmt.Errorf("user_id is required")
	}
	if row.EventType == "" {
		return RawEvent{}, nil, fmt.Errorf("event_type or event_name is required")
	}
	if row.RequestID == "" {
		return RawEvent{}, nil, fmt.Errorf("request_id is required")
	}

	payload, err := normalizePayload(incoming.Payload, compacted)
	if err != nil {
		return RawEvent{}, nil, err
	}
	row.Payload = payload

	value, err := json.Marshal(row)
	if err != nil {
		return RawEvent{}, nil, err
	}
	return row, value, nil
}

func normalizeOccurredAt(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("occurred_at or event_time is required")
	}

	for _, layout := range []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.000",
		"2006-01-02 15:04:05",
	} {
		parsed, err := time.Parse(layout, value)
		if err == nil {
			return parsed.UTC().Format("2006-01-02 15:04:05.000"), nil
		}
	}

	return "", fmt.Errorf("occurred_at or event_time must be RFC3339 or ClickHouse DateTime64 text")
}

func normalizePayload(raw json.RawMessage, compactedBody []byte) (string, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return string(compactedBody), nil
	}

	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		if strings.TrimSpace(text) == "" {
			return "{}", nil
		}
		return text, nil
	}

	compacted, err := compactAnyJSON(raw)
	if err != nil {
		return "", fmt.Errorf("payload must be a JSON string, object, or array")
	}
	return string(compacted), nil
}

func compactJSON(body []byte) ([]byte, error) {
	var buf bytes.Buffer
	if err := json.Compact(&buf, body); err != nil {
		return nil, fmt.Errorf("event body must be valid JSON: %w", err)
	}
	if !bytes.HasPrefix(bytes.TrimSpace(body), []byte("{")) {
		return nil, fmt.Errorf("event body must be one JSON object")
	}
	return buf.Bytes(), nil
}

func compactAnyJSON(body []byte) ([]byte, error) {
	var buf bytes.Buffer
	if err := json.Compact(&buf, body); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
